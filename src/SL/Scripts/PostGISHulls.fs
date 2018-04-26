﻿module SL.Scripts.PostGISHulls

open Npgsql

open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.AnswerMonad
open SL.PostGIS.PGSQLConn
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS

open SL.Scripts.CsvOutput
open SL.Scripts.Grouping

// ***** Concave and convex hulls

/// MakeCsvRow - the int parameter is a incrementing serial 
/// number, users can ignore it if they wish.
/// TODO - this dictionary is too big, we should have one 
/// dictionary for grouping and other dictionaries for 
/// generating output.
type GroupingMakeHullsDict<'Key,'a> = 
    { GroupByOperation: 'a -> 'Key
      GetElementLoc: 'a -> WGS84Point option
      CsvHeaders: string list
      MakeCsvRow: int -> 'Key -> WellKnownText<WGS84> -> seq<'a> -> RowWriter }

let private extractPoints (dict:GroupingMakeHullsDict<'Key,'a>) (source:Grouping<'Key,'a>) : seq<WGS84Point> = 
    Seq.choose id <| Seq.map (dict.GetElementLoc) source.Elements

let private concaveHull1 (dict:GroupingMakeHullsDict<'Key,'a>) (source:Grouping<'Key,'a>) (targetPercent:float) : Script<WellKnownText<WGS84>> = 
    pgConcaveHull wktIsoWGS84 (Seq.toList <| extractPoints dict source) targetPercent

let private convexHull1 (dict:GroupingMakeHullsDict<'Key,'a>) (source:Grouping<'Key,'a>) : Script<WellKnownText<WGS84>> = 
    pgConvexHull wktIsoWGS84 (Seq.toList <| extractPoints dict source)

let private centroid1 (dict:GroupingMakeHullsDict<'Key,'a>) (source:Grouping<'Key,'a>) : Script<WGS84Point option> = 
    pgCentroid wktIsoWGS84 (Seq.toList <| extractPoints dict source)


/// THE WKT generated by PostGIS for hulls can contain different geometries 
/// (POINT, LINESTRING, POLYGON) depending on on the number of points in the
/// result set.
/// QGIS Delimited Text Layer Import issues warnings (soift errors) if the import
/// has multiple geometry types.
let private filterPOLYGONs (source:seq<'Key * WellKnownText<'srid> * seq<'a>>) : seq<'Key * WellKnownText<'srid> * seq<'a>> = 
    Seq.filter (fun (a,WellKnownText(b),c) -> b.Contains "POLYGON") source

type ConcaveHullOptions = 
    { TargetPercentage: float }



let private genHullsCsv (make1:Grouping<'Key,'a> -> Script<'Key * WellKnownText<WGS84> * seq<'a>>) (dict:GroupingMakeHullsDict<'Key,'a>) (source:seq<'a>) (outputFile:string) : Script<unit> =
    scriptMonad { 
        let groups = groupingBy dict.GroupByOperation source
        let! hulls = SL.PostGIS.ScriptMonad.traverseM make1 groups
        let rows = 
            Seq.mapi (fun ix (key,wkt,elts) -> dict.MakeCsvRow ix key wkt elts) <| filterPOLYGONs hulls
        let csvProc:CsvOutput<unit> = writeRowsWithHeaders dict.CsvHeaders rows
        do! liftAction <| outputToNew {Separator=","} csvProc outputFile
        }



// Note - This API is making Hulls (plural) as it was our original use case.
// Making a single hull is done by functions in Scripts.PostGIS.
// Maybe splitting between two modules is a bad API decision.

let generateConcaveHullsCsv (options:ConcaveHullOptions) (dict:GroupingMakeHullsDict<'Key,'a>) (source:seq<'a>) (outputFile:string) : Script<unit> =
    let make1 (group1:Grouping<'Key,'a>) : Script<'Key * WellKnownText<WGS84> * seq<'a>> = 
        SL.PostGIS.ScriptMonad.fmapM (fun x -> (group1.GroupingKey, x, group1.Elements)) 
            <| concaveHull1 dict group1 options.TargetPercentage
    genHullsCsv make1 dict source outputFile

let generateConvexHullsCsv (dict:GroupingMakeHullsDict<'Key,'a>) (source:seq<'a>) (outputFile:string) : Script<unit> =
    let make1 (group1:Grouping<'Key,'a>) : Script<'Key * WellKnownText<WGS84> * seq<'a>> =  
        SL.PostGIS.ScriptMonad.fmapM (fun x -> (group1.GroupingKey, x, group1.Elements)) 
            <| convexHull1 dict group1
    genHullsCsv make1 dict source outputFile


