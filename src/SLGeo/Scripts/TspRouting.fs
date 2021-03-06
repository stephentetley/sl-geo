﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SLGeo.Scripts.TspRouting

open Npgsql

open SLGeo.Base.PostGISConn.SqlUtils
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Base.PostGISConn
open SLGeo.Extra.CsvOutput
open SLGeo.Base
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell




// 


// Generate a route of List<label * grid_ref * cost> 

// We want to:
// (1) print out to Csv in order, 
// (2) generate a WKT linestring for viewing
// Maybe other things...




let private deleteAllTSPDbRows () : Script<int> = 
    liftAtomically <| deleteAllRowsRestartIdentity "spt_tsp_nodes"


/// wgs84:Longitude is x, wgs84:Latitude is y
/// We have a label in the vertex table so we can interpret the data after the route 
/// is generated.
let private makeVertexINSERT (vertex:WGS84Point) (label:string) : string = 
    sqlINSERT "spt_tsp_nodes" 
            <|  [ floatValue    "x"         <| float vertex.Longitude
                ; floatValue    "y"         <| float vertex.Latitude
                ; stringValue   "label"     label
                ]


/// An asset has a label, it should have a location (but it might not).
type TspNodeInsertDict<'asset> = 
    { TryMakeNodeLocation : 'asset -> Script<WGS84Point option>
      MakeNodeLabel: 'asset -> string }


/// Design "problem" 
/// This procedure associates a row with an Id but the Id is (probably) unknown
/// to the user when it is returned from pgr_tsp, hence I think we need label 
/// and a join.
let insertVertices (dict:TspNodeInsertDict<'row>) (vertices:'row list) : Script<int> = 
    let proc1 (point:WGS84Point, label:string) : PGSQLConn<int> = 
        execNonQuery <| makeVertexINSERT point label
    
    let good1 (row:'row) : Script<(WGS84Point * string) option> = 
         dict.TryMakeNodeLocation row >>= fun opt -> 
            match opt with
            | None -> sreturn None
            | Some pt -> sreturn <| Some (pt, dict.MakeNodeLabel row)
            // Option.map (fun pt -> (pt, dict.MakeNodeLabel row)) 
    scriptMonad {             
        let! goodData = fmapM (List.choose id) <| mapM good1 vertices 
        let! count =  liftAtomically <| PGSQLConn.sumTraverseM proc1 goodData
        return count
    }

let setupTspNodeDB (dict:TspNodeInsertDict<'row>) (vertices:'row list) : Script<int> = 
    scriptMonad { 
        let! _      = deleteAllTSPDbRows ()         |> logScript (sprintf "%i rows deleted")
        let! count  = insertVertices dict vertices  |> logScript (sprintf "%i rows inserted") 
        return count
     }


let makeEucledianTSPQUERY (startId:int) (endId:int) : string = 
    System.String.Format("""
        SELECT r.seq, r.node, v.label, v.x, v.y, r.cost, r.agg_cost
        FROM 
            pgr_eucledianTSP( 'SELECT id, x, y FROM spt_tsp_nodes', 
                    start_id := {0},
                    end_id := {1}
                   ) AS r
        LEFT JOIN spt_tsp_nodes AS v ON r.node = v.id;
        """, startId, endId)

/// NodeLabel must be a short string so we can store it in the DB.
type TspRouteNode = 
    { SeqNumber: int
      NodeLabel: string
      GridRef: WGS84Point
      Cost: float
      AggCost: float }

type TspRoute = TspRouteNode list

let private dropLast (source:'a list) : 'a list = 
    let rec work ac xs = 
        match xs with
        | [] -> List.rev ac
        | [x] -> List.rev ac
        | x :: xs -> work (x::ac) xs
    work [] source

/// NOte - need at least two elements in the database.
let eucledianTSP (startId:int) (endId:int) : Script<TspRoute> = 
    let query = makeEucledianTSPQUERY startId endId
    let procM (reader:NpgsqlDataReader) : TspRouteNode = 
        let gridRef = 
            let lon = float <| reader.GetDouble(3)
            let lat = float <| reader.GetDouble(4)
            { Longitude = 1.0<degree> * lon; Latitude = 1.0<degree> * lat}
        { SeqNumber     = reader.GetInt32(0)
        ; NodeLabel     = reader.GetString(2)
        ; GridRef       = gridRef
        ; Cost          = float <| reader.GetDouble(5)
        ; AggCost       = float <| reader.GetDouble(6) } 
    ScriptMonad.fmapM dropLast << liftAtomically <| execReaderList query procM  


let makeFindIdByLabelQUERY (label:string) : string = 
    System.String.Format("""
        SELECT id FROM spt_tsp_nodes WHERE label='{0}';
        """, label)

let private findIdQuery (query:string) : Script<int> = 
    let procM (reader:NpgsqlDataReader) : int = reader.GetInt32(0)
    liftAtomically <| execReaderFirst query procM  


let findIdByLabel (label:string) : Script<int> = 
    findIdQuery <| makeFindIdByLabelQUERY label
    

let furthestNorthIdQUERY : string  = 
    "SELECT a.id FROM spt_tsp_nodes a WHERE y = (SELECT MAX(b.y) FROM spt_tsp_nodes b);"

let furthestSouthIdQUERY : string  = 
    "SELECT a.id FROM spt_tsp_nodes a WHERE y = (SELECT MIN(b.y) FROM spt_tsp_nodes b);"

let furthestEastIdQUERY : string  = 
    "SELECT a.id FROM spt_tsp_nodes a WHERE x = (SELECT MIN(b.x) FROM spt_tsp_nodes b);"

let furthestWestIdQUERY : string  = 
    "SELECT a.id FROM spt_tsp_nodes a WHERE x = (SELECT MAX(b.x) FROM spt_tsp_nodes b);"


let furthestNorthId () : Script<int> = findIdQuery furthestNorthIdQUERY
    
let furthestSouthId () : Script<int> = findIdQuery furthestSouthIdQUERY

let furthestEastId () : Script<int> = findIdQuery furthestEastIdQUERY

let furthestWestId () : Script<int> = findIdQuery furthestWestIdQUERY

type TspPrintRouteStepDict = 
    { CsvHeaders: string list
      MakeCsvRow: TspRouteNode -> RowWriter
    }

let generateTspRouteCsv (dict:TspPrintRouteStepDict) (startId:int) (endId:int) (outputFile:string) : Script<unit> =
    scriptMonad { 
        let! steps = eucledianTSP startId endId
        let rows = List.map dict.MakeCsvRow steps
        let csvProc:CsvOutput<unit> = writeRowsWithHeaders dict.CsvHeaders rows
        do (outputToNew {Separator=","} csvProc outputFile)
        return ()
        }


let private getPoints (nodes:TspRoute) : WGS84Point list = 
    List.map (fun x -> x.GridRef) nodes

let generateTspRouteWKT (startId:int) (endId:int) : Script<string> =
    scriptMonad { 
        let! steps = eucledianTSP startId endId
        let (points:WGS84Point list) = getPoints steps
        return (showWktLineString << WktLineString <| makeWktCoordList wktIsoWGS84 points)
        }

// New code...
// API of this module to think about at some point.

let tspRoute (dict:TspNodeInsertDict<'asset>) (assets:'asset list) : Script<(int * string) list> = 
    match assets with
    | [] -> scriptMonad.Return []
    | [one] -> scriptMonad.Return [(1, dict.MakeNodeLabel one)]
    | xs -> 
        scriptMonad { 
            let! _          = setupTspNodeDB dict assets
            let! startId    = furthestNorthId ()
            let! endId      = furthestSouthId ()
            let! steps      = eucledianTSP startId endId
            let  path       = List.map (fun (x:TspRouteNode) -> (x.SeqNumber, x.NodeLabel)) steps
            return path
            }

    