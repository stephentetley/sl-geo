﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0-beta3\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\Npgsql.4.0.0-preview2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.4.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql



#I @"..\packages\FParsec.1.0.3\lib\net40-client"
#r "FParsec"
#r "FParsecCS"

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

#load @"SL\Base\ErrorTrace.fs"
#load @"SL\Base\SqlUtils.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\Coord.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\PostGIS\ScriptMonad.fs"
#load @"SL\PostGIS\PostGIS.fs"
#load @"SL\Scripts\CsvOutput.fs"
open SL.Base.SqlUtils
open SL.Base.PGSQLConn
open SL.Geo.Coord
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS
open SL.Scripts.CsvOutput


// PostgresSQL with PostGIS enabled.
// Use Table: spt_outfalls


type OutfallData = 
    CsvProvider< @"G:\work\Projects\events2\db-import-tables\outlets.csv",
                 HasHeaders = true>

type OutfallRow = OutfallData.Row

let getOutfalls () : OutfallRow list = (new OutfallData ()).Rows |> Seq.toList

type NeighboursData = 
    CsvProvider< @"G:\work\Projects\events2\Asset-collected-data.csv",
                 HasHeaders = true>

type NeighboursRow = NeighboursData.Row

let getDataForNeighbours () : seq<NeighboursRow> = (new NeighboursData ()).Rows |> Seq.cast<NeighboursRow>


// ********** SCRIPT **********

let deleteAllData () : Script<int> = 
    liftPGSQLConn <| deleteAllRows "spt_outfalls"

let makeOutfallINSERT (row:OutfallRow) : string = 
    let east        = 1.0<meter> * (float <| row.METREEASTING)
    let north       = 1.0<meter> * (float <| row.METRENORTHING)
    let osgb36Pt    =  { Easting = east; Northing = north }
    let wgs84Pt     = osgb36ToWGS84 osgb36Pt
    let pointLit = 
        // SRID=4326 is WGS 84 coordinate reference system
        sprintf "ST_GeogFromText('SRID=4326;POINT(%f %f)')"
                wgs84Pt.Longitude wgs84Pt.Latitude

    sqlINSERT "spt_outfalls" 
        <|  [ stringValue       "stc25_ref"         row.STC25_REF
            ; stringValue       "function_node"     row.FUNCTION_NODE
            ; stringValue       "osgb36_grid"       (showOSGB36Point osgb36Pt)
            ; literalValue      "point_loc"         pointLit
            ]



let insertOutfalls () : Script<int> = 
    let rows = getOutfalls ()
    let proc1 (row:OutfallRow) : PGSQLConn<int> = execNonQuery <| makeOutfallINSERT row
    liftPGSQLConn <| SL.Base.PGSQLConn.sumForM rows proc1



let SetupDB(password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
  
    runScript (failwith) (printfn "Success: %i modifications") (consoleLogger) conn 
        <| sumSequenceM 
            [ deleteAllData ()          |> logScript (sprintf "%i rows deleted")
            ; insertOutfalls ()         |> logScript (sprintf "%i rows inserted") 
            ]

/// Limit is closest N neighbours
let makeNearestNeighbourQUERY (limit:int) (point:WGS84Point) : string = 
    System.String.Format("""
        SELECT 
            stc25_ref, function_node, osgb36_grid
        FROM 
            spt_outfalls 
        ORDER BY point_loc <-> ST_Point({0}, {1}) LIMIT {2} ;
        """, point.Longitude, point.Latitude, limit)

type NeighbourRec = 
    { STC25Ref: string
      FunctionNode: string 
      Osgb36Grid: string } 

let pgNearestNeighbourQuery (limit:int) (point:WGS84Point) : PGSQLConn<NeighbourRec list> = 
    let query = makeNearestNeighbourQUERY limit point
    let procM (reader:NpgsqlDataReader) : NeighbourRec = 
        { STC25Ref      = reader.GetString(0)
        ; FunctionNode  = reader.GetString(1)
        ; Osgb36Grid    = reader.GetString(2) }
    execReaderList query procM   




let printNeighbours (recs:NeighbourRec list) : string = 
    let print1 (x:NeighbourRec) : string = 
        sprintf "%s (stc25=%s)" x.Osgb36Grid x.STC25Ref
    String.concat " & " <| List.map print1 recs

type OutputRow = 
    { Uid: string
      Name: string
      AssetNGR: string
      CatsNGRs: string
      OutfallBestGuessNgr: string
      OutfallBestGuessStc25: string
      OutfallNNs: string }


let genOutputRow (limit:int) (row:NeighboursRow) : Script<OutputRow> = 
    let neighbours () : Script<NeighbourRec list>= 
        match Option.map osgb36ToWGS84 <| tryReadOSGB36Point row.``Cats NGRs`` with
        | Some wgs84 -> 
            liftPGSQLConn <| pgNearestNeighbourQuery 5 wgs84
        | None -> scriptMonad.Return []

    let ngrAndStc25OfOne (xs:NeighbourRec list) : string*string = 
        match xs with
        | x :: _ -> x.Osgb36Grid, x.STC25Ref
        | _ -> "??","??"

    scriptMonad { 
        let! nsList = neighbours ()
        let (ngr1,stc1) = ngrAndStc25OfOne nsList
        return { Uid = row.``SAI Number``
               ; Name = row.``Asset Name``
               ; AssetNGR = row.``Asset OSGB36``
               ; CatsNGRs = row.``Cats NGRs``
               ; OutfallBestGuessNgr = ngr1
               ; OutfallBestGuessStc25 = stc1
               ; OutfallNNs = printNeighbours nsList }
    }           


let tellOutputRow (row:OutputRow) : CellWriter list = 
    [ tellString row.Uid
    ; tellString row.Name
    ; tellString row.AssetNGR
    ; tellString row.CatsNGRs
    ; tellString row.OutfallBestGuessNgr
    ; tellString (sprintf "stc25=%s" row.OutfallBestGuessStc25)
    ; tellString row.OutfallNNs ]

let csvHeaders = 
    [ "Asset Uid"; "Asset Name"; "Asset OSGB36"; "Cats NGRs"
    ; "Best Match NGR"; "Best Match STC25Ref"
    ; "Outfall NNs"]

let OutputNN(password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let outFile = @"G:\work\Projects\events2\outfall-neighbours.csv"

    runConsoleScript (printfn "Success: %A") conn 
        <| scriptMonad { 
                let rows1:seq<NeighboursRow> = getDataForNeighbours ()
                let! (rows2:seq<OutputRow>) = SL.PostGIS.ScriptMonad.traverseM (genOutputRow 5) rows1
                let csvProc:CsvOutput<unit> = writeRecordsWithHeaders csvHeaders rows2 tellOutputRow
                do (outputToNew {Separator=","} csvProc outFile)
                return ()
                }
