﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0-beta4\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

#load "..\SLGeo\SL\Base\ErrorTrace.fs"
#load "..\SLGeo\SL\Base\SqlUtils.fs"
#load "..\SLGeo\SL\Base\PGSQLConn.fs"
#load "..\SLGeo\SL\Base\CsvOutput.fs"
#load "..\SLGeo\SL\Geo\Tolerance.fs"
#load "..\SLGeo\SL\Geo\Coord.fs"
#load "..\SLGeo\SL\Geo\WellKnownText.fs"
#load "..\SLGeo\SL\Geo\WGS84.fs"
#load "..\SLGeo\SL\PostGIS\ScriptMonad.fs"
#load "..\SLGeo\SL\PostGIS\PostGIS.fs"
#load "..\SLGeo\SL\Scripts\TspRouting.fs"
open SL.Base.PGSQLConn
open SL.Base.CsvOutput
open SL.Geo.Coord
open SL.PostGIS.ScriptMonad
open SL.Scripts.TspRouting


// let [<Literal>] StationsCsv  =  @"..\data\stations.csv"
type StationData = 
    CsvProvider< @"..\data\stations.csv",
                 HasHeaders = true,
                 Schema = "Name(string),Grid_Ref(string)" >

type StationRow = StationData.Row

let getStations () : StationRow list = (new StationData ()).Rows |> Seq.toList



let tspVertexInsertDict:TspNodeInsertDict<StationRow> = 
    { TryMakeNodeLocation = 
        fun (row:StationRow) -> Option.map osgb36ToWGS84 <| tryReadOSGB36Point row.Grid_Ref
      MakeNodeLabel = 
        fun (row:StationRow) -> row.Name
    }


let SetupDB(password:string) : unit = 
    let rows = getStations ()
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| setupTspNodeDB tspVertexInsertDict rows 


let stationOutputDict : TspPrintRouteStepDict = 
    { CsvHeaders = [ "Serial Num"; "Station"; "Grid Ref"; "Aggregate Cost" ]
      MakeCsvRow =  
        fun (node:TspRouteNode) -> 
            [ tellInt           node.SeqNumber
            ; tellString        node.NodeLabel
            ; tellString        << showOSGB36Point << wgs84ToOSGB36 <| node.GridRef
            ; tellFloat         node.AggCost
            ]
    }


let main (password:string) : unit = 
    let outputFile = __SOURCE_DIRECTORY__ + @"\..\data\stations-route.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A ") conn 
        <| scriptMonad { 
            let! startId    = findIdByLabel "Bradford Interchange"
            let! endId      = findIdByLabel "Mytholmroyd"
            do! generateTspRouteCsv stationOutputDict startId endId outputFile
            }


let test02 (password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A ") conn 
        <| scriptMonad { 
            let! startId    = furthestEastId ()
            let! endId      = furthestWestId ()
            let! ans        = generateTspRouteWKT startId endId 
            return ans
            }

