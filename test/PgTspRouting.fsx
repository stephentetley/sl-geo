// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.3\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

#load "..\src\SLGeo\Base\PostGISConn\ErrorTrace.fs"
#load "..\src\SLGeo\Base\PostGISConn\SqlUtils.fs"
#load "..\src\SLGeo\Base\PostGISConn\PGSQLConn.fs"
#load "..\src\SLGeo\Extra\CsvOutput.fs"
#load "..\src\SLGeo\Base\Coord.fs"
#load "..\src\SLGeo\Base\WellKnownText.fs"
#load "..\src\SLGeo\Base\WGS84.fs"
#load "..\src\SLGeo\Shell\ScriptMonad.fs"
#load "..\src\SLGeo\Shell\PostGIS.fs"
#load "..\src\SLGeo\Scripts\TspRouting.fs"
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.CsvOutput
open SLGeo.Base.Coord
open SLGeo.Shell.ScriptMonad
open SLGeo.Scripts.TspRouting


let [<Literal>] StationsCsv  =  __SOURCE_DIRECTORY__ +  @"\..\data\stations.csv"
type StationData = 
    CsvProvider< StationsCsv,
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


