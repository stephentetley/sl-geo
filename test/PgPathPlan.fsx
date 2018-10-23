// Copyright (c) Stephen Tetley 2018
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

// For Dictionary.tryFind
#I @"..\packages\FSharpx.Collections.2.0.0-beta3\lib\net45"
#r "FSharpx.Collections"
open FSharpx.Collections

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames


#load "..\src\SL\Base\ErrorTrace.fs"
#load "..\src\SL\Base\SqlUtils.fs"
#load "..\src\SL\Base\PGSQLConn.fs"
#load "..\src\SL\Base\CsvOutput.fs"
#load "..\src\SL\Base\NameGen.fs"
#load "..\src\SL\Base\GraphvizOutput.fs"
#load "..\src\SL\Geo\Tolerance.fs"
#load "..\src\SL\Geo\Coord.fs"
#load "..\src\SL\Geo\WellKnownText.fs"
#load "..\src\SL\Geo\WGS84.fs"
#load "..\src\SL\Geo\OSGB36.fs"
#load "..\src\SL\Geo\SRTransform.fs"
#load "..\src\SL\PostGIS\ScriptMonad.fs"
#load "..\src\SL\PostGIS\PostGIS.fs"
#load "..\src\SL\Scripts\PathFinder.fs"
open SL.Base.PGSQLConn
open SL.Geo
open SL.PostGIS.ScriptMonad
open SL.Scripts.PathFinder

// PostgresSQL with PostGIS enabled.
// Table Schema: see sql/pg_pathfind_tables.sql

// Read Node data
type NodeImportTable = 
    CsvProvider< @"..\data\pathfind\pathfind-mock-nodes.csv",
                 HasHeaders = true>

type NodeImportRow = NodeImportTable.Row

let getNodeImportRows () : seq<NodeImportRow> = 
    (new NodeImportTable ()).Rows |> Seq.cast<NodeImportRow>

let tryMakeNode (row:NodeImportRow) : UserLandNode option = 
    let convert1 : string -> WGS84Point option = 
        Option.bind WGS84Point.FromWktPoint << tryReadWktPoint
    match convert1 row.WKT with
    | Some gridRef -> 
        Some <| { TypeTag       = row.``Feature Type``
                ; NodeLabel     = row.``Feature Name``
                ; NodeLocation  = gridRef }
    | _ -> None


// Read Edge data
type EdgeImportTable = 
    CsvProvider< @"..\data\pathfind\pathfind-mock-edges.csv",
                 HasHeaders = true>

type EdgeImportRow = EdgeImportTable.Row

let getEdgeImportRows () : seq<EdgeImportRow> = 
    (new EdgeImportTable ()).Rows |> Seq.cast<EdgeImportRow>

// Input data is WGS84
let tryMakeEdge (row:EdgeImportRow) : UserLandEdge option = 
    let convert1 : string -> WGS84Point option = 
        Option.bind WGS84Point.FromWktPoint << tryReadWktPoint
    match convert1 row.``Start Point (WKT)``, convert1 row.``End Point (WKT)`` with
    | Some startPt, Some endPt -> 
        Some <| { TypeTag       = row.Type
                ; EdgeLabel     = row.Description
                ; EdgeStart     = startPt
                ; EdgeEnd       = endPt }
    | _,_ -> None

let pathFindInsertDict : PathFindInsertDict<NodeImportRow,EdgeImportRow> = 
    { TryMakeUserLandNode = tryMakeNode
      TryMakeUserLandEdge = tryMakeEdge }

let SetupDB(password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let nodeRows = getNodeImportRows ()
    let edgeRows = getEdgeImportRows ()
    runConsoleScript (printfn "Success: %i modifications") conn 
        <| SetupPathsDB pathFindInsertDict nodeRows edgeRows 

// ***** Testing towards path finding...

let test01 () : unit =
    match tryReadWktPoint "POINT  ( 389330.850 501189.852) " with
    | Some (pt:WktPoint<OSGB36>) -> 
        printfn "%s => %A" (showWktPoint pt) (Option.map showOSGB36Point <| OSGB36Point.FromWktPoint pt) 
    | None -> failwith "Grr!"

let roseTree1 : PathTree<string> = 
    PathTree("A",[PathTree("B",[PathTree("C",[PathTree("D",[]); PathTree("E",[])])])])

let test02 () = 
    List.transpose  [ ['h';'e';'l';'l';'o']
                    ; ['w';'o';'r';'l';'d']
                    ; ['!']
                    ]

let test03 (password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password

    runConsoleScript (printfn "Success: %A") conn 
        <| scriptMonad { 
            let! startPt = 
                liftAtomically <| osgb36ToWGS84 { Easting = 389330.850<meter> ; Northing = 501189.852<meter> }
            let! vs = findOutwardEdges startPt
            do (List.iter (printfn "Edge: %A") vs)
            return ()
            }


let test04 (password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (fun a -> printfn "Success!") conn 
        <| scriptMonad { 
            let! startNode = findNode "Station" "Bradford Forster Square"
            let! forest = buildLinkForest startNode.GridRef
            do! outputDot "plan" forest @"G:\work\working\output1.dot"
            do (printfn "%s" <| drawLinkForest forest)
            return ()
            }


//let test05 () = 
//    let dict:MakeEdgeDict<string,GraphvizEdge> = 
//        { MakeEdgeFromRouteNodes = 
//            fun n1 n2 -> { StartId = n1; 
//                            EndId = n2; 
//                            LineStyle = None;
//                            LineColour = Some "red1"; 
//                            EdgeLabel= None }
//        }
//    let change (route1:RouteOld<string>) : EdgeList<GraphvizEdge> = routeToEdgeList dict route1
//    let procM = generateDot "plan" << List.map change <| allRoutesTreeOld roseTree1
//    execGraphvizOutput procM
