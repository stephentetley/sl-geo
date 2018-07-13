// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\ExcelProvider.0.8.2\lib"
#r "ExcelProvider.dll"
open FSharp.ExcelProvider

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

#load @"SL\Base\ErrorTrace.fs"
#load @"SL\Base\SqlUtils.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Base\ExcelProviderHelper.fs"
#load @"SL\Base\Grouping.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\Coord.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\PostGIS\ScriptMonad.fs"
#load @"SL\PostGIS\PostGIS.fs"
#load @"SL\Scripts\CsvOutput.fs"
#load @"SL\Scripts\PostGISHulls.fs"
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.Grouping
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS
open SL.Scripts.CsvOutput

open SL.Scripts.PostGISHulls

type ImportTable = 
    ExcelFile< @"G:\work\Projects\events2\site-list-for-hospitals.xlsx",
               SheetName = "Site_List",
               ForceString = true >

type ImportRow = ImportTable.Row


let getImportRows () : seq<ImportRow> = 
    let dict : GetRowsDict<ImportTable, ImportRow> = 
        { GetRows     = fun imports -> imports.Data 
          NotNullProc = fun row -> match row.GetValue(0) with null -> false | _ -> true }
    excelTableGetRowsSeq dict (new ImportTable())

let test01 () = 
    groupingBy (fun (x:ImportRow) -> x.operational_contact ) <| getImportRows ()

let concaveHullOutput (ix:int) (key:string) (wtk:WellKnownText<_>) (elts:seq<ImportRow>) : RowWriter = 
    [ tellQuotedString key
    ; tellInt <| Seq.length elts
    ; tellQuotedString <| unwrapWellKnownText wtk
    ]


let hullsMethodDict:GroupingMakeHullsDict<string,ImportRow> = 
    { GroupByOperation = fun (x:ImportRow) -> x.operational_contact 
      GetElementLoc = 
            fun (x:ImportRow) -> Option.map osgb36ToWGS84 <| tryReadOSGB36Point x.site_ngr
      CsvHeaders = [ "operations"; "asset count"; "well_known_text" ]
      MakeCsvRow = concaveHullOutput
    }

let WktConcaveHulls (pwd:string) = 
    let outputFile = @"G:\work\Projects\events2\wkt_concave_hulls_sitelist.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" pwd 
    let importRows = getImportRows ()

    runConsoleScript (printfn "Success: %A") conn 
        <| generateConcaveHullsCsv { TargetPercentage = 0.9 }
                                    hullsMethodDict
                                    importRows
                                    outputFile

let WktConvexHulls (pwd:string) = 
    let outputFile = @"G:\work\Projects\events2\wkt_convex_hulls_sitelist.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" pwd 
    let importRows = getImportRows ()

    runConsoleScript (printfn "Success: %A") conn 
        <| generateConvexHullsCsv hullsMethodDict importRows outputFile



