// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0-beta4\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\ExcelProvider.0.8.2\lib"
#r "ExcelProvider.dll"
open FSharp.ExcelProvider


#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

#load "..\src\SL\Base\ErrorTrace.fs"
#load "..\src\SL\Base\SqlUtils.fs"
#load "..\src\SL\Base\PGSQLConn.fs"
#load "..\src\SL\Base\ExcelProviderHelper.fs"
#load "..\src\SL\Base\Grouping.fs"
#load "..\src\SL\Base\CsvOutput.fs"
#load "..\src\SL\Geo\Coord.fs"
#load "..\src\SL\Geo\WellKnownText.fs"
#load "..\src\SL\Geo\WGS84.fs"
#load "..\src\SL\Geo\OSGB36.fs"
#load "..\src\SL\Geo\SRTransform.fs"
#load "..\src\SL\PostGIS\ScriptMonad.fs"
#load "..\src\SL\PostGIS\PostGIS.fs"
#load "..\src\SL\Scripts\PostGISHulls.fs"
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.Grouping
open SL.Base.CsvOutput
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS
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



