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

#load "..\src\SLGeo\Base\PostGISConn\ErrorTrace.fs"
#load "..\src\SLGeo\Base\PostGISConn\SqlUtils.fs"
#load "..\src\SLGeo\Base\PostGISConn\PGSQLConn.fs"
#load "..\src\SLGeo\Extra\ExcelProviderHelper.fs"
#load "..\src\SLGeo\Extra\Grouping.fs"
#load "..\src\SLGeo\Extra\CsvOutput.fs"
#load "..\src\SLGeo\Base\Coord.fs"
#load "..\src\SLGeo\Base\WellKnownText.fs"
#load "..\src\SLGeo\Base\WGS84.fs"
#load "..\src\SLGeo\Base\OSGB36.fs"
#load "..\src\SLGeo\Base\SRTransform.fs"
#load "..\src\SLGeo\Shell\ScriptMonad.fs"
#load "..\src\SLGeo\Shell\PostGIS.fs"
#load "..\src\SLGeo\Scripts\PostGISHulls.fs"
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.ExcelProviderHelper
open SLGeo.Extra.Grouping
open SLGeo.Extra.CsvOutput
open SLGeo.Base.Coord
open SLGeo.Base.WellKnownText
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell.PostGIS
open SLGeo.Scripts.PostGISHulls

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



