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

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

#load @"SL\Base\ErrorTrace.fs"
#load @"SL\Base\SqlUtils.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Base\ExcelProviderHelper.fs"
#load @"SL\Base\Grouping.fs"
#load @"SL\Base\CsvOutput.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\Coord.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\PostGIS\ScriptMonad.fs"
#load @"SL\PostGIS\PostGIS.fs"
#load @"SL\Scripts\TspRouting.fs"
#load @"SL\Scripts\SiteOrder.fs"
open SL.Base.SqlUtils
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.Grouping
open SL.Base.CsvOutput
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS
open SL.Scripts.TspRouting
open SL.Scripts.SiteOrder

type SiteListTable = 
    ExcelFile< @"G:\work\Projects\events2\EDM2 Site-List SK.xlsx",
                SheetName = "SITE_LIST",
                ForceString = true >

type SiteListRow = SiteListTable.Row

//let getSiteListRows () : seq<SiteListRow> = 
//    let dict : GetRowsDict<SiteListTable, SiteListRow> = 
//        { GetRows     = fun imports -> imports.Data 
//          NotNullProc = fun row -> match row.GetValue(0) with null -> false | _ -> true }
//    excelTableGetRowsSeq dict (new SiteListTable())

/// Use object expressions / interfaces as per F# design guidelines...
let getSiteListRows () : seq<SiteListRow> = 
    let dict () = 
        { new IExcelProviderHelper<SiteListTable,SiteListRow>
          with member this.GetTableRows table = table.Data 
               member this.IsBlankRow row = match row.GetValue(0) with null -> true | _ -> false }
         
    excelGetRows (dict ()) (new SiteListTable())



let filterOutMoreInfo (rows:seq<SiteListRow>) : seq<SiteListRow> = 
    Seq.filter (fun (row:SiteListRow) ->
                    row.``Work Center`` <> "MORE DETAILS REQUIRED") rows

let test01 () =
    let makeGrouping = fun (row:SiteListRow) -> row.``Work Center``
    let groups = groupingBy makeGrouping <| getSiteListRows ()
    Seq.iter (printfn "GROUP:\n%A") <| groups

let test02 () =
    let source = [(2,"two"); (4,"four"); (1,"one"); (3,"three")]
    let keys = [4;3;2;1]
    Seq.iter (printfn "%A") <| sortToKeyList (fst) (source |> List.toSeq) keys


let siteOrderDict:SiteOrderDict<string,SiteListRow> = 
    { GroupingOp = 
        fun (row:SiteListRow) -> 
            match row.``Work Center`` with
            | null -> "UNKNOWN WORK CENTER"
            | ss -> ss
    ; ExtractGridRef = 
        fun (row:SiteListRow) -> 
            Option.map osgb36ToWGS84 <| tryReadOSGB36Point row.``Site Grid Ref``
    ; ExtractNodeLabel = 
        fun (row:SiteListRow) -> 
            sprintf "%s" row.``Site Common Name`` }


let main (password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let siteList = filterOutMoreInfo <| getSiteListRows ()
    runConsoleScript (List.iter (fun (i,s) -> printfn "%i,%s" i s)) conn
        <| siteOrder siteOrderDict siteList
            



