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

#load "..\src\SL\Base\ErrorTrace.fs"
#load "..\src\SL\Base\SqlUtils.fs"
#load "..\src\SL\Base\PGSQLConn.fs"
#load "..\src\SL\Base\ExcelProviderHelper.fs"
#load "..\src\SL\Base\Grouping.fs"
#load "..\src\SL\Base\CsvOutput.fs"
#load "..\src\SL\Geo\Base.fs"
#load "..\src\SL\Geo\WellKnownText.fs"
#load "..\src\SL\Geo\WGS84.fs"
#load "..\src\SL\Geo\OSGB36.fs"
#load "..\src\SL\Geo\SRTransform.fs"
#load "..\src\SL\PostGIS\ScriptMonad.fs"
#load "..\src\SL\PostGIS\PostGIS.fs"
#load "..\src\SL\Scripts\TspRouting.fs"
#load "..\src\SL\Scripts\SiteOrder.fs"
open SL.Base.SqlUtils
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.Grouping
open SL.Base.CsvOutput
open SL.Geo.WellKnownText
open SL.Geo.WGS84
open SL.Geo.OSGB36
open SL.Geo.SRTransform
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS
open SL.Scripts.TspRouting
open SL.Scripts.SiteOrder


/// Note SheetName does not seem to like underscores...
type SiteListTable = 
    ExcelFile< FileName = @"G:\work\Projects\rtu\MK5 MMIM Replacement\SiteList-2010-2011-2012.xlsx",
                SheetName = "Sites2012",
                ForceString = true >

type SiteListRow = SiteListTable.Row


/// Use object expressions / interfaces as per F# design guidelines...
let getSiteListRows () : seq<SiteListRow> = 
    let helper = 
        { new IExcelProviderHelper<SiteListTable,SiteListRow>
          with member this.ReadTableRows table = table.Data 
               member this.IsBlankRow row = match row.GetValue(0) with null -> true | _ -> false }         
    excelReadRows helper (new SiteListTable())



let filterOutMoreInfo (rows:seq<SiteListRow>) : seq<SiteListRow> = 
    Seq.filter (fun (row:SiteListRow) ->
                    row.``Work Centre`` <> "MORE DETAILS REQUIRED") rows

let test01 () =
    let makeGrouping = fun (row:SiteListRow) -> row.``Work Centre``
    let groups = groupingBy makeGrouping <| getSiteListRows ()
    Seq.iter (printfn "GROUP:\n%A") <| groups

let test02 () =
    let source = [(2,"two"); (4,"four"); (1,"one"); (3,"three")]
    let keys = [4;3;2;1]
    Seq.iter (printfn "%A") <| sortToKeyList (fst) (source |> List.toSeq) keys


let siteOrderDict:SiteOrderDict<string,SiteListRow> = 
    { GroupingOp = 
        fun (row:SiteListRow) -> 
            match row.``Work Centre`` with
            | null -> "UNKNOWN WORK CENTER"
            | ss -> ss
    ; ExtractGridRef = 
        fun (row:SiteListRow) -> 
            match tryReadOSGB36Point row.``Grid Ref`` with
            | None -> sreturn None
            | Some pt -> fmapM Some <| liftAtomically (osgb36ToWGS84 pt)
    ; ExtractNodeLabel = 
        fun (row:SiteListRow) -> 
            sprintf "%s" row.``Site Name`` }


let main (password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let siteList = filterOutMoreInfo <| getSiteListRows ()
    runConsoleScript (List.iter (fun (i,s) -> printfn "%i,%s" i s)) conn
        <| siteOrder siteOrderDict siteList
            



