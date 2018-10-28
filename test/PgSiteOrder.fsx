// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\ExcelProvider.1.0.1\lib\net45"
#r "ExcelProvider.Runtime.dll"

#I @"..\packages\ExcelProvider.1.0.1\typeproviders\fsharp41\net45"
#r "ExcelDataReader.DataSet.dll"
#r "ExcelDataReader.dll"
#r "ExcelProvider.DesignTime.dll"
open FSharp.Interop.Excel


#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.3\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

#load "..\src\SLGeo\Base\PostGISConn\ErrorTrace.fs"
#load "..\src\SLGeo\Base\PostGISConn\SqlUtils.fs"
#load "..\src\SLGeo\Base\PostGISConn\PGSQLConn.fs"
#load "..\src\SLGeo\Extra\ExcelProviderHelper.fs"
#load "..\src\SLGeo\Extra\Grouping.fs"
#load "..\src\SLGeo\Extra\CsvOutput.fs"
#load "..\src\SLGeo\Base\Base.fs"
#load "..\src\SLGeo\Base\WellKnownText.fs"
#load "..\src\SLGeo\Base\WGS84.fs"
#load "..\src\SLGeo\Base\OSGB36.fs"
#load "..\src\SLGeo\Base\SRTransform.fs"
#load "..\src\SLGeo\Shell\ScriptMonad.fs"
#load "..\src\SLGeo\Shell\PostGIS.fs"
#load "..\src\SLGeo\Scripts\TspRouting.fs"
#load "..\src\SLGeo\Scripts\SiteOrder.fs"
open SLGeo.Base.PostGISConn.SqlUtils
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.ExcelProviderHelper
open SLGeo.Extra.Grouping
open SLGeo.Extra.CsvOutput
open SLGeo.Base.WellKnownText
open SLGeo.Base.WGS84
open SLGeo.Base.OSGB36
open SLGeo.Base.SRTransform
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell.PostGIS
open SLGeo.Scripts.TspRouting
open SLGeo.Scripts.SiteOrder


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
            



