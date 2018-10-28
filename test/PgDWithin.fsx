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
#load "..\src\SLGeo\Extra\CsvOutput.fs"
#load "..\src\SLGeo\Base\Coord.fs"
#load "..\src\SLGeo\Base\WellKnownText.fs"
#load "..\src\SLGeo\Base\WGS84.fs"
#load "..\src\SLGeo\Shell\ScriptMonad.fs"
#load "..\src\SLGeo\Shell\PostGIS.fs"
open SLGeo.Base.PostGISConn.SqlUtils
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.ExcelProviderHelper
open SLGeo.Extra.CsvOutput
open SLGeo.Base.Coord
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell




type SiteListTable = 
    ExcelFile< @"G:\work\Projects\events2\site-list-for-GEN.xlsx",
                SheetName = "SITE_LIST",
                ForceString = true >

type SiteListRow = SiteListTable.Row

let getSiteListRows () : SiteListRow list = 
    let helper = 
        { new IExcelProviderHelper<SiteListTable,SiteListRow>
          with member this.ReadTableRows table = table.Data 
               member this.IsBlankRow row = match row.GetValue(0) with null -> true | _ -> false }
    excelReadRowsAsList helper (new SiteListTable())

let deleteAllData () : Script<int> = 
    liftAtomically <| deleteAllRowsRestartIdentity "spt_dwithin"


let private makeDWithinINSERT (row:SiteListRow) : string option = 
    let make1 (osgb36:OSGB36Point)  = 
        sqlINSERT "spt_dwithin" 
            <|  [ stringValue       "uid"               (row.``#SAINUMBER``.Trim())
                ; stringValue       "name"              (row.``#SITENAME``.Trim())
                ; stringValue       "function_type"     (row.``#ASSETTYPE``.Trim())
                ; stringValue       "osgb36_ref"        (row.``#GRIDREF``.Trim())
                ; literalValue      "location"          <| PostGIS.makeSTGeogFromTextPointLiteral (osgb36ToWGS84 osgb36)
                ]
    Option.map make1 <| tryReadOSGB36Point row.``#GRIDREF``


let insertRows (rows:seq<SiteListRow>) : Script<int> = 
    let proc1 (row:SiteListRow) : PGSQLConn<int> = 
        match makeDWithinINSERT row with
        | Some sql -> execNonQuery sql
        | None -> pgsqlConn.Return 0
    liftAtomically <| SLGeo.Base.PostGISConn.PGSQLConn.sumTraverseM proc1 rows


let SetupDB(password:string) : unit = 
    let rows = 
        List.distinctBy (fun (x:SiteListRow) -> x.``#SAINUMBER``) <|  getSiteListRows ()
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <|  scriptMonad { 
                let! _      = deleteAllData ()  |> logScript (sprintf "%i rows deleted")
                let! count  = insertRows rows   |> logScript (sprintf "%i rows inserted") 
                return count
                }   
                
/// Objects within distance.
let makeDWithinQUERY (point:WGS84Point)  (distance:float<meter>) : string = 
    System.String.Format("""
        SELECT 
            d.uid, d.name
        FROM 
            spt_dwithin d
        WHERE
            ST_DWithin(d.location, ST_Point({0}, {1}), {2}); 
        """, point.Longitude, point.Latitude, distance)


type NeighbourRec = 
    { Uid: string
      Name: string }

let neighboursWithin (point:WGS84Point) (distance:float<meter>) : Script<NeighbourRec list> = 
    let query = makeDWithinQUERY point distance
    let procM (reader:NpgsqlDataReader) : NeighbourRec = 
        { Uid   = reader.GetString(0)
        ; Name  = reader.GetString(1) }
    liftAtomically <| execReaderList query procM  

let csvHeaders = [ "Uid"; "Name"; "Neighbours within 25m"; "Neighbours within 1km" ]


let makeOutputRow (row:SiteListRow) : Script<RowWriter> =
    let removeSelf xs = List.filter (fun (x:NeighbourRec) -> x.Name <> row.``#SITENAME``) xs
    let getNeighbours (dist:float<meter>) (gridref:string) : Script<string list> = 
        match tryReadOSGB36Point row.``#GRIDREF`` with
        | None -> scriptMonad.Return []
        | Some osgb -> 
            ScriptMonad.fmapM (List.map (fun (x:NeighbourRec) -> x.Name) << removeSelf)
                <| neighboursWithin (osgb36ToWGS84 osgb) dist
    scriptMonad { 
        let! neighbours25m  = ScriptMonad.fmapM (String.concat "; ") <| getNeighbours 25.0<meter> row.``#GRIDREF``
        let! neighbours1k   = ScriptMonad.fmapM (String.concat "; ") <| getNeighbours 1000.0<meter> row.``#GRIDREF``
        return [ tellQuotedString   row.``#SAINUMBER``
               ; tellQuotedString   row.``#SITENAME``
               ; tellQuotedString   neighbours25m
               ; tellQuotedString   neighbours1k]
        }
        

let main (password:string) : unit = 
    let sites = getSiteListRows ()
    let outputFile = @"G:\work\Projects\events2\site-list-neighbours.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| scriptMonad { 
                let! rows = ScriptMonad.mapM makeOutputRow sites
                let csvProc:CsvOutput<unit> = 
                    writeRowsWithHeaders csvHeaders rows
                do (outputToNew {Separator=","} csvProc outputFile)
                return ()
                }
