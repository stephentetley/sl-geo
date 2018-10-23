// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause


#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FSharp.Data.3.0.0-beta4\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data


#I @"..\packages\ExcelProvider.0.8.2\lib"
#r "ExcelProvider.dll"
open FSharp.ExcelProvider

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

#load "..\SLGeo\SL\Base\ErrorTrace.fs"
#load "..\SLGeo\SL\Base\SqlUtils.fs"
#load "..\SLGeo\SL\Base\PGSQLConn.fs"
#load "..\SLGeo\SL\Base\CsvOutput.fs"
#load "..\SLGeo\SL\Base\ExcelProviderHelper.fs"
#load "..\SLGeo\SL\Geo\Tolerance.fs"
#load "..\SLGeo\SL\Geo\Coord.fs"
#load "..\SLGeo\SL\Geo\WellKnownText.fs"
#load "..\SLGeo\SL\Geo\OSGB36.fs"
#load "..\SLGeo\SL\Geo\WGS84.fs"
#load "..\SLGeo\SL\Geo\SRTransform.fs"
#load "..\SLGeo\SL\PostGIS\ScriptMonad.fs"
#load "..\SLGeo\SL\PostGIS\PostGIS.fs"
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.CsvOutput
open SL.Geo
open SL.PostGIS.ScriptMonad
open SL.PostGIS.PostGIS

type OutstationData = 
    CsvProvider< @"G:\work\Projects\flood_manholes\RTS-export-for-Parent_OU.trim.csv",
                 HasHeaders = true>

type OutstationRow = OutstationData.Row

let readOutstations () : seq<OutstationRow> = (new OutstationData ()).Rows |> Seq.cast<OutstationRow>

type OutstationRecord = 
    { OsName: string
      OdName: string
      SetName: string
      ParentOU: string
      ParentOUComment: string
      GridRefText: string }





// POINT(Lon lat)
let geomFromText (source:string) : Script<string option> = 
    match tryReadOSGB36Point source with
    | None -> sreturn None
    | Some osgb36 -> 
        liftAtomically (osgb36ToWGS84 osgb36) >>= fun (wgs:WGS84Point) -> 
        sreturn (Some <| sprintf "ST_GeomFromText('POINT(%f %f)', 4326)" wgs.Longitude wgs.Latitude)



let makeInsert (row:OutstationRow) : Script<string option> = 
    let sk (wkt:string) : string = 
        sprintf "INSERT INTO spt_outstations (os_name, od_name, set_name, parent_ou, parent_ou_comment, osgb36_text, os_location) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s) ;"
                row.``OS name``
                row.``OD name``
                row.``Set name``
                row.``Parent OU``
                row.``Parent OU Comment``
                row.``Grid ref``
                wkt

    geomFromText row.``Grid ref`` |>> Option.map sk
    
let genDbInsertScript (password:string) : unit = 
    let script = 
         scriptMonad { 
            let! xs = readOutstations () |> Seq.toList |> mapM makeInsert
            let ys = Seq.choose id xs 
            return (String.concat "\n" ys)
        }

    let conn = pgsqlConnParamsTesting "spt_geo" password
    let sk (sql:string) : unit = 
        System.IO.File.WriteAllText(@"D:\coding\fsharp\sl-geo\sql\temp_os_insert.sql", sql)
    runConsoleScript (sk) conn script
        
        
/// Limit is closest N neighbours, probably should be fixed to 1
/// for this use-case.
let makeNearestNeighbourQUERY (limit:int) (point:WGS84Point) : string = 
    System.String.Format("""
        SELECT 
            os_name, od_name, set_name, parent_ou, parent_ou_comment, osgb36_text, ST_AsText(os_location)
        FROM 
            spt_outstations 
        ORDER BY os_location <-> ST_Point({0}, {1}) LIMIT {2} ;
        """, point.Longitude, point.Latitude, limit)


let nearestExistingOutstationList (point:WGS84Point) : Script<OutstationRecord list> = 
    let query = makeNearestNeighbourQUERY 1 point
    let procM (reader:NpgsqlDataReader) : OutstationRecord = 
        { OsName            = reader.GetString(0)
        ; OdName            = reader.GetString(1)
        ; SetName           = reader.GetString(2)
        ; ParentOU          = reader.GetString(3) 
        ; ParentOUComment   = reader.GetString(4)
        ; GridRefText       = reader.GetString(5) }
    // printfn "***** QUERY:\n%s" query
    liftAtomically <| execReaderList query procM  


let nearestExistingOutstation (point:WGS84Point) : Script<OutstationRecord option> = 
    let first xs = match xs with | x :: _ -> Some x; | [] -> None
    SL.PostGIS.ScriptMonad.fmapM first <| nearestExistingOutstationList point

let getNearestExistingOutstation (source:string) : Script<OutstationRecord option> = 
    match tryReadOSGB36Point source with
    | None -> sreturn None
    | Some osgb36 -> 
        liftAtomically (osgb36ToWGS84 osgb36) >>= fun (wgsPt:WGS84Point) -> 
        nearestExistingOutstation wgsPt

let temp02 (password:string) = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| getNearestExistingOutstation "SE0252744901"

type SitesTable = 
    ExcelFile< FileName = @"G:\work\Projects\flood_manholes\S4811-Site_List_Initial_141.xlsx",
                SheetName = "Sites",
                HasHeaders = true,
                ForceString = true>

type SiteRow = SitesTable.Row

let readSiteRows () : SiteRow list = 
    let helper = 
        { new IExcelProviderHelper<SitesTable,SiteRow>
          with member this.ReadTableRows table = table.Data 
               member this.IsBlankRow row = match row.GetValue(0) with null -> true | _ -> false }
    excelReadRowsAsList helper (new SitesTable())

let tellOutputRow (row:SiteRow, neighbour:OutstationRecord option) : CellWriter list = 
    let extractN (fn:OutstationRecord -> string) (v:OutstationRecord option) : string =
        match v with
        | None -> ""
        | Some o -> fn o
    [ tellString row.``STC25 Ref``
    ; tellString row.``Region (Stantec)``
    ; tellString row.``Grid Ref (NGR)``
    ; tellString <| extractN (fun v -> v.OsName ) neighbour
    ; tellString <| extractN (fun v -> v.SetName) neighbour
    ; tellString <| extractN (fun v -> v.ParentOU) neighbour
    ; tellString <| extractN (fun v -> v.ParentOUComment) neighbour
    ]

let csvHeaders : string list = 
    [ "STC25 Ref"
    ; "Region (Stantec)"
    ; "Grid Ref (NGR)"
    ; "Neighbour Name"
    ; "Neighbour Set"
    ; "Neighbour Parent_OU"
    ; "Neighbour Parent_OU_Comment"
    ]

let OutputSites(password:string) : unit = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let outFile = @"G:\work\Projects\flood_manholes\manhole-neighbours.csv"

    runConsoleScript (printfn "Success: %A") conn 
        <| scriptMonad { 
                let rows1:SiteRow list = readSiteRows ()
                let! rows2 = 
                    forM rows1 (fun (manhole:SiteRow) -> 
                                    getNearestExistingOutstation manhole.``Grid Ref (NGR)`` >>= fun ans ->
                                    sreturn (manhole, ans)  )
                let csvProc:CsvOutput<unit> = writeRecordsWithHeaders csvHeaders rows2 tellOutputRow
                do (outputToNew {Separator=","} csvProc outFile)
                return ()
                }