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

#load @"SL\Base\ErrorTrace.fs"
#load @"SL\Base\SQLUtils.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Base\ExcelProviderHelper.fs"
#load @"SL\Base\CsvOutput.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\Coord.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\PostGIS\ScriptMonad.fs"
#load @"SL\PostGIS\PostGIS.fs"
#load @"SL\Scripts\NearestHospital.fs"
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base.CsvOutput
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.ScriptMonad
open SL.Scripts.NearestHospital



let SetupDB(password:string) : unit = 
    let rows = getHospitalImportRows ()
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runScript (failwith) (printfn "Success: %i modifications") (consoleLogger) conn 
        <| insertHospitals MakeDict rows 



// Note - input file has bad initial rows stopping type interence (???)
//type AssetDataset = 
//    CsvProvider< @"G:\work\Projects\rtu\peer-to-peer.csv",
//                 HasHeaders = true >

//type AssetRow = AssetDataset.Row

//let readAssetRows () : AssetRow list = (new AssetDataset()).Rows |> Seq.toList

type AssetTable = 
    ExcelFile< @"G:\work\Projects\rtu\AR-asset-expired-mmims-c2010\SiteList-2010-2011-2012.xlsx",
                SheetName = "Sites_2010",
                ForceString = true >

type AssetRow = AssetTable.Row

let getAssetRows () : AssetRow list= 
    let dict : GetRowsDict<AssetTable, AssetRow> = 
        { GetRows     = fun imports -> imports.Data 
          NotNullProc = fun row -> match row.GetValue(0) with null -> false | _ -> true }
    excelTableGetRowsSeq dict (new AssetTable()) |> Seq.toList

let nearestMethodDict : NearestHospitalDict<AssetRow>  = 
    let extractLocation (row:AssetRow) : WGS84Point option = 
        Option.map osgb36ToWGS84 <| tryReadOSGB36Point row.``Grid Ref``

    let outputRow (row:AssetRow) (optBest : BestMatch option) : SL.Scripts.CsvOutput.RowWriter = 
        match optBest with
        | None -> 
            printfn "no best: %s %s" row.``Site Name`` row.``Grid Ref``
            [ SL.Scripts.CsvOutput.tellString row.``Sai Number``
            ; SL.Scripts.CsvOutput.tellString row.``Site Name`` ] 
        | Some bestMatch -> 
            let hospitalLine = 
                sprintf "%s, %s, %s. Tel: %s" 
                        bestMatch.NearestHospital.HospitalName
                        bestMatch.NearestHospital.Address
                        bestMatch.NearestHospital.Postcode
                        bestMatch.NearestHospital.Phone
            [ tellQuotedString      row.``Sai Number``
            ; tellQuotedString      row.``Site Name``
            ; tellQuotedString      bestMatch.NearestHospital.HospitalName
            ; tellQuotedString      hospitalLine
            ; tellFloat             <| float bestMatch.Distance]

    { CsvHeaders = [ "Uid"; "Name"; "Hospital"; "Hospital Details"; "Distance" ]
    ; ExtractLocation = extractLocation
    ; OutputCsvRow = outputRow
    } 

let main (password:string) : unit = 
    let assetData = getAssetRows ()
    let outputFile = @"G:\work\Projects\rtu\AR-asset-expired-mmims-c2010\sites2010-hopitals.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| generateNearestHospitalsCsv nearestMethodDict assetData outputFile
        

