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
#load "..\src\SL\Base\SQLUtils.fs"
#load "..\src\SL\Base\PGSQLConn.fs"
#load "..\src\SL\Base\ExcelProviderHelper.fs"
#load "..\src\SL\Base\CsvOutput.fs"
#load "..\src\SL\Geo\Base.fs"
#load "..\src\SL\Geo\WellKnownText.fs"
#load "..\src\SL\Geo\WGS84.fs"
#load "..\src\SL\Geo\OSGB36.fs"
#load "..\src\SL\Geo\SRTransform.fs"
#load "..\src\SL\PostGIS\ScriptMonad.fs"
#load "..\src\SL\PostGIS\PostGIS.fs"
#load "..\src\SL\Scripts\NearestHospital.fs"
open SL.Base.PGSQLConn
open SL.Base.ExcelProviderHelper
open SL.Base
open SL.Geo.WGS84
open SL.Geo.OSGB36
open SL.Geo.SRTransform
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
    ExcelFile< FileName = @"G:\work\Projects\rtu\MK5 MMIM Replacement\SiteList-2010-2011-2012.xlsx",
                SheetName = "Sites_2012",
                HasHeaders = true,
                ForceString = true>

type AssetRow = AssetTable.Row

let getAssetRows () : AssetRow list= 
    let dict : GetRowsDict<AssetTable, AssetRow> = 
        { GetRows     = fun imports -> imports.Data 
          NotNullProc = fun row -> match row.GetValue(0) with null -> false | _ -> true }
    excelTableGetRowsSeq dict (new AssetTable()) |> Seq.toList

let tempZ (row:AssetRow) : unit = 
    printfn "%s" (row.GetValue(0) :?> string)

let nearestMethodDict : NearestHospitalDict<AssetRow>  = 
    let extractLocation (row:AssetRow) : Script<WGS84Point> = 
        match tryReadOSGB36Point row.``Grid Ref`` with
        | None -> throwError "extractLocation"
        | Some osgb36Pt -> liftAtomically (osgb36ToWGS84 osgb36Pt)

    let outputRow (row:AssetRow) (optBest : BestMatch option) : CsvOutput.RowWriter = 
        match optBest with
        | None -> 
            printfn "no best: %s %s" row.``Site Name`` row.``Grid Ref``
            [ CsvOutput.tellString row.``Sai Number``
            ; CsvOutput.tellString row.``Site Name`` ] 
        | Some bestMatch -> 
            let hospitalLine = 
                sprintf "%s, %s, %s. Tel: %s" 
                        bestMatch.NearestHospital.HospitalName
                        bestMatch.NearestHospital.Address
                        bestMatch.NearestHospital.Postcode
                        bestMatch.NearestHospital.Phone
            [ CsvOutput.tellQuotedString      row.``Sai Number``
            ; CsvOutput.tellQuotedString      row.``Site Name``
            ; CsvOutput.tellQuotedString      bestMatch.NearestHospital.HospitalName
            ; CsvOutput.tellQuotedString      hospitalLine
            ; CsvOutput.tellFloat             <| float bestMatch.Distance]

    { CsvHeaders = [ "Uid"; "Name"; "Hospital"; "Hospital Details"; "Distance" ]
    ; ExtractLocation = extractLocation
    ; OutputCsvRow = outputRow
    } 

let main (password:string) : unit = 
    let assetData = getAssetRows ()
    let outputFile = @"G:\work\Projects\rtu\MK5 MMIM Replacement\sites2012-hopitals.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| generateNearestHospitalsCsv nearestMethodDict assetData outputFile
        

