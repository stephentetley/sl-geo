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

#load "..\src\SLGeo\Base\PostGISConn\ErrorTrace.fs"
#load "..\src\SLGeo\Base\PostGISConn\SqlUtils.fs"
#load "..\src\SLGeo\Base\PostGISConn\PGSQLConn.fs"
#load "..\src\SLGeo\Extra\CsvOutput.fs"
#load "..\src\SLGeo\Extra\ExcelProviderHelper.fs"
#load "..\src\SLGeo\Base\Base.fs"
#load "..\src\SLGeo\Base\WellKnownText.fs"
#load "..\src\SLGeo\Base\WGS84.fs"
#load "..\src\SLGeo\Base\OSGB36.fs"
#load "..\src\SLGeo\Base\SRTransform.fs"
#load "..\src\SLGeo\Shell\ScriptMonad.fs"
#load "..\src\SLGeo\Shell\PostGIS.fs"
#load "..\src\SLGeo\Scripts\NearestHospital.fs"
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.ExcelProviderHelper
open SLGeo.Extra
open SLGeo.Base
open SLGeo.Base.WGS84
open SLGeo.Base.OSGB36
open SLGeo.Base.SRTransform
open SLGeo.Shell.ScriptMonad
open SLGeo.Scripts.NearestHospital



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
        

