// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

#I @"..\packages\FSharp.Data.3.0.0-beta3\lib\net45"
#r @"FSharp.Data.dll"
open FSharp.Data

#I @"..\packages\Npgsql.4.0.0-preview2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.4.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql

#I @"..\packages\FParsec.1.0.3\lib\net40-client"
#r "FParsec"
#r "FParsecCS"

#load @"SL\Base\SQLUtils.fs"
#load @"SL\Base\AnswerMonad.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\Coord.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\PostGIS\ScriptMonad.fs"
#load @"SL\PostGIS\PostGIS.fs"
#load @"SL\Scripts\CsvOutput.fs"
#load @"SL\Scripts\NearestHospital.fs"
open SL.Base.PGSQLConn
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.Scripts.CsvOutput
open SL.PostGIS.ScriptMonad
open SL.Scripts.NearestHospital



let SetupDB(password:string) : unit = 
    let rows = getHospitalImportRows ()
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runScript (failwith) (printfn "Success: %i modifications") (consoleLogger) conn 
        <| insertHospitals MakeDict rows 



// Note - input file has bad initial rows stopping type interence (???)
type AssetDataset = 
    CsvProvider< @"G:\work\Projects\rtu\peer-to-peer.csv",
                 HasHeaders = true >

type AssetRow = AssetDataset.Row

let readAssetRows () : AssetRow list = (new AssetDataset()).Rows |> Seq.toList



let nearestMethodDict : NearestHospitalDict<AssetRow>  = 
    let extractLocation (row:AssetRow) : WGS84Point option = 
        Option.map osgb36ToWGS84 <| tryReadOSGB36Point row.``Grid Reference``

    let outputRow (row:AssetRow) (optBest : BestMatch option) : SL.Scripts.CsvOutput.RowWriter = 
        match optBest with
        | None -> 
            [ SL.Scripts.CsvOutput.tellString row.Reference
            ; SL.Scripts.CsvOutput.tellString row.``Common Name`` ] 
        | Some bestMatch -> 
            let hospitalLine = 
                sprintf "%s, %s, %s. Tel: %s" 
                        bestMatch.NearestHospital.HospitalName
                        bestMatch.NearestHospital.Address
                        bestMatch.NearestHospital.Postcode
                        bestMatch.NearestHospital.Phone
            [ tellQuotedString      row.Reference
            ; tellQuotedString      row.``Common Name``
            ; tellQuotedString      bestMatch.NearestHospital.HospitalName
            ; tellQuotedString      hospitalLine
            ; tellFloat             <| float bestMatch.Distance]

    { CsvHeaders = [ "Uid"; "Name"; "Hospital"; "Hospital Details"; "Distance" ]
    ; ExtractLocation = extractLocation
    ; OutputCsvRow = outputRow
    } 

let main (password:string) : unit = 
    let assetData = readAssetRows ()
    let outputFile = @"G:\work\Projects\rtu\p2p-sites-with-hospital2.csv"
    let conn = pgsqlConnParamsTesting "spt_geo" password
    runConsoleScript (printfn "Success: %A") conn 
        <| generateNearestHospitalsCsv nearestMethodDict assetData outputFile
        

