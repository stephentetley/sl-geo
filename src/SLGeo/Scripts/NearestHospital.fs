// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SLGeo.Scripts.NearestHospital

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

open FSharp.Data
open Npgsql


open SLGeo.Base.PostGISConn.SqlUtils
open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Extra.CsvOutput
open SLGeo.Extra
open SLGeo.Base
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell.PostGIS
open SLGeo.Shell

// Use PostGIS for nearest neighour and distance.


/// Hard dependency on input data (though this file is in the project and Git)
/// Another user could set up the DB with a different data set.
type HospitalsData = 
    CsvProvider< @"..\data\Accident-and-Emergency-Hospitals-Yorkshire.csv",
                 HasHeaders = true>

type HospitalsRow = HospitalsData.Row


let getHospitalImportRows () : seq<HospitalsRow> = 
    (new HospitalsData ()).Rows |> Seq.cast<HospitalsRow>



let deleteAllData () : Script<int> = 
    liftAtomically <| deleteAllRowsRestartIdentity "spt_hospitals"



[<StructuredFormatDisplay("{Name} ({LatLon})")>]
type HospitalRecord = 
    { HospitalName: string
      Address: string
      Phone: string
      Postcode: string
      GridRef: WGS84Point }

type HospitalInsertDict<'inputrow> = 
    { tryMakeHospitalRecord : 'inputrow -> Script<HospitalRecord option> }

let tryMakeRecord (row:HospitalsRow) : Script<HospitalRecord option> = 
    match tryReadOSGB36Point row.``Grid Reference`` with
    | Some osgbPt ->
        liftAtomically (osgb36ToWGS84 osgbPt) >>= fun gridRef -> 
        sreturn (Some { HospitalName  = row.Name
                      ; Address       = row.Address
                      ; Phone         = row.Telephone
                      ; Postcode      = row.Postcode
                      ; GridRef       = gridRef })
    | _ -> sreturn None

let MakeDict : HospitalInsertDict<HospitalsRow> = { tryMakeHospitalRecord = tryMakeRecord }


let private makeHospitalINSERT (hospital:HospitalRecord) : string = 
    sqlINSERT "spt_hospitals" 
        <|  [ stringValue       "name"              hospital.HospitalName
            ; stringValue       "telephone"         hospital.Phone
            ; stringValue       "address"           hospital.Address
            ; stringValue       "postcode"          hospital.Postcode
            ; literalValue      "grid_ref"          <| makeSTGeogFromTextPointLiteral hospital.GridRef.ToWktPoint
            ]


let insertHospitals (dict:HospitalInsertDict<'inputrow>) (outfalls:seq<'inputrow>) : Script<int> = 
    let proc1 (row:'inputrow) : Script<int> = 
        dict.tryMakeHospitalRecord row >>= fun opt ->
        match opt with
        | Some vertex -> liftAtomically (execNonQuery <| makeHospitalINSERT vertex)
        | None -> sreturn 0
    sumTraverseM proc1 outfalls


let SetupHospitalDB (dict:HospitalInsertDict<'inputrow>) (hospitals:seq<'inputrow>) : Script<int> = 
    scriptMonad { 
        let! _      = deleteAllData ()                  |> logScript (sprintf "%i rows deleted")
        let! count  = insertHospitals dict hospitals    |> logScript (sprintf "%i rows inserted") 
        return count
     }


/// Limit is closest N neighbours, probably should be fixed to 1
/// for this use-case.
let makeNearestNeighbourQUERY (limit:int) (point:WGS84Point) : string = 
    System.String.Format("""
        SELECT 
            name, telephone, address, postcode, ST_AsText(grid_ref)
        FROM 
            spt_hospitals 
        ORDER BY grid_ref <-> ST_Point({0}, {1}) LIMIT {2} ;
        """, point.Longitude, point.Latitude, limit)




type BestMatch = 
    { NearestHospital: HospitalRecord
      Distance: float<kilometer> }


let nearestHospitalQuery (point:WGS84Point) : Script<HospitalRecord list> = 
    let query = makeNearestNeighbourQUERY 1 point
    let procM (reader:NpgsqlDataReader) : HospitalRecord = 
        let gridRef = readPgWGS84Point reader 4
        { HospitalName  = reader.GetString(0)
        ; Phone         = reader.GetString(1)
        ; Address       = reader.GetString(2) 
        ; Postcode      = reader.GetString(3)
        ; GridRef       = gridRef }
    // printfn "***** QUERY:\n%s" query
    liftAtomically <| execReaderList query procM  


let nearestHospitalToPoint (point:WGS84Point) : Script<HospitalRecord option> = 
    let first xs = match xs with | x :: _ -> Some x; | [] -> None
    ScriptMonad.fmapM first <| nearestHospitalQuery point



let nearestHospital (extractLoc:'asset -> Script<WGS84Point>) (asset:'asset) : Script<HospitalRecord option> = 
       extractLoc asset >>= nearestHospitalToPoint
           
    
let nearestHospitals (extractLoc:'asset -> Script<WGS84Point>) (assets:seq<'asset>) : Script<seq<'asset * HospitalRecord option>> = 
    let proc1 (asset:'asset) = 
        scriptMonad { 
            // optional is the wrong combinator here, it masks hard failures
            // that need to be seen (e.g DB connect failure).
            let! hospital = nearestHospital extractLoc asset
            return asset, hospital
            }
    ScriptMonad.traverseM proc1 assets
    

// "End-to-end" procedure for finding and outputing nearest hospitals.
// We don't set up the DB as we expect the DB to be long lived.

type NearestHospitalDict<'asset> = 
    { CsvHeaders: string list
      ExtractLocation: 'asset -> Script<WGS84Point>
      OutputCsvRow: 'asset -> BestMatch option -> RowWriter }



let generateNearestHospitalsCsv (dict:NearestHospitalDict<'asset>) (source:'asset list) (outputFile:string) : Script<unit> =
    
    let rowProc (asset1:'asset, opt:HospitalRecord option) : Script<RowWriter> =
        let noBest = dict.OutputCsvRow asset1 None
        replaceFailure noBest <| scriptMonad { 
            let! hospital   = liftOption opt
            let! assetLoc   = dict.ExtractLocation asset1
            let! distance   = pgDistanceSpheroid assetLoc hospital.GridRef
            let best        = Some <| { NearestHospital = hospital; Distance=distance }
            return (dict.OutputCsvRow asset1 best)
        }

    scriptMonad { 
        let! xs = nearestHospitals dict.ExtractLocation source
        let! (rowWriters:seq<RowWriter>) = ScriptMonad.traverseM rowProc xs
        let csvProc:CsvOutput<unit> = writeRowsWithHeaders dict.CsvHeaders rowWriters
        do (CsvOutput.outputToNew {Separator=","} csvProc outputFile)
        return ()
    }



