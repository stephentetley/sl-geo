// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SL.Geo


open Microsoft.FSharp.Core


open SL.Base.PGSQLConn
open SL.Geo.WellKnownText
open SL.Geo.WGS84
open SL.Geo.OSGB36



// Spatial Reference transformations - performed by PostGIS

module SRTransform = 
    open System

    // PGSQLConn

    let private pointToPoint (source:WktPoint<'srid1>) (srid1:int) (srid2:int) : PGSQLConn<WktPoint<'srid2>> = 
        let query =     
            sprintf "SELECT ST_AsText(ST_Transform(ST_GeomFromText('%s', %d), %d));" (showWktPoint source) srid1 srid2

        let proc : PGSQLConn<string> = execReaderSingleton query (fun reader -> reader.GetString(0) )
        printfn "%s" query
        proc >>= fun txt ->
            match tryReadWktPoint txt with
            | Some pt -> pgreturn pt
            | None -> throwError (sprintf "pointToPoint - decoding failed: '%s'" txt)



    // SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),4326));

    let osgb36ToWGS84 (osgb36:OSGB36Point) : PGSQLConn<WGS84Point> = 
        pointToPoint osgb36.ToWktPoint 27700 4326 >>= fun wkt ->  
            match WGS84Point.FromWktPoint wkt with
            | Some pt -> pgreturn pt
            | None -> throwError "osgb36ToWGS84 - decoding failed"


