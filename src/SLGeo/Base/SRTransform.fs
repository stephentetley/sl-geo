﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SLGeo.Base


open Microsoft.FSharp.Core


open SLGeo.Base.PostGISConn.PGSQLConn
open SLGeo.Base.WellKnownText
open SLGeo.Base.WGS84
open SLGeo.Base.OSGB36



// Spatial Reference transformations - performed by PostGIS

[<AutoOpen>]
module SRTransform = 
    open System

    // TODO - PGSQLConn is the wrong monad, it should be Script
    // Even though we only need the power of PGSQLConn, letting it
    // escape into user code makes a very confusing API.

    let private pointToPoint (source:WktPoint<'srid1>) (srid1:int) (srid2:int) : PGSQLConn<WktPoint<'srid2>> = 
        let query =     
            sprintf "SELECT ST_AsText(ST_Transform(ST_GeomFromText('%s', %d), %d));" (showWktPoint source) srid1 srid2

        let proc : PGSQLConn<string> = execReaderSingleton query (fun reader -> reader.GetString(0) )
        printfn "%s" query
        proc >>>= fun txt ->
            match tryReadWktPoint txt with
            | Some pt -> pgreturn pt
            | None -> throwError (sprintf "pointToPoint - decoding failed: '%s'" txt)


    let osgb36ToWGS84 (osgb36:OSGB36Point) : PGSQLConn<WGS84Point> = 
        pointToPoint osgb36.ToWktPoint 27700 4326 >>>= fun wkt ->  
            match WGS84Point.FromWktPoint wkt with
            | Some pt -> pgreturn pt
            | None -> throwError "osgb36ToWGS84 - decoding failed"

    let wgs84ToOSGB36 (pt:WGS84Point) : PGSQLConn<OSGB36Point> = 
        pointToPoint pt.ToWktPoint 4326 27700  >>>= fun wkt ->  
            match OSGB36Point.FromWktPoint wkt with
            | Some pt -> pgreturn pt
            | None -> throwError "wgs84ToOSGB36 - decoding failed"

