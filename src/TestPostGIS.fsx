﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause


#I @"..\packages\NetTopologySuite.Core.1.15.0\lib\net45"
#I @"..\packages\GeoAPI.Core.1.7.5\lib\net45"
#I @"..\packages\GeoAPI.CoordinateSystems.1.7.5\lib\net45"
#I @"..\packages\ProjNET4GeoAPI.1.4.0-pre001\lib\net45"
#I @"..\packages\System.Runtime.CompilerServices.Unsafe.4.5.0\lib\netstandard2.0"
#I @"..\packages\Npgsql.4.0.2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.5.0\lib\portable-net45+win8+wp8+wpa81"
#I @"..\packages\Npgsql.NetTopologySuite.1.0.1\lib\net45"
#r "GeoAPI"
#r "GeoAPI.CoordinateSystems"
#r "ProjNET"
#r "NetTopologySuite"
#r "Npgsql"
#r "Npgsql.NetTopologySuite"
open GeoAPI.Geometries
open GeoAPI
open ProjNet
open Npgsql
open NetTopologySuite.Geometries
open NetTopologySuite.IO
open Npgsql.NetTopologySuite

#I @"..\packages\FParsec.1.0.4-RC3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

#load @"SL\Base\ErrorTrace.fs"
#load @"SL\Base\PGSQLConn.fs"
#load @"SL\Geo\Base.fs"
#load @"SL\Geo\Tolerance.fs"
#load @"SL\Geo\WellKnownText.fs"
#load @"SL\Geo\WGS84.fs"
#load @"SL\Geo\OSGB36.fs"
#load @"SL\Geo\SRTransform.fs"
open SL.Base.PGSQLConn
open SL.Geo.Base
open SL.Geo.WGS84
open SL.Geo.OSGB36
open SL.Geo.SRTransform
open SL.Base
open NetTopologySuite.IO
open NetTopologySuite.IO

/// Work towards WebMercator <-> WGS84
let osgb36BaildonHillTP = {Easting=414124.69<meter>; Northing=440072.41<meter> }

let wgs84BaildonHillTP = { Longitude = -1.7867456<degree>; Latitude = 53.856689<degree> }

let test01 (password:string) : unit =
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let query = "SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),27700));"
    let procM = execReaderSingleton query (fun reader -> printfn "%A" reader.[0])
    atomically conn procM |> ignore


let test02 (password:string) : unit =
    let conn = pgsqlConnParamsTesting "spt_geo" password
    let query = "SELECT ST_AsText(ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),27700));"
    let procM = execReaderSingleton query (fun reader -> printfn "%s" (reader.GetString(0)) )
    atomically conn procM |> ignore

let test03 (password:string) : WGS84Point = 
    let conn = pgsqlConnParamsTesting "spt_geo" password
    match atomically conn (osgb36ToWGS84 osgb36BaildonHillTP) with
    | Success pt -> pt
    | Failure trace-> failwith (ErrorTrace.getErrorTrace trace)

//let test04 (password:string) : Result<Point> =
//    let conn = pgsqlConnParamsTesting "spt_geo" password
//    let query = "SELECT ST_Transform(ST_GeomFromText('POINT(-1.7867456 53.856689)',4326),27700);"
//    let procM = execReaderSingleton query (fun reader -> reader.getString(0) ) |>> 
//    atomically conn procM 

let test05 () : string = 
    let pt:Point = new Point(new Coordinate(-1.7867456, 53.856689))
    pt.SRID <- 4326
    pt.AsText()

let test06 ()  = 
    let pt:Point = WKTReader().Read("POINT (-1.7867456 53.856689)") :?> Point
    pt.X, pt.Y

let OSGB36Wkt : string = 
    """
    PROJCS["OSGB 1936 / British National Grid",
        GEOGCS["OSGB 1936",
            DATUM["OSGB_1936",
                SPHEROID["Airy 1830",6377563.396,299.3249646,
                    AUTHORITY["EPSG","7001"]],
                TOWGS84[446.448,-125.157,542.06,0.15,0.247,0.842,-20.489],
                AUTHORITY["EPSG","6277"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.0174532925199433,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4277"]],
        PROJECTION["Transverse_Mercator"],
        PARAMETER["latitude_of_origin",49],
        PARAMETER["central_meridian",-2],
        PARAMETER["scale_factor",0.9996012717],
        PARAMETER["false_easting",400000],
        PARAMETER["false_northing",-100000],
        UNIT["metre",1,
            AUTHORITY["EPSG","9001"]],
        AXIS["Easting",EAST],
        AXIS["Northing",NORTH],
        AUTHORITY["EPSG","27700"]]
    """


// see
// https://gis.stackexchange.com/questions/165022/how-do-i-transform-a-point-using-nettopologysuite
let test07 () = 
    let csFactory = new ProjNet.CoordinateSystems.CoordinateSystemFactory ()
    let ctFactory = new ProjNet.CoordinateSystems.Transformations.CoordinateTransformationFactory ()

    /// UK is UTM Zone 30N (N=North)

    let wgs84 = CoordinateSystems.ProjectedCoordinateSystem.WGS84_UTM(zone = 30, zoneIsNorth = true)
    let osgb36 = csFactory.CreateFromWkt(OSGB36Wkt)

    let transform = ctFactory.CreateFromCoordinateSystems(sourceCS = osgb36, targetCS = wgs84)

    /// let c1 = new Coordinate(-1.7867456, 53.856689)
    let c1 = new Coordinate(414124.69, 440072.41)

    let c2 = transform.MathTransform.Transform(c1)
    c2
