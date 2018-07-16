// Copyright (c) Stephen Tetley 2018
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
open SL.Geo.Base
open SL.Geo.WGS84
open SL.Geo.OSGB36


/// Work towards WebMercator <-> WGS84
let osgb36BaildonHillTP = {Easting=414124.69<meter>; Northing=440072.41<meter> }

let wgs84BaildonHillTP = { Longitude = -1.7867456<degree>; Latitude = 53.856689<degree> }

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
let test01 () = 
    let csFactory = new ProjNet.CoordinateSystems.CoordinateSystemFactory ()
    let ctFactory = new ProjNet.CoordinateSystems.Transformations.CoordinateTransformationFactory ()

    /// UK is UTM Zone 30N (N=North)

    let wgs84 = CoordinateSystems.ProjectedCoordinateSystem.WGS84_UTM(zone = 30, zoneIsNorth = true)
    let osgb36 = csFactory.CreateFromWkt(OSGB36Wkt)

    let osgb36ToWgs84 = ctFactory.CreateFromCoordinateSystems(sourceCS = osgb36, targetCS = wgs84)
    let wgs84ToOsgb36 = ctFactory.CreateFromCoordinateSystems(sourceCS = wgs84, targetCS = osgb36)


    let coordWgs84 = new Coordinate(-1.7867456, 53.856689)
    let coordOsgb36 = new Coordinate(414124.69, 440072.41)

    let ansWgs84 = osgb36ToWgs84.MathTransform.Transform(coordOsgb36)
    let ansOsgb36 = wgs84ToOsgb36.MathTransform.Transform(coordWgs84)
    printfn "osgb36ToWgs84.TransformType = %A" osgb36ToWgs84.TransformType 
    

    ansWgs84, ansOsgb36


let test02 () = 
    let csFact = new ProjNet.CoordinateSystems.CoordinateSystemFactory ()
    let ctFact = new ProjNet.CoordinateSystems.Transformations.CoordinateTransformationFactory ()

    let utm35ETRS : CoordinateSystems.ICoordinateSystem = 
        csFact.CreateFromWkt(
            "PROJCS[\"ETRS89 / ETRS-TM35\",GEOGCS[\"ETRS89\",DATUM[\"D_ETRS_1989\",SPHEROID[\"GRS_1980\",6378137,298.257222101]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\",0.017453292519943295]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",27],PARAMETER[\"scale_factor\",0.9996],PARAMETER[\"false_easting\",500000],PARAMETER[\"false_northing\",0],UNIT[\"Meter\",1]]" )

    let utm33 : CoordinateSystems.IProjectedCoordinateSystem  = 
        CoordinateSystems.ProjectedCoordinateSystem.WGS84_UTM(33, true);

    let trans : CoordinateSystems.Transformations.ICoordinateTransformation = 
        ctFact.CreateFromCoordinateSystems(utm35ETRS, utm33);

    let c1 = new Coordinate(290586.087, 6714000.0)
    
    let ans = trans.MathTransform.Transform(c1) 
    ans 

