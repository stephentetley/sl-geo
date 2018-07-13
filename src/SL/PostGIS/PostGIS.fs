﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SL.PostGIS.PostGIS

open Npgsql

open SL.Base.ErrorTrace
open SL.Base.AnswerMonad
open SL.Base.PGSQLConn
open SL.Geo.Coord
open SL.Geo.WellKnownText
open SL.PostGIS.ScriptMonad
// open SL.Base

// Common Script type for working with PGSQL connections
type Script<'a> = ScriptMonad<PGSQLConnParams,'a>

let liftResult (result:Result<'a>) : ScriptMonad<'r,'a> = 
    match result with
    | Success a -> sreturn a
    | Failure stk -> throwError (getErrorTrace stk)


let withConnParams (fn:PGSQLConnParams -> Script<'a>) : Script<'a> = 
    scriptMonad.Bind (ask (), fn)

let liftWithConnParams (fn:PGSQLConnParams -> Result<'a>) : Script<'a> = 
    withConnParams <| (liftResult << fn)

/// Note this is now a bad name as the underlying PGSQLConn functiona name
/// has changed.
let liftPGSQLConn (pgsql:PGSQLConn<'a>) : Script<'a> = 
    withConnParams <| fun conn -> liftResult <| atomically conn pgsql

let private singletonWithReader (query:string) (proc:NpgsqlDataReader -> 'a) : Script<'a> = 
    liftPGSQLConn <| execReaderSingleton query proc

let private singletonAsText1 (query:string) : Script<string> = 
    singletonWithReader query <| fun reader -> reader.GetString(0)


// TODO - how much of the PostGIS API can we wrap? (do we want to?) 
// e.g ST_Centroid, ST_AsGeoJSON, ST_GeomFromText

// pgr_tsp can't really be wrapped. It requires an existing table and data setup, 
// thus we consider it a script in itself.

// TODO - be more clever about the input and output arguments.
// Should be know if we are sending / receiving Geom, WKT (and WKB?) etc.

// In this module we shouldn't consider generating output for 
// e.g. QGIS Delimited Text imports. This is the domain of scripts. 
// Note though, for QGIS files with WKT can have any number of fields.


/// Generates ```ST_GeogFromText('SRID=4326;POINT(1.12345 2.12345')```
/// The name for this needs to be considered.
/// Favour calling ST_Point instead.
let makeSTGeogFromTextPointLiteral (pt:WGS84Point) : string = 
    sprintf "ST_GeogFromText('SRID=4326;%s')" << showWktPoint <| wgs84WktPoint pt

// (dict:WktCoordIso<'point,'srid>) (points:seq<'point>)

// ***** Distance Spheroid

// Absolutely must use ST_DistanceSpheroid!
let makeDistanceQUERY (dict:WktCoordIso<'point,'srid>) (point1:'point) (point2:'point) : string = 
    System.String.Format("""
        SELECT ST_DistanceSpheroid(
            ST_GeomFromText('{0}', {1}),
            ST_GeomFromText('{2}', {1}),
            '{3}');
        """, showWktPoint <| makeWktPoint dict point1
           , dict.SRID
           , showWktPoint <| makeWktPoint dict point2
           , dict.Spheroid )


let pgDistanceSpheroid (point1:WGS84Point) (point2:WGS84Point) : Script<float<kilometer>> = 
    let procM (reader:NpgsqlDataReader) : float<kilometer> = 
        0.001<kilometer> * (float <| reader.GetDouble(0))
    singletonWithReader (makeDistanceQUERY wktIsoWGS84 point1 point2) procM  


// ***** Concave and convex hulls


let private makeConvexHullQUERY (dict:WktCoordIso<'point,'srid>) (points:'point list) : string = 
    System.String.Format("""
        SELECT ST_AsText(ST_ConvexHull(
	        ST_Collect(
		        ST_GeomFromText('{0}')
                )) );
        """, showWktMultiPoint <| makeWktMultiPoint dict points )

// TODO - (Ideally) Move to this, potentially with a list of geom rather than a single one.
let private makeConvexHullQUERY2 (dict:WktCoordIso<'point,'srid>) (geom:WktGeometry<'srid>) : string = 
    System.String.Format("""
        SELECT ST_AsText(ST_ConvexHull(
	        ST_Collect(
		        ST_GeomFromText('{0}, {1}')
                )) );
        """, showWktGeometry geom, dict.SRID )


/// Returns WellKnownText.
/// May return different geometry types depending on number of points in the result set.
/// One point - POINT
/// Two points - LINESTRING
/// Three or more points - POLYGON
let pgConvexHull (dict:WktCoordIso<'point,'srid>) (points:'point list) : Script<WellKnownText<WGS84>> = 
    fmapM WellKnownText << singletonAsText1 <| makeConvexHullQUERY dict points
    

// Note TargetPercent of 1.0 gives a convex hull (0.9 seems okay)
let private makeConcaveHullQUERY (dict:WktCoordIso<'point,'srid>) (points:'point list) (targetPercent:float) : string = 
    System.String.Format("""
        SELECT ST_AsText(ST_ConcaveHull(
	        ST_Collect(
		        ST_GeomFromText('{0}')
                ), {1}) );
        """, showWktMultiPoint <| makeWktMultiPoint dict points
           , targetPercent)

/// Returns WellKnownText.
/// May return different geometry types depending on number of points in the result set.
/// One point - POINT
/// Two points - LINESTRING
/// Three or more points - POLYGON
///
/// TODO - need to return a composite geometry type rather then WellKnownText<WGS84>
let pgConcaveHull (dict:WktCoordIso<'point,'srid>) (points:'point list) (targetPercent:float) : Script<WellKnownText<WGS84>> = 
    fmapM WellKnownText << singletonAsText1 <| makeConcaveHullQUERY dict points targetPercent


// ***** Centroid

let private makeCentroidQUERY (dict:WktCoordIso<'point,'srid>) (points:seq<'point>) : string = 
    System.String.Format("""
        SELECT ST_AsText(ST_Centroid('{0}'));
        """, showWktMultiPoint <| makeWktMultiPoint dict points)


// TODO - pgCentroid should obviously return a point...
// Which in reality probably needs to be ``point option``.
// Ideally we shouldn't be fixed to WGS84Points.

let pgCentroidOld (points:WGS84Point list) : Script<string> = 
    singletonAsText1 <| makeCentroidQUERY wktIsoWGS84 points 

/// TODO - generalizing...
/// The input should not need to be a point list it should be a 
/// geometry like a polygon or a multipoint.
/// (or even a list of geometries?)
let pgCentroid (dict:WktCoordIso<'point,'srid>) (points:seq<'point>) : Script<'point option> = 
    scriptMonad {
        let! wkt = singletonAsText1 <| makeCentroidQUERY dict points 
        let optPoint = Option.bind (wktExtractPoint dict) <| tryReadWktPoint wkt
        return optPoint
        }

