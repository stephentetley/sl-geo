// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SL.Geo

open System
open System.Text.RegularExpressions

open Microsoft.FSharp.Core
open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

open SL.Geo


// TODO Transformations should use the proj4 binding (fs-proj4) when it is ready...

[<AutoOpen>]
module WGS84 = 
    open SL.Base
    open Npgsql



    /// World Geodetic System 1984             
    /// The SRID for this system is ESPG:4326
    type WGS84 = class end


    [<StructuredFormatDisplay("{Latitude}Lat {Longitude}Lon")>]
    type WGS84Point = 
        { Latitude : float<degree>
          Longitude : float<degree> }
        member point.ToWktPoint : WktPoint<WGS84> = 
                    WktPoint (Some { WktLon = decimal point.Longitude
                                   ; WktLat = decimal point.Latitude } )
        static member FromWktPoint (pt:WktPoint<WGS84>) = 
            match pt with
            | WktPoint None -> None
            | WktPoint (Some coord) -> Some ({ Longitude = 1.0<degree> * float coord.WktLon
                                             ; Latitude = 1.0<degree> * float coord.WktLat })


    let wktIsoWGS84:WktCoordIso<WGS84Point,WGS84> = 
        { SRID = 4326
        ; Spheroid = "SPHEROID[\"WGS 84\", 6378137, 298.257223563]"
        ; ToWktCoord = 
            fun point -> 
                { WktLon = decimal point.Longitude
                ; WktLat = decimal point.Latitude }
        ; FromWktCoord = 
            fun coord -> 
                { Longitude = 1.0<degree> * float coord.WktLon
                ; Latitude = 1.0<degree> * float coord.WktLat }
        }



    /// The WGS84 spheroid string for use in PostGIS queries (e.g ST_DistanceSpheroid).
    /// Note the value is not quoted, you will likely have to enclose it in single quotes
    /// when using it in a query.
    let wgs84Spheroid:string = "SPHEROID[\"WGS 84\", 6378137, 298.257223563]"

    /// This intentionally throws an exception if if cannot read the string.
    /// Read errors on NpgsqlDataReader should always be caught
    let readPgWGS84Point (reader:NpgsqlDataReader) (ordinal:int) : WGS84Point = 
        let str = reader.GetString(ordinal) 
        match tryReadWktPoint str with
        | None -> failwith "readPgWGS84Point error"
        | Some wkt -> 
            match WGS84Point.FromWktPoint wkt with
            | None -> failwith "readPgWGS84Point error"
            | Some pt -> pt
        