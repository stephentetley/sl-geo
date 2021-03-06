﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause


namespace SLGeo.Base

open System

open Microsoft.FSharp.Core
open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

open SLGeo.Base.Coord
open SLGeo.Base.WellKnownText

// This is the coordinate system of OpenStreetMap data

[<AutoOpen>]
module WebMercator = 
    

    [<StructuredFormatDisplay("{WmEasting}E {WmNorthing}N:7483")>]
    type WMPoint = 
        { WmEasting : float<meter>
          WmNorthing : float<meter> }


    /// Projection used by Open Street Map and Google Maps
    /// There are several SRID for this system is ESPG:3857, ESPG:900913, ...
    type WebMercator = class end

    // ***** construction / Conversion *****
    let wktIsoWebMercator:WktCoordIso<WMPoint,WebMercator> = 
        { SRID = 7483
        ; Spheroid = "SPHEROID[\"WGS 84\",6378137,298.257223563]"
        ; ToWktCoord = 
            fun point -> 
                { WktLon = decimal point.WmEasting
                ; WktLat = decimal point.WmNorthing }
        ; FromWktCoord = 
            fun coord -> 
                { WmEasting = 1.0<meter> * float coord.WktLon
                ; WmNorthing = 1.0<meter> * float coord.WktLat }
        }


    let wmWktCoord (point:WMPoint) : WktCoord =
        { WktLon = decimal point.WmEasting; WktLat = decimal point.WmNorthing }

    let wktCoordToWM (coord:WktCoord) : WMPoint =
        { WmEasting = 1.0<meter> * float coord.WktLon
        ; WmNorthing = 1.0<meter> * float coord.WktLat }