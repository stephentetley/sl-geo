// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SLGeo.Base

open System

open Microsoft.FSharp.Core

[<AutoOpen>]
module Base = 


    [<Measure>]
    type kilometer

    // type metres = double    
    
    [<Measure>]
    type degree

    [<Measure>]
    type radian

    let inline degreeToRadian (d : float<degree>) : float<radian> = 
        1.0<radian> * (Math.PI/180.0) * float d

    let inline radianToDegree (r : float<radian>) : float<degree> = 
        1.0<degree> * (180.0/Math.PI) * float r

    let inline private deg2rad (d : float) : float = (Math.PI/180.0) * d

    let inline private rad2deg (r : float) : float = (180.0/Math.PI) * r

    /// fromDMS :: Int -> Int -> Double -> DDegrees
    let makeDegree (d : int) (m : int) (s : float) : float<degree> = 
        LanguagePrimitives.FloatWithMeasure (float d + float m / 60.0 + s / 3600.0)

