// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SL.Geo

open System
open System.Text.RegularExpressions

open Microsoft.FSharp.Core
open Microsoft.FSharp.Data.UnitSystems.SI.UnitNames

open SL.Geo.WellKnownText


// TODO Transformations should use the proj4 binding (fs-proj4) when it is ready...

// Coordinate transformations from 
// A guide to coordinate systems in Great Britain (v2.4)
// Ordnance Survey
// Ref D00659

[<AutoOpen>]
module OSGB36 = 


    /// Ordinance Survey Great Britain National Grid reference system 
    /// The SRID for this system is ESPG:27700
    type OSGB36 = class end
    
    // Note - the the grid letter plus grid digits is a synonymous representation for OSGB36
    // E.g Sullom Voe oil terminal in the Shetlands can be specified as HU396753 or 439668,1175316.
    // So we have two versions.

    [<StructuredFormatDisplay("{Easting}E {Northing}N")>]
    type OSGB36Point = 
        { Easting : float<meter>
          Northing : float<meter> }
        member v.ToWktPoint : WktPoint<OSGB36> = 
                    WktPoint (Some { WktLon = decimal v.Easting
                                   ; WktLat = decimal v.Northing } )
        static member FromWktPoint (pt:WktPoint<OSGB36>) = 
            match pt with
            | WktPoint None -> None
            | WktPoint (Some coord) -> Some ({ Easting = 1.0<meter> * float coord.WktLon
                                             ; Northing = 1.0<meter> * float coord.WktLat })

  

    let wktIsoOSGB36:WktCoordIso<OSGB36Point,OSGB36> = 
        { SRID = 27700
        ; Spheroid = "SPHEROID[\"Airy 1830\",6377563.396,299.3249646]"
        ; ToWktCoord = 
            fun point -> 
                { WktLon = decimal point.Easting
                ; WktLat = decimal point.Northing }
        ; FromWktCoord = 
            fun coord -> 
                { Easting = 1.0<meter> * float coord.WktLon
                ; Northing = 1.0<meter> * float coord.WktLat }
        }

    /// OS Grid refs, see
    /// https://en.wikipedia.org/wiki/Ordnance_Survey_National_Grid

    
    [<StructuredFormatDisplay("{MajorSquare}{MinorSquare} {MinorEasting} {MinorNorthing}")>]
    type private OSGB36GridRef = 
        { MajorSquare : char
          MinorSquare : char 
          MinorEasting : float<meter>
          MinorNorthing : float<meter> }

   
    let private decodeMajor (ch : char) : (float*float) = 
        match ch with
            | 'S' | 's' -> (0.0, 0.0)
            | 'T' | 't' -> (500000.0, 0.0)
            | 'N' | 'n' -> (0.0, 500000.0)
            | 'O' | 'o' -> (500000.0, 500000.0)
            | 'H' | 'h' -> (0.0, 1000000.0)
            | _         -> (-1000000.0, -1000000.0)

    let private decodeMinor (ch : char) : (float*float) =  
        let shifti x  = 
            match x with
                // There is no 'i' in the list of grid letters
                | _ when x > 8  -> x-1
                | _             -> x

        let fn =  shifti << (fun i -> i - 65) << int << Char.ToUpper
        let (n0, e1) = Math.DivRem(fn ch,5)
        let n1 = 4 - n0
        (float e1 * 100000.0, float n1 * 100000.0)

    let private decodeAlpha (s : char) (t : char) : (float*float) =  
        let (eM, nM) = decodeMajor s
        let (em, nm) = decodeMinor t
        (eM + em, nM + nm)

    // Expects even length string
    let private readContigNumberPair (ss : string) : int*int = 
        if (String.length ss) % 2 = 0 then
            let sz = String.length ss / 2
            let left = ss.[0..sz-1]
            let right = ss.[sz..(sz*2-1)]
            (int left,int right)
        else
            (0,0)


    let private findMajor (easting:float) (northing:float) : char =
        match (easting,northing) with
        | _ when easting >= 0.0 && easting < 500000.0 && northing >= 0.0 && northing < 500000.0 -> 'S'
        | _ when easting >= 500000.0 && easting < 1000000.0 && northing >= 0.0 && northing < 500000.0 -> 'T'
        | _ when easting >= 0.0 && easting < 500000.0 && northing >= 500000.0 && northing < 1000000.0 -> 'N'
        | _ when easting >= 500000.0 && easting < 1000000.0 && northing >= 500000.0 && northing < 1000000.0 -> 'O'
        | _ when easting >= 0.0 && easting < 500000.0 && northing >= 1000000.0 && northing < 1500000.0 -> 'H'
        | _ when easting >= 500000.0 && easting < 1000000.0 && northing >= 1000000.0 && northing < 1500000.0 -> 'J'
        | _ -> 'X'

    let private minorGrid : char[,] = 
        array2D [   [ 'V'; 'Q'; 'L'; 'F'; 'A' ];
                    [ 'W'; 'R'; 'M'; 'G'; 'B' ];
                    [ 'X'; 'S'; 'N'; 'H'; 'C' ];
                    [ 'Y'; 'T'; 'O'; 'J'; 'D' ];
                    [ 'Z'; 'U'; 'P'; 'K'; 'E' ]     ]

    let private findMinor (easting:float) (northing:float) : char =
        let modE = easting % 500000.0
        let modN = northing % 500000.0
        let divE = int (modE / 100000.0)
        let divN = int <| modN / 100000.0
        if divE >=0 && divE < 5 && divN >= 0 && divN < 5 then
            minorGrid.[divE,divN]
        else 'X'

    let private makeOSGB36GridRef (m:char) (mm:char) (east:int) (north:int) : OSGB36GridRef =  
        { MajorSquare = m
        ; MinorSquare = mm
        ; MinorEasting = 1.0<meter> * float east
        ; MinorNorthing = 1.0<meter> * float north }

        
    let private makeOSGB36Point (m : char) (mm : char) (east : float<meter>) (north : float<meter>) : OSGB36Point = 
        let (majE, majN) = decodeAlpha m mm 
        { Easting = east + LanguagePrimitives.FloatWithMeasure majE
        ; Northing = north + LanguagePrimitives.FloatWithMeasure majN }
    

    let private osgb36ToGridRef ({Easting = easting; Northing = northing} : OSGB36Point) : OSGB36GridRef =  
        let major = findMajor (float easting) (float northing)
        let minor = findMinor (float easting) (float northing)
        let smallE = easting % 100000.0<meter>
        let smallN = northing % 100000.0<meter>
        { MajorSquare = major; MinorSquare = minor; MinorEasting = smallE; MinorNorthing = smallN }
    
    let private gridRefToOSGB36(gridRef:OSGB36GridRef) : OSGB36Point =
        makeOSGB36Point gridRef.MajorSquare gridRef.MinorSquare gridRef.MinorEasting gridRef.MinorNorthing


    
    /// Print in the form 'SE9055679132' 
    let private showOSGB36GridReference (gridRef:OSGB36GridRef) : string = 
        sprintf "%c%c%05i%05i" 
                gridRef.MajorSquare 
                gridRef.MinorSquare 
                (int gridRef.MinorEasting) 
                (int gridRef.MinorNorthing)
    
    let showOSGB36Point (point:OSGB36Point) : string = 
        showOSGB36GridReference <| osgb36ToGridRef point

    let private (|OSGB36Regex|_|) (pattern:string) (input:string) : option<GroupCollection> =
        let m = Regex.Match(input.Trim(), pattern)
        if m.Success then Some m.Groups
        else None
    
    let private decodeOSGBNumber1 (s:string) : int =
        match s.Length with
        | 1 -> 10000 * System.Convert.ToInt32 s
        | 2 -> 1000 * System.Convert.ToInt32 s
        | 3 -> 100 * System.Convert.ToInt32 s
        | 4 -> 10 * System.Convert.ToInt32 s
        | 5 -> System.Convert.ToInt32 s
        | _ -> 0

    // precondition length = 6, 8 or 10
    let private decodeOSGBNumber2 (ss:string) : int*int = 
        let width = String.length ss
        let (a,b) = readContigNumberPair ss
        match width with 
        | 6 -> (a*100, b*100)
        | 8 -> (a*10, b*10)
        | 10 -> (a,b)
        | _ -> (0,0)

    /// Try to read an OSGB 36 grid reference.
    /// The format is [Char][Char][Number1][Number2], spacing between the numbers is optional.
    /// Numbers can either be 3,4, or 5 digits long.
    /// Note null string is handled (returns None).
    let tryReadOSGB36Point (input:string) : OSGB36Point option = 
        let getChar1 (groups:GroupCollection) = groups.[1].Value.[0]
        let getChar2 (groups:GroupCollection) = groups.[2].Value.[0]
        match input with
        | null -> None
        | OSGB36Regex @"^([A-Za-z])([A-Za-z])\s*([0-9]+)$" groups -> 
            let (e,n) = decodeOSGBNumber2 (groups.[3].Value)
            Some << gridRefToOSGB36 <| makeOSGB36GridRef (getChar1 groups) (getChar2 groups) e n
        | OSGB36Regex @"^([A-Za-z])([A-Za-z])\s*([0-9]+)\s+([0-9]+)$" groups -> 
            let e = decodeOSGBNumber1 <| groups.[3].Value
            let n = decodeOSGBNumber1 <| groups.[4].Value
            Some << gridRefToOSGB36 <| makeOSGB36GridRef (getChar1 groups) (getChar2 groups) e n
        | _ -> None

    let readOSGB36Point (input:string) : OSGB36Point = 
        match tryReadOSGB36Point input with
        | Some(x) -> x
        | None -> failwith <| sprintf "readOSGB36Grid - could not read '%s'" input



    
