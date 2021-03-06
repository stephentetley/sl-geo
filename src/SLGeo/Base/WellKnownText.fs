﻿// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

namespace SLGeo.Base


open FParsec



// Only concerned with 2d.
// 3d or 4d would be the subject for another module.
// Note BNF starts on page 54 of the spec.
// This should be the authority for naming etc.

[<AutoOpen>]
module WellKnownText = 
    
    // Note - Wkt should not favour the WGS84 reference system.
    // Other spatial references with "Lon" & "Lat" are possible.
    
    


    // ** SRIDs for Phantom types

    // Point modules should also define a SRID phantom type.


    /// The undefined SRID.
    type private NoSRID = class end


    // Probably need: 
    // type WellKnownText<'a> = WKT of string
    // Represent answers from PostGIS
    type WellKnownText<'a> = WellKnownText of string

    let inline unwrapWellKnownText (text:WellKnownText<'a>) : string = 
        match text with | WellKnownText ss -> ss

    /// The base point type does not have a phantom type wrapper.
    /// This means the phantom param is only wrapped once for LINESTRING etc.
    type WktCoord = 
        { WktLon: decimal      
          WktLat: decimal }




    /// Encode coordinate reference system as a phantom type.
    /// Values are represented as decimal
    /// A null point must be allowed, encode it internally with option.
    type WktPoint<'a> = WktPoint of WktCoord option

    let inline unwrapWktPoint (pt:WktPoint<'a>) : WktCoord option = 
        match pt with | WktPoint opt -> opt
    


    type WktLineString<'a> = WktLineString of WktCoord list 

    let inline unwrapWktLineString (source:WktLineString<'a>) : WktCoord list = 
        match source with | WktLineString xs -> xs

    type WktSurface = 
        { ExteriorBoundary: WktCoord list 
          InteriorBoundaries: (WktCoord list) list } 
    


    /// The user is responsible to ensure the polygon is closed before printing and
    /// that points are in counter-clockwise direction.        
    type WktPolygon<'a> =  WktPolygon of WktSurface 

    let inline unwrapWktPolygon (source:WktPolygon<'a>) : WktSurface = 
        match source with | WktPolygon s -> s


    type WktPolyhedralSurface<'a> = WktPolyhedralSurface of WktSurface list 

    let inline unwrapWktPolyhedralSurface (source:WktPolyhedralSurface<'a>) : WktSurface list = 
        match source with | WktPolyhedralSurface xs -> xs
    
    type WktTriangle<'a> = WktTriangle of WktCoord list 

    let inline unwrapWktTriangle (source:WktTriangle<'a>) : WktCoord list = 
        match source with | WktTriangle xs -> xs

    type WktTin<'a> = WktTin of (WktCoord list) list
    
    let inline unwrapWktTin (source:WktTin<'a>) : (WktCoord list) list= 
        match source with | WktTin xs -> xs


    /// According to the spec, we should allow Null Points in a MULTIPOINT
    /// string, however there are pragmatic reasons not to (1. efficiency, 2. we 
    /// can't interpret them anyway).
    type WktMultiPoint<'a> = WktMultiPoint of WktCoord list 

    let inline unwrapWktMultiPoint (source:WktMultiPoint<'a>) : WktCoord list = 
        match source with | WktMultiPoint xs -> xs


    type WktMultiLineString<'a> = WktMultiLineString of (WktCoord list) list

    let inline unwrapWktMultiLineString (source:WktMultiLineString<'a>) : (WktCoord list) list= 
        match source with | WktMultiLineString xs -> xs

    type WktMultiPolygon<'a> =  WktMultiPolygon of WktSurface list

    let inline unwrapWktMultiPolygon (source:WktMultiPolygon<'a>) : WktSurface list= 
        match source with | WktMultiPolygon xs -> xs

    type WktGeometry<'a> =
        | Point of WktPoint<'a>
        | LineString of WktLineString<'a>
        | Polygon of WktPolygon<'a>
        | PolyhedralSurface of WktPolyhedralSurface<'a>
        | Triangle of WktTriangle<'a>
        | Tin of WktTin<'a>
        | MultiPoint of WktMultiPoint<'a>
        | MultiLineString of WktMultiLineString<'a>
        | MultiPolygon of WktMultiPolygon<'a>
            


    // ***** construction / Conversion *****

    /// Does dictionary passing let us usefully generalize things?
    /// Initial results seem very positive!
    /// Should we store Spheroid as string (flexible / error prone...) or a proper data type?
    type WktCoordIso<'T,'srid> =  
        { SRID: int 
          Spheroid: string
          ToWktCoord : 'T -> WktCoord
          FromWktCoord: WktCoord -> 'T }



    let inline makeWktCoordList (dict:WktCoordIso<'point,'srid>) (points:seq<'point>) : WktCoord list =
        Seq.map dict.ToWktCoord points |> Seq.toList

    let inline wktExtractCoordList (dict:WktCoordIso<'point,'srid>) (coords:WktCoord list) : 'point list =
        List.map dict.FromWktCoord coords 

    let inline wktExtractCoordSeq (dict:WktCoordIso<'point,'srid>) (coords:WktCoord list) : seq<'point> =
        List.map dict.FromWktCoord coords |> List.toSeq

    let makeWktPoint (dict:WktCoordIso<'point,'srid>) (point:'point) : WktPoint<'srid> =
        WktPoint << Some <| dict.ToWktCoord point

    let wktExtractPoint (dict:WktCoordIso<'point,'srid>) (point:WktPoint<'srid>) : 'point option =
        Option.map dict.FromWktCoord <| unwrapWktPoint point 

    let makeWktLineString (dict:WktCoordIso<'point,'srid>) (points:seq<'point>) : WktLineString<'srid> =
        WktLineString <| makeWktCoordList dict points

    let wtkExtractLineString (dict:WktCoordIso<'point,'srid>) (lineString:WktLineString<'srid>) : seq<'point> = 
        Seq.map dict.FromWktCoord <| unwrapWktLineString lineString 

    let makeWktSurface (dict:WktCoordIso<'point,'srid>) (exterior:seq<'point>) (interiors:seq<seq<'point>>) : WktSurface = 
        { ExteriorBoundary = makeWktCoordList dict exterior
          InteriorBoundaries = Seq.map (makeWktCoordList dict) interiors |> Seq.toList } 


    let makeWktPolygon (dict:WktCoordIso<'point,'srid>) (exterior:seq<'point>) (interiors:seq<seq<'point>>) : WktPolygon<'srid> =
        WktPolygon <| makeWktSurface dict exterior interiors

    let makeWktMultiPoint (dict:WktCoordIso<'point,'srid>) (points:seq<'point>) : WktMultiPoint<'srid> = 
        WktMultiPoint <| makeWktCoordList dict points

    let wtkExtractMultiPoint (dict:WktCoordIso<'point,'srid>) (multiPoint:WktMultiPoint<'srid>) : seq<'point> = 
        Seq.map dict.FromWktCoord <| unwrapWktMultiPoint multiPoint 




    // Previously we have had SRID changing functions, e.g. 
    //
    // val wktPointOSGB36ToWGS84 : point:WktPoint<OSGB36> -> WktPoint<WGS84>
    // val wktPointWGS84ToOSGB36 : point:WktPoint<WGS84> -> WktPoint<OSGB36>
    //
    // However, maybe they are not so wise, if we need to change SRIDs we should be working
    // with more concrete objects / datatypes. The Wtk datatypes are meant for data transfer.



    // ***** PRINTING *****
    
    /// Prints in parens
    let inline private showWktCoordText (coord:WktCoord) : string = 
        sprintf "(%.5f %.5f)" coord.WktLon coord.WktLat

    let inline showWktCoord (coord:WktCoord) : string = 
        sprintf "%.5f %.5f" coord.WktLon coord.WktLat

    
    let inline private showPointText (point:WktPoint<'a>) : string = 
        match unwrapWktPoint point with
        | None -> "EMPTY"
        | Some coord -> sprintf "(%s)" (showWktCoord coord)

    /// Prints as 'POINT(14.12345, 15.12345)'
    /// (Ideally we would print with user supplied precision)
    let inline showWktPoint (point:WktPoint<'a>) : string = 
        sprintf "POINT %s" <| showPointText point


    let private showLineStringText (source:WktCoord list) = 
        match source with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showWktCoord xs)

    /// Prints as 'LINESTRING(-1.08066 53.93863,-1.43627 53.96907)'
    let showWktLineString (source:WktLineString<'a>) : string =
        sprintf "LINESTRING %s" << showLineStringText <| unwrapWktLineString source
        

    let private showPolygonText (source:WktSurface) = 
        match (source.ExteriorBoundary :: source.InteriorBoundaries) with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showLineStringText xs)


    let showWktPolygon (source:WktPolygon<'a>) : string =
        sprintf "POLYGON %s" << showPolygonText <| unwrapWktPolygon source


    let private showPolyhedralSurfaceText (source:WktSurface list) : string =
        match source with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showPolygonText xs)


    let showWktPolyhedralSurface (source:WktPolyhedralSurface<'a>) : string =
        sprintf "POLYHEDRALSURACE %s" << showPolyhedralSurfaceText <| unwrapWktPolyhedralSurface source

    
    let showWktTriangle (source:WktTriangle<'a>) : string =
        sprintf "TRIANGLE %s" 
            << showPolygonText <| { ExteriorBoundary = unwrapWktTriangle source; InteriorBoundaries = [] }
                    
    let showWktTin (source:WktTin<'a>) : string = 
        let toSurface1 = fun xs ->  { ExteriorBoundary = xs; InteriorBoundaries = [] }
        sprintf "TIN %s" << showPolyhedralSurfaceText << List.map toSurface1 <| unwrapWktTin source
            
        
    let private showMultiPointText (source:WktMultiPoint<'a>) : string =
        match unwrapWktMultiPoint source with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showWktCoordText xs)

    /// Prints as 'MULTIPOINT((-1.08066 53.93863), (-1.43627 53.96907))'
    let showWktMultiPoint (source:WktMultiPoint<'a>) : string =
        sprintf "MULTIPOINT %s" <| showMultiPointText source

    /// Prints as 'MULTIPOINT(-1.08066 53.93863,-1.43627 53.96907)'
    let showWktMultiPointLax (source:WktMultiPoint<'a>) : string =
        match unwrapWktMultiPoint source with
        | [] -> "MULTIPOINT EMPTY"
        | xs -> sprintf "MULTIPOINT(%s)" (String.concat "," <| List.map showWktCoord xs)


    let private showMultiLineStringText (source:(WktCoord list) list) : string =
        match source with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showLineStringText xs)


    let showWktMultiLineString (source:WktMultiLineString<'a>) : string = 
        sprintf "MULTILINESTRING %s" << showMultiLineStringText <| unwrapWktMultiLineString source

    let private showMultiPolygonText (source:WktSurface list) : string =
        match source with
        | [] -> "EMPTY"
        | xs -> sprintf "(%s)" (String.concat "," <| List.map showPolygonText xs)

    let showWktMultiPolygon (source:WktMultiPolygon<'a>) : string = 
        sprintf "MULTIPOLYGON %s" << showMultiPolygonText <| unwrapWktMultiPolygon source

    let showWktGeometry(geom:WktGeometry<'a>) : string =
        match geom with
        | Point x -> showWktPoint x
        | LineString x -> showWktLineString x
        | Polygon x -> showWktPolygon x
        | PolyhedralSurface x -> showWktPolyhedralSurface x
        | Triangle x -> showWktTriangle x
        | Tin x -> showWktTin x
        | MultiPoint x -> showWktMultiPoint x
        | MultiLineString x -> showWktMultiLineString x
        | MultiPolygon x -> showWktMultiPolygon x


    // ***** PARSING *****

    let private pSymbol (s:string) : Parser<string,'u> = 
        pstring s .>> spaces

    let private pParens (p:Parser<'a,'u>) : Parser<'a,'u> =
        between (pSymbol "(") (pSymbol ")") p

    // We have got over the value restriction by fixing the parser state to unit.


    let private pDecimal : Parser<decimal,unit> = pfloat |>> decimal

    type private OneAndMany<'a> = 'a * 'a list

    

    /// This is the pattern for Polygon
    let private sepByOneMany (p:Parser<'a,unit>) (sepa:Parser<'z,unit>) : Parser<OneAndMany<'a>,unit> = 
        parse { let! ans1 = p
                let! ansMany = sepBy p sepa
                return (ans1,ansMany) }

    // This is not within parens

    type PointText1 = WktCoord
     
    let private pPointText1 : Parser<WktCoord, unit> = 
        pipe2   (pDecimal .>> spaces)
                (pDecimal .>> spaces) 
                (fun lon lat -> { WktLon = lon; WktLat = lat })

    let private pComma : Parser<unit,unit> = 
        pSymbol "," |>> ignore

    let private pEMPTY (emptyDefault:'a) : Parser<'a, unit> = 
        pSymbol "EMPTY" |>> (fun _ -> emptyDefault)

    let private pTagged (tag:string) (p:Parser<'a,unit>) :Parser<'a,unit> = 
        pSymbol tag >>. p

    type private PointText = WktCoord option

    let private pPointText : Parser<PointText,unit> = 
        (pEMPTY None) <|> (pParens pPointText1 |>> Some)



    type private LineStringText = WktCoord list

    let private pLineStringText : Parser<LineStringText, unit> = 
        pEMPTY [] <|> pParens (sepBy1 pPointText1 (pSymbol ","))
        

    let private pParenLineStringText1 : Parser<LineStringText, unit> = 
        sepBy1 (pParens pPointText1) (pSymbol ",")

    type private PolygonText = LineStringText list

    let private buildSurface (source:PolygonText) : WktSurface = 
        match source with
        | [] -> { ExteriorBoundary = []; InteriorBoundaries = [] }
        | x :: xs -> { ExteriorBoundary = x; InteriorBoundaries = xs }

    let private pPolygonText : Parser<PolygonText,unit> =
        pEMPTY [] <|> pParens (sepBy1 pLineStringText (pSymbol ","))

    
    let private pWktSurface : Parser<WktSurface,unit> =
        let build1 xs xss = { ExteriorBoundary = xs; InteriorBoundaries = xss}
        let buildXs xss = 
            match xss with 
            | [] -> { ExteriorBoundary = []; InteriorBoundaries = []}
            | y :: ys -> { ExteriorBoundary = y; InteriorBoundaries = ys}
        pEMPTY (build1 [] []) <|> pParens (sepBy1 pLineStringText pComma |>> buildXs)

    
    let pTriangleText : Parser<WktCoord list,unit> =
        pEMPTY [] <|> pParens pLineStringText

    let private pPolyhedralSurfaceText: Parser<WktSurface list,unit> =
        pEMPTY [] <|> pParens (sepBy1 pWktSurface (pSymbol ","))


    // Divergence from the EBNF - Tin is disallowed interior rings, so cannot use 
    // pPolyhedralSurfaceText
    let private pTinText: Parser<(WktCoord list) list,unit> =
        pParens (sepBy1 pLineStringText pComma)

    /// Note - we diverge from the WKT Spec. Null Points are not allowed.
    type private MultiPointText = PointText1 list
    

    /// Note - we diverge from the WKT Spec. Null Points are not recognized.
    /// We also allow old style Point Lists where individual points are not within parens.
    let private pMultiPointText : Parser<MultiPointText,unit> = 
        let newStyle = sepBy1 (pParens pPointText1) pComma
        let oldStyle = sepBy1 pPointText1 pComma
        pEMPTY [] <|> pParens (newStyle <|> oldStyle)

    let private pMultiLineStringText : Parser<LineStringText list,unit> = 
        pEMPTY [] <|> pParens (sepBy1 pLineStringText pComma)
        

    let private pMultiPolygonText: Parser<WktSurface list,unit> =
        pEMPTY [] <|> pParens (sepBy1 pWktSurface (pSymbol ","))
        
    // *** utlity
                        
    let tryReadParse (p1:Parser<'a,unit>) (source:string) : 'a option = 
        let ans1 = runParserOnString p1 () "none" source
        match ans1 with
        | Success(a,_,_) -> Some <| a
        | Failure(s,_,_) -> None


    // ***** Parsing Public API

    let tryReadWktPoint (source:string) : WktPoint<'srid> option = 
        let parsePOINT = pTagged "POINT" pPointText |>> WktPoint
        tryReadParse parsePOINT source

    let tryReadWktLineString (source:string) : WktLineString<'srid> option = 
        let parseLINESTRING = pTagged "LINESTRING" pLineStringText |>> WktLineString
        tryReadParse parseLINESTRING source


    // Surface
    let private buildPolygon (source:PolygonText) : WktPolygon<'srid> = 
        WktPolygon <| buildSurface source

    // WARNING - to test
    let tryReadWktPolygon (source:string) : WktPolygon<'srid> option = 
        let parsePOLYGON = pTagged "POLYGON" pPolygonText |>> buildPolygon
        tryReadParse parsePOLYGON source

    let tryReadWktTriangle (source:string) : WktTriangle<'srid> option = 
        let parseTRIANGLE = pTagged "TRIANGLE" pTriangleText |>> WktTriangle
        tryReadParse parseTRIANGLE source

    let tryReadWktPolyhedralSurface (source:string) : WktPolyhedralSurface<'srid> option = 
        let parsePOLYHEDRAL = 
            pTagged "POLYHEDRALSURFACE" pPolyhedralSurfaceText |>> WktPolyhedralSurface
        tryReadParse parsePOLYHEDRAL source


    let tryReadWktTin (source:string) : WktTin<'srid> option = 
        let parseTIN = pTagged "TIN" pTinText |>> WktTin
        tryReadParse parseTIN source


    /// Note - the proper version is MULTIPOINT((1 2), (3 4))            
    /// This hould handle both cases - to check...
    let tryReadWktMultiPoint (source:string) : WktMultiPoint<'srid> option = 
        let parseMULTIPOINT = pTagged "MULTIPOINT" pMultiPointText |>> WktMultiPoint
        tryReadParse parseMULTIPOINT source


    let tryReadWktMultiLineString (source:string) : WktMultiLineString<'srid> option = 
        let parseMULTILINESTRING = pTagged "MULTILINESTRING" pMultiLineStringText |>> WktMultiLineString
        tryReadParse parseMULTILINESTRING source


    let tryReadWktMultiPolygon (source:string) : WktMultiPolygon<'srid> option = 
        let parseMULTILINESTRING = pTagged "MULTIPOLYGON" pMultiPolygonText |>> WktMultiPolygon
        tryReadParse parseMULTILINESTRING source


 