// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SLGeo.Scripts.SiteOrder

open SLGeo.Extra.Grouping
open SLGeo.Base.WellKnownText
open SLGeo.Base.WGS84
open SLGeo.Shell.ScriptMonad
open SLGeo.Shell.PostGIS
open SLGeo.Scripts.TspRouting

// Note Scripts.Grouping now feels "not worked out".
// Potentially this module will replace it.

type LocationExtractor<'T,'point> = 'T -> Script<'point option>

let private pointsFromGrouping (getLocation:LocationExtractor<'T,'point>) (group:Grouping<'Key,'T>) : Script<'point list> = 
    fmapM (List.choose id) << mapM getLocation <| Seq.toList group.Elements

let private getCentroid1 (dict:WktCoordIso<'point,'srid>) (getLocation:LocationExtractor<'T,'point>) (group:Grouping<'Key,'T>) : Script<'point option> = 
    scriptMonad {
        let! (points:'point list) = pointsFromGrouping getLocation group
        let! opt = pgCentroid dict points
        return opt
        }

let getCentroids (dict:WktCoordIso<'point,'srid>) (getLocation:LocationExtractor<'T,'point>) (groups:seq<Grouping<'Key,'T>>) : Script<seq<'Key * 'point option>> = 
    let proc (group:Grouping<'Key,'T>) = 
        fmapM (fun pto -> (group.GroupingKey, pto)) <| getCentroid1 dict getLocation group
    traverseM proc groups

let private renumber (source : ('a *'obj) list) : (int * 'obj) list = 
    let rec work ac ix xs = 
        match xs with 
        | [] -> List.rev ac
        | (_,b) :: ys -> work ((ix,b) :: ac) (ix+1) ys
    work [] 1 source 

type SiteOrderDict<'Key,'elem> = 
    { GroupingOp: 'elem -> 'Key
      ExtractGridRef: 'elem -> Script<WGS84Point option>
      ExtractNodeLabel: 'elem -> string }


let siteOrder (dict:SiteOrderDict<string,'elem>) (unordered:seq<'elem>) : Script<(int * string) list> = 
    let centroidsTspDict:TspNodeInsertDict<string * WGS84Point option> = 
        { TryMakeNodeLocation = sreturn << snd; MakeNodeLabel = fst }

    let sitesTspDict:TspNodeInsertDict<'elem> =  
        { TryMakeNodeLocation = dict.ExtractGridRef
        ; MakeNodeLabel = dict.ExtractNodeLabel }

    scriptMonad { 
            
            let groups          = groupingBy dict.GroupingOp unordered
            let! groupCentroids = getCentroids wktIsoWGS84 dict.ExtractGridRef groups
            let! groupRoute     = tspRoute centroidsTspDict (Seq.toList groupCentroids)
            let keyList         = Seq.map snd groupRoute |> Seq.toList
            let orderedGroups   = 
                sortToKeyList (fun (x:Grouping<string,'elem>) -> x.GroupingKey) groups keyList
            let! sitesInGroups  = 
                forM (Seq.toList orderedGroups) 
                     (fun og -> tspRoute sitesTspDict (Seq.toList og.Elements))
            let finalOrder      = renumber <| List.concat sitesInGroups  
            return finalOrder
            }