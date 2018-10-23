// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SL.PostGIS.ScriptMonad

open System.IO

open SL.Base.ErrorTrace
open SL.Base


type Result<'a> = 
    | Ok of 'a
    | Error of ErrorTrace
    | Fatal of string


type LogAction = StringWriter -> unit
type ConnParams = PGSQLConn.PGSQLConnParams

/// This is the default logger, writes the output to the console with printfn.
let consoleLogger : LogAction = fun sw -> printfn "--- Log: ----------\n%s" (sw.ToString())

type Script<'a> = private ScriptMonad of (ConnParams -> StringWriter -> Result<'a>)


let inline private apply1 (ma : Script<'a>) 
                            (connp:ConnParams) (sw:StringWriter)  : Result<'a> = 
    let (ScriptMonad fn) = ma  in  fn connp sw

let inline sreturn (x:'a) : Script<'a> = 
    ScriptMonad <| fun sw r -> Ok x



let inline private bindM (ma:Script<'a>) (fn : 'a -> Script<'b>) : Script<'b> =
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> apply1 (fn a) connp sw 
        | Error stk -> Error stk
        | Fatal msg -> Fatal msg


let szero () : Script<'a> = 
    ScriptMonad <| fun _ _ -> Error (errorTrace1 "szero")

/// For ScriptMonad combineM is Haskell's (>>)
let inline private combineM (ma:Script<unit>) (mb:Script<'b>) : Script<'b> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok _ -> apply1 mb connp sw
        | Error stk -> Error stk
        | Fatal msg -> Fatal msg

let inline private delayM (fn:unit -> Script<'a>) : Script<'a> = 
    bindM (sreturn ()) fn 




type ScriptBuilder() = 
    member self.Return x            = sreturn x
    member self.Bind (p,f)          = bindM p f
    member self.Zero ()             = szero ()
    member self.Combine (ma,mb)     = combineM ma mb
    member self.Delay fn            = delayM fn


let (scriptMonad:ScriptBuilder) = new ScriptBuilder()

let (>>=) (ma:Script<'a>) (fn : 'a -> Script<'b>) : Script<'b> =
    bindM ma fn

// Common monadic operations
let fmapM (fn:'a -> 'b) (ma:Script<'a>) : Script<'b> = 
    ScriptMonad <| fun connp sw -> 
       match apply1 ma connp sw with
       | Ok ans -> Ok <| fn ans
       | Error stk -> Error stk
       | Fatal msg -> Fatal msg

let (|>>) (ma:Script<'a>) (fn:'a -> 'b)  : Script<'b> = fmapM fn ma
let (<<|) (fn:'a -> 'b)  (ma:Script<'a>)  : Script<'b> = fmapM fn ma


let liftM (fn:'a -> 'x) (ma:Script<'a>) : Script<'x> = 
    fmapM fn ma

let liftM2 (fn:'a -> 'b -> 'x) (ma:Script<'a>) (mb:Script<'b>) : Script<'x> = 
    scriptMonad { 
        let! a = ma
        let! b = mb
        return (fn a b)
    }

let liftM3 (fn:'a -> 'b -> 'c -> 'x) (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) : Script<'x> = 
    scriptMonad { 
        let! a = ma
        let! b = mb
        let! c = mc
        return (fn a b c)
    }

let liftM4 (fn:'a -> 'b -> 'c -> 'd -> 'x) (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) (md:Script<'d>) : Script<'x> = 
    scriptMonad { 
        let! a = ma
        let! b = mb
        let! c = mc
        let! d = md
        return (fn a b c d)
    }

let liftM5 (fn:'a -> 'b -> 'c -> 'd -> 'e -> 'x) (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) (md:Script<'d>) (me:Script<'e>) : Script<'x>= 
    scriptMonad { 
        let! a = ma
        let! b = mb
        let! c = mc
        let! d = md
        let! e = me
        return (fn a b c d e)
    }

let tupleM2 (ma:Script<'a>) (mb:Script<'b>) : Script<'a * 'b> = 
    liftM2 (fun a b -> (a,b)) ma mb

let tupleM3 (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) : Script<'a * 'b * 'c> = 
    liftM3 (fun a b c -> (a,b,c)) ma mb mc

let tupleM4 (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) (md:Script<'d>) : Script<'a * 'b * 'c * 'd> = 
    liftM4 (fun a b c d -> (a,b,c,d)) ma mb mc md

let tupleM5 (ma:Script<'a>) (mb:Script<'b>) (mc:Script<'c>) (md:Script<'d>) (me:Script<'e>)  : Script<'a * 'b * 'c * 'd * 'e> = 
    liftM5 (fun a b c d e -> (a,b,c,d,e)) ma mb mc md me

// NOTE - FParsec defines flipped versions of liftM* (e.g. pipe2, pipe3, ...)

let mapM (fn:'a -> Script<'b>) (source:'a list) : Script<'b list> = 
    ScriptMonad <| fun connp sw -> 
        let rec work ac xs = 
            match xs with
            | [] -> Ok (List.rev ac)
            | z :: zs -> 
                match apply1 (fn z) connp sw with
                | Ok ans -> work (ans::ac) zs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
        work [] source 


let forM (xs:'a list) (fn:'a -> Script<'b>) : Script<'b list> = 
    mapM fn xs

let mapMz (fn:'a -> Script<'b>) (source:'a list) : Script<unit> = 
    ScriptMonad <| fun connp sw -> 
        let rec work  xs = 
            match xs with
            | [] -> Ok ()
            | z :: zs -> 
                match apply1 (fn z) connp sw with
                | Ok ans -> work zs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
        work source 

let forMz (xs:'a list) (fn:'a -> Script<'b>) : Script<unit> = 
    mapMz fn xs


let mapiM (fn:int -> 'a -> Script<'b>) (source:'a list) : Script<'b list> = 
    ScriptMonad <| fun connp sw -> 
        let rec work ix ac xs = 
            match xs with
            | [] -> Ok (List.rev ac)
            | z :: zs -> 
                match apply1 (fn ix z) connp sw with
                | Ok ans -> work (ix+1) (ans::ac) zs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
        work 0 [] source 

let mapiMz (fn:int -> 'a -> Script<'b>) (source:'a list) : Script<unit> = 
    ScriptMonad <| fun connp sw -> 
        let rec work ix xs = 
            match xs with
            | [] -> Ok ()
            | z :: zs -> 
                match apply1 (fn ix z) connp sw with
                | Ok ans -> work (ix+1) zs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
        work 0 source 

let foriM (xs:'a list) (fn:int -> 'a -> Script<'b>) : Script<'b list> =
    mapiM fn xs

let foriMz (xs:'a list) (fn:int -> 'a -> Script<'b>) : Script<unit> =
    mapiMz fn xs

/// Note - Seq going through list seems better than anything I can manage directly
/// either with recursion (bursts the stack) or an enumerator (very slow)
/// The moral is `traverse` is a bad API (currently)
let traverseM (fn: 'a -> Script<'b>) (source:seq<'a>) :  Script<seq<'b>> = 
    fmapM (List.toSeq) (mapM fn <| Seq.toList source) 

let traverseMz (fn: 'a -> Script<'b>) (source:seq<'a>) :  Script<unit> = 
    mapMz fn <| Seq.toList source

let traverseiM (fn:int -> 'a -> Script<'b>) (source:seq<'a>) :  Script<seq<'b>> = 
    fmapM (List.toSeq) (mapiM fn <| Seq.toList source) 

let traverseiMz (fn:int -> 'a -> Script<'b>) (source:seq<'a>) :  Script<unit> = 
    mapiMz fn <| Seq.toList source


let sequenceM (source:Script<'a> list) : Script<'a list> = 
    ScriptMonad <| fun connp sw -> 
        let rec work ac xs = 
            match xs with
            | mf :: fs -> 
                match apply1 mf connp sw with
                | Ok a -> work (a::ac) fs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
            | [] -> Ok (List.rev ac)
        work [] source

let sequenceMz (source:Script<'a> list) : Script<unit> = 
    ScriptMonad <| fun connp sw -> 
        let rec work xs = 
            match xs with
            | mf :: fs -> 
                match apply1 mf connp sw with
                | Ok a -> work fs
                | Error stk -> Error stk
                | Fatal msg -> Fatal msg
            | [] -> Ok ()
        work source

// Summing variants

let sumMapM (fn:'a -> Script<int>) (xs:'a list) : Script<int> = 
    fmapM List.sum <| mapM fn xs

let sumMapiM (fn:int -> 'a -> Script<int>) (xs:'a list) : Script<int> = 
    fmapM List.sum <| mapiM fn xs

let sumForM (xs:'a list) (fn:'a -> Script<int>) : Script<int> = 
    fmapM List.sum <| forM xs fn

let sumForiM (xs:'a list) (fn:int -> 'a -> Script<int>) : Script<int> = 
    fmapM List.sum <| foriM xs fn

let sumTraverseM (fn: 'a -> Script<int>) (source:seq<'a>) : Script<int> =
    fmapM Seq.sum <| traverseM fn source

let sumTraverseiM (fn:int -> 'a -> Script<int>) (source:seq<'a>) : Script<int> =
    fmapM Seq.sum <| traverseiM fn source

let sumSequenceM (source:Script<int> list) : Script<int> = 
    fmapM List.sum <| sequenceM source


// Applicatives (<*>)
let apM (mf:Script<'a ->'b>) (ma:Script<'a>) : Script<'b> = 
    scriptMonad { 
        let! f = mf
        let! a = ma
        return (f a)
    }

// Perform two actions in sequence. Ignore the results of the second action if both succeed.
let seqL (ma:Script<'a>) (mb:Script<'b>) : Script<'a> = 
    scriptMonad { 
        let! a = ma
        let! _ = mb
        return a
    }

// Perform two actions in sequence. Ignore the results of the first action if both succeed.
let seqR (ma:Script<'a>) (mb:Script<'b>) : Script<'b> = 
    scriptMonad { 
        let! _ = ma
        let! b = mb
        return b
    }

// Answer sepcific operations
let runScript (failure: string -> 'b) (success: 'a -> 'b) 
                (logger:LogAction) (connp:ConnParams) (ma:Script<'a>) : 'b = 
    use sw = new System.IO.StringWriter()
    let ans = apply1 ma connp sw
    let () = logger sw
    match ans with
    | Ok a -> success a
    | Fatal msg -> failwith msg
    | Error stk -> failwith (getErrorTrace stk)



let runConsoleScript (success:'a -> unit) (connp:ConnParams) (ma:Script<'a>) : unit = 
    runScript failwith id (consoleLogger) connp ma |> success
    

let throwError (msg:string) : Script<'a> =     
    ScriptMonad <| fun _ _ -> Error (errorTrace1 msg)

let fatalError (msg:string) : Script<'a> =     
    ScriptMonad <| fun _ _ -> Fatal msg


let swapError (msg:string) (ma:Script<'a>) : Script<'a> = 
    ScriptMonad <| fun connp sw ->
        match apply1 ma connp sw with
        | Ok a -> Ok a 
        | Fatal msg -> Fatal msg
        | Error stk -> Error (renameTraceTop msg stk)

let augmentError (msg:string) (ma:Script<'a>) : Script<'a> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> Ok a 
        | Fatal msg -> Fatal msg
        | Error stk -> Error (augmentErrorTrace msg stk)



let replaceFailure (defaultValue:'a) (ma:Script<'a>) : Script<'a> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> Ok a 
        | Error stk -> Ok defaultValue
        | Fatal msg -> Fatal msg
        

let logWriteLine (text:string) : Script<unit> = 
    ScriptMonad <| fun connp sw -> 
        let () = sw.WriteLine text
        Ok ()

let logScript (makeLine:'a -> string) (proc:Script<'a>) : Script<'a> = 
    scriptMonad { 
        let! a = proc
        do! logWriteLine (makeLine a)
        return a
        }



let liftOption (source:'a option) : Script<'a> = 
    match source with
    | None -> throwError "liftOption"
    | Some a -> sreturn a
        


// Left biased choice, if ``ma`` succeeds return its result, otherwise try ``mb``.
let alt (ma:Script<'a>) (mb:Script<'a>) : Script<'a> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> Ok a 
        | Fatal msg -> Fatal msg
        | Error stk -> apply1 mb connp sw


/// Catch failing computations, return None. 
/// Successful operations are returned as Some(_).
/// Fatal errors are passed forward rather than caught.
let optional (ma:Script<'a>) : Script<'a option> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> Ok (Some a)
        | Fatal msg -> Fatal msg
        | Error stk -> Ok None

// Perform an operation for its effect, ignore whether it succeeds or fails.
// (Comptations always return ``Ok ()``)
let optionalz (ma:Script<'a>) : Script<unit> = 
    ScriptMonad <| fun connp sw -> 
        match apply1 ma connp sw with
        | Ok a -> Ok ()
        | Error stk -> Ok ()
        | Fatal msg -> Fatal msg
        

let runOptional (failMsg:string) (ma:Script<'a option>) : Script<'a> = 
    scriptMonad { 
        let! opt1 = ma
        match opt1 with
        | None -> 
            let! err = throwError failMsg
            return err
        | Some a -> return a        
        }    



let private liftResult (result:PGSQLConn.Result<'a>) : Result<'a> = 
    match result with
    | PGSQLConn.Success a -> Ok a
    | PGSQLConn.Failure stk -> Fatal (ErrorTrace.getErrorTrace stk)

let liftAtomically (pgsql:PGSQLConn.PGSQLConn<'a>) : Script<'a> = 
    ScriptMonad <| fun connp sw -> 
        liftResult <| PGSQLConn.atomically connp pgsql

