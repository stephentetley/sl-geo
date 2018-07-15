// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SL.Base.PGSQLConn


open Npgsql

open SL.Base.ErrorTrace



/// Design Note 
/// To manage transactions / rollback we should think of running
/// the conn monad frequently (c.f Haskell STM monads `automically`), 
/// rather than design all the code within a single invocation of
/// the monad.
///
/// This is facilitate with the ``atomically`` function.
///
/// Potentially a phantom type could be used to inidicate commands 
/// that must be run atomically.





type Result<'a> = 
    | Success of 'a
    | Failure of ErrorTrace




type PGSQLConnParams = 
    { Host : string 
      Username : string 
      Password : string 
      Database : string }


let paramsConnString (config:PGSQLConnParams) : string = 
    sprintf "Host=%s;Username=%s;Password=%s;Database=%s" config.Host config.Username config.Password config.Database

/// Host="localhost"; Username="postgres"
let pgsqlConnParamsTesting (dbName:string) (password:string) : PGSQLConnParams = 
    { Host = "localhost"; Username = "postgres"; Database = dbName; Password = password }



// SQLiteConn Monad
type PGSQLConn<'a> = PGSQLConn of (NpgsqlConnection -> Result<'a>)

let inline private apply1 (ma : PGSQLConn<'a>) (conn:NpgsqlConnection) : Result<'a> = 
    let (PGSQLConn f) = ma in f conn


let inline pgreturn (x:'a) : PGSQLConn<'a> = 
    PGSQLConn (fun _ -> Success x)


let inline private bindM (ma:PGSQLConn<'a>) (f : 'a -> PGSQLConn<'b>) : PGSQLConn<'b> =
    PGSQLConn <| fun conn -> 
        match apply1 ma conn with
        | Success a -> apply1 (f a) conn
        | Failure msg -> Failure msg


let inline pgzero () : PGSQLConn<'a> = 
    PGSQLConn <| fun _ -> Failure (errorTrace1 "pgzero")



/// For PGSQLConn combineM is Haskell's (>>)
let inline private combineM (ma:PGSQLConn<unit>) (mb:PGSQLConn<'b>) : PGSQLConn<'b> = 
    PGSQLConn <| fun conn -> 
        match apply1 ma conn with
        | Failure stk -> Failure stk
        | Success _ -> apply1 mb conn

let inline private  delayM (fn:unit -> PGSQLConn<'a>) : PGSQLConn<'a> = 
    bindM (pgreturn ()) fn 


type PGSQLConnBuilder() = 
    member self.Return x            = pgreturn x
    member self.Bind (p,f)          = bindM p f
    member self.Zero ()             = pgzero ()
    member self.Combine (ma,mb)     = combineM ma mb
    member self.Delay fn            = delayM fn

let (pgsqlConn:PGSQLConnBuilder) = new PGSQLConnBuilder()


let (>>=) (ma:PGSQLConn<'a>) (fn : 'a -> PGSQLConn<'b>) : PGSQLConn<'b> = 
    bindM ma fn

// Common operations
let fmapM (fn:'a -> 'b) (ma:PGSQLConn<'a>) : PGSQLConn<'b> = 
    PGSQLConn <| fun conn ->
       match apply1 ma conn with
       | Success ans -> Success <| fn ans
       | Failure msg -> Failure msg


let (|>>) (fn:'a -> 'b) (ma:PGSQLConn<'a>) : PGSQLConn<'b> = 
    fmapM fn ma

let (<<|) (ma:PGSQLConn<'a>) (fn:'a -> 'b) : PGSQLConn<'b> = 
    fmapM fn ma

let mapM (fn:'a -> PGSQLConn<'b>) (source:'a list) : PGSQLConn<'b list> = 
    PGSQLConn <| fun conn ->
        let rec work ac xs = 
            match xs with
            | [] -> Success (List.rev ac)
            | z :: zs -> 
                match apply1 (fn z) conn with
                | Failure stk -> Failure stk
                | Success ans -> work (ans::ac) zs
        work [] source 

let forM (xs:'a list) (fn:'a -> PGSQLConn<'b>) : PGSQLConn<'b list> = 
    mapM fn xs

let mapMz (fn:'a -> PGSQLConn<'b>) (source:'a list) : PGSQLConn<unit> = 
    PGSQLConn <| fun conn ->
        let rec work xs = 
            match xs with
            | [] -> Success ()
            | z :: zs -> 
                match apply1 (fn z) conn with
                | Failure stk -> Failure stk
                | Success _ -> work zs
        work source 

let forMz (xs:'a list) (fn:'a -> PGSQLConn<'b>) : PGSQLConn<unit> = mapMz fn xs

let mapiM (fn:int -> 'a -> PGSQLConn<'b>) (source:'a list) : PGSQLConn<'b list> = 
    PGSQLConn <| fun conn ->
        let rec work ix ac xs = 
            match xs with
            | [] -> Success (List.rev ac)
            | z :: zs -> 
                match apply1 (fn ix z) conn with
                | Failure stk -> Failure stk
                | Success ans -> work (ix+1) (ans::ac) zs
        work 0 [] source 

let mapiMz (fn:int -> 'a -> PGSQLConn<'b>) (source:'a list) : PGSQLConn<unit> = 
    PGSQLConn <| fun conn ->
        let rec work ix  xs = 
            match xs with
            | [] -> Success ()
            | z :: zs -> 
                match apply1 (fn ix z) conn with
                | Failure stk -> Failure stk
                | Success ans -> work (ix+1) zs
        work 0 source 

let foriM (xs:'a list) (fn:int -> 'a -> PGSQLConn<'b>) : PGSQLConn<'b list> = 
    mapiM fn xs



/// Note - Seq going through list seems better than anything I can manage directly
/// either with recursion (bursts the stack) or an enumerator (very slow)
/// The moral is `traverse` is a bad API (currently)
let traverseM (fn: 'a -> PGSQLConn<'b>) (source:seq<'a>) : PGSQLConn<seq<'b>> = 
    fmapM (List.toSeq) (mapM fn <| Seq.toList source) 

let traverseMz (fn: 'a -> PGSQLConn<'b>) (source:seq<'a>) : PGSQLConn<unit> = 
    mapMz fn <| Seq.toList source

let traverseiM (fn:int -> 'a -> PGSQLConn<'b>) (source:seq<'a>) : PGSQLConn<seq<'b>> = 
    fmapM (List.toSeq) (mapiM fn <| Seq.toList source) 

let traverseiMz (fn:int -> 'a -> PGSQLConn<'b>) (source:seq<'a>) : PGSQLConn<unit> = 
    mapiMz fn <| Seq.toList source


/// Shortcuts on first error...
let sequenceM (source:PGSQLConn<'a> list) : PGSQLConn<'a list> = 
    PGSQLConn <| fun conn ->
        let rec work ac xs = 
            match xs with
            | mf :: fs -> 
                match apply1 mf conn with
                | Failure msg -> Failure msg
                | Success a -> work  (a::ac) fs
            | [] -> Success (List.rev ac)
        work [] source

let sequenceMz (source:PGSQLConn<'a> list) : PGSQLConn<unit> = 
    PGSQLConn <| fun conn ->
        let rec work xs = 
            match xs with
            | mf :: fs -> 
                match apply1 mf conn with
                | Failure msg -> Failure msg
                | Success a -> work fs
            | [] -> Success ()
        work source


// Summing variants

let sumMapM (fn:'a -> PGSQLConn<int>) (xs:'a list) : PGSQLConn<int> = 
    fmapM List.sum <| mapM fn xs

let sumMapiM (fn:int -> 'a -> PGSQLConn<int>) (xs:'a list) : PGSQLConn<int> = 
    fmapM List.sum <| mapiM fn xs

let sumForM (xs:'a list) (fn:'a -> PGSQLConn<int>) : PGSQLConn<int> = 
    fmapM List.sum <| forM xs fn

let sumForiM (xs:'a list) (fn:int -> 'a -> PGSQLConn<int>) : PGSQLConn<int> = 
    fmapM List.sum <| foriM xs fn

let sumTraverseM (fn: 'a -> PGSQLConn<int>) (source:seq<'a>) : PGSQLConn<int> =
    fmapM Seq.sum <| traverseM fn source

let sumTraverseiM (fn:int -> 'a -> PGSQLConn<int>) (source:seq<'a>) : PGSQLConn<int> =
    fmapM Seq.sum <| traverseiM fn source

let sumSequenceM (source:PGSQLConn<int> list) : PGSQLConn<int> = 
    fmapM List.sum <| sequenceM source


// *************************************
// Errors

let throwError (msg:string) : PGSQLConn<'a> = 
    PGSQLConn <| fun _ -> Failure (errorTrace1 msg)

let swapError (msg:string) (ma:PGSQLConn<'a>) : PGSQLConn<'a> = 
    PGSQLConn <| fun conn -> 
        match apply1 ma conn with
        | Failure stk -> Failure (renameTraceTop msg stk)
        | Success a -> Success a

let augmentError (msg:string) (ma:PGSQLConn<'a>) : PGSQLConn<'a> = 
    PGSQLConn <| fun conn -> 
        match apply1 ma conn with
        | Failure stk -> Failure (augmentErrorTrace msg stk)
        | Success a -> Success a


// *************************************
// Run functions


let private atomically1 (pgconn : NpgsqlConnection) (ma:PGSQLConn<'a>) : Result<'a> = 
    pgconn.Open()
    let trans = pgconn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted)
    try 
        let ans = apply1 ma pgconn
        match ans with
        | Success a -> trans.Commit () ; pgconn.Close(); ans
        | Failure msg -> trans.Rollback () ; pgconn.Close(); ans
    with
    | ex -> trans.Rollback();  pgconn.Close(); Failure (errorTrace1 <| ex.ToString() )


/// Run the script guarded by a transaction / rollback.
/// This is the favoured method to run PostgreSQL operations.
let atomically (connParams:PGSQLConnParams) (ma:PGSQLConn<'a>) : Result<'a> = 
    let connStr = paramsConnString connParams 
    try
        let pgconn : NpgsqlConnection = new NpgsqlConnection(connStr)
        atomically1 pgconn ma  
    with
    | err -> 
        printfn "FAILURE: %s" err.Message
        Failure (errorTrace1 err.Message)


/// Run the script without the guard of a transaction / rollback.
let runUnguarded (connParams:PGSQLConnParams) (ma:PGSQLConn<'a>) : Result<'a> = 
    let connStr = paramsConnString connParams 
    try
        let pgconn : NpgsqlConnection = new NpgsqlConnection(connStr)
        pgconn.Open()
        let a = match ma with | PGSQLConn(f) -> f pgconn
        pgconn.Close()
        a   
    with
    | err -> Failure (errorTrace1 err.Message)



let liftConn (proc:NpgsqlConnection -> 'a) : PGSQLConn<'a> = 
    PGSQLConn <| fun conn -> 
        try 
            let ans = proc conn in Success ans
        with
        | err -> Failure (errorTrace1 <| err.ToString ())  // ToString shows stack trace



let execNonQuery (statement:string) : PGSQLConn<int> = 
    liftConn <| fun conn -> 
        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
        cmd.ExecuteNonQuery ()

//let execPreparedStatement (statement:string) : PGSQLConn<int> = 
//    liftConn <| fun conn -> 
//        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
//        cmd.Prepare()
//        printfn "IsPrepared: %A"       cmd.IsPrepared
//        cmd.ExecuteNonQuery ()

let execReader (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<'a> =
    liftConn <| fun conn -> 
        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
        let reader = cmd.ExecuteReader()
        let ans = proc reader
        reader.Close()
        ans

/// The read procedure (proc) is expected to read from a single row.
/// WARNING - this does not seem to work, possibly releasing the reader
/// before all records are read. Use execReaderList until we have invetigated...
let execReaderSeq (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<seq<'a>> =
    liftConn <| fun conn -> 
        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
        let reader = cmd.ExecuteReader()
        let resultset = 
            seq { while reader.Read() do
                    let ans = proc reader
                    yield ans } 
        reader.Close()
        resultset

// The read procedure (proc) is expected to read from a single row.
let execReaderList (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<'a list> =
    liftConn <| fun conn -> 
        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
        let reader = cmd.ExecuteReader()
        let resultset = 
            seq { while reader.Read() do
                    let ans = proc reader
                    yield ans }  |> Seq.toList
        reader.Close()
        resultset

// The read procedure (proc) is expected to read from a single row.
let execReaderArray (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<'a []> =
    liftConn <| fun conn -> 
        let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
        let reader = cmd.ExecuteReader()
        let resultset = 
            seq { while reader.Read() do
                    let ans = proc reader
                    yield ans }  |> Seq.toArray
        reader.Close()
        resultset


// The read procedure (proc) is expected to read from a single row.
// The query should return exactly one row.
let execReaderSingleton (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<'a> =
    PGSQLConn <| fun conn -> 
        try 
            let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
            let reader = cmd.ExecuteReader()
            if reader.Read() then
                let ans = proc reader
                let hasMore =  reader.Read()
                reader.Close()
                if not hasMore then
                    Success <| ans
                else 
                    Failure <| errorTrace1 "execReaderSingleton - too many results."
            else
                reader.Close ()
                Failure <| errorTrace1 "execReaderSingleton - no results."
        with
        | ex -> Failure (errorTrace1 <| ex.ToString())

/// Err if no answers
let execReaderFirst (statement:string) (proc:NpgsqlDataReader -> 'a) : PGSQLConn<'a> =
    PGSQLConn <| fun conn -> 
        try 
            let cmd : NpgsqlCommand = new NpgsqlCommand(statement, conn)
            let reader = cmd.ExecuteReader()
            if reader.Read() then
                let ans = proc reader
                reader.Close()
                Success <| ans
            else
                reader.Close ()
                Failure <| errorTrace1 "execReaderFirst - no results."
        with
        | ex -> Failure (errorTrace1 <| ex.ToString())



/// Run a ``TRUNCATE TABLE`` query
let deleteAllRows (tableName:string) : PGSQLConn<int> = 
    let query = sprintf "TRUNCATE TABLE %s;" tableName
    execNonQuery query

/// Run a ``TRUNCATE TABLE name RESTART IDENTITY;`` query.
/// Use this for tables with a SERIAL id.
let deleteAllRowsRestartIdentity (tableName:string) : PGSQLConn<int> = 
    let query = sprintf "TRUNCATE TABLE %s RESTART IDENTITY;" tableName
    execNonQuery query
