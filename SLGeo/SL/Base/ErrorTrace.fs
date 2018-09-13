// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SL.Base.ErrorTrace

open System.Text

type ErrorTrace = ErrorTrace of string * list<ErrorTrace>

let getErrorTrace (trace:ErrorTrace) : string = 
    let writeLine (depth:int) (str:string) (sb:StringBuilder) : StringBuilder = 
        let line = sprintf "%s %s" (String.replicate depth "*") str
        sb.AppendLine(line)
    let rec work (e1:ErrorTrace) (depth:int) (sb:StringBuilder) : StringBuilder  = 
        match e1 with
        | ErrorTrace (s,[]) -> writeLine depth s sb
        | ErrorTrace (s,xs) ->
            let sb1 = writeLine depth s sb
            List.fold (fun buf branch -> work branch (depth+1) buf) sb1 xs
    work trace 0 (new StringBuilder()) |> fun sb -> sb.ToString()

/// Create a fresh error message.
let  errorTrace1 (errMsg:string) : ErrorTrace = 
    ErrorTrace(errMsg, [])

let concatErrorTrace (errMsg:string) (failures:ErrorTrace list) : ErrorTrace = 
    ErrorTrace(errMsg, failures)

let augmentErrorTrace (errMsg:string) (trace:ErrorTrace) : ErrorTrace = 
    ErrorTrace(errMsg, [trace])

let renameTraceTop (errMsg:string) (trace:ErrorTrace) = 
    match trace with | ErrorTrace (_,stk) -> ErrorTrace (errMsg,stk)