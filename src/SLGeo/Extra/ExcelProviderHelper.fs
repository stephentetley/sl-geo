// Copyright (c) Stephen Tetley 2018
// License: BSD 3 Clause

module SLGeo.Extra.ExcelProviderHelper

// The Excel Type Provider seems to read a trailing null row.
// This dictionary and procedure provide a skeleton to get round this.

type GetRowsDict<'table, 'row> = 
    { GetRows : 'table -> seq<'row>
      NotNullProc : 'row -> bool }

let excelTableGetRowsSeq (dict:GetRowsDict<'table,'row>) (table:'table) : seq<'row> = 
    let allrows = dict.GetRows table
    allrows |> Seq.filter dict.NotNullProc

let excelTableGetRows (dict:GetRowsDict<'table,'row>) (table:'table) : 'row list = 
    let allrows = dict.GetRows table
    allrows |> Seq.filter dict.NotNullProc |> Seq.toList


/// F# design guidelines say favour object-interfaces rather than records of functions...
type IExcelProviderHelper<'table,'row> = 
    abstract member ReadTableRows : 'table -> seq<'row>
    abstract member IsBlankRow: 'row -> bool

let excelReadRows (helper:IExcelProviderHelper<'table,'row>) (table:'table) : seq<'row> = 
    let allrows = helper.ReadTableRows table
    allrows |> Seq.filter (not << helper.IsBlankRow)


let excelReadRowsAsList (helper:IExcelProviderHelper<'table,'row>) (table:'table) : 'row list = 
    excelReadRows helper table |> Seq.toList
