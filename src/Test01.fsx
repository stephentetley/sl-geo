#I @"..\packages\FParsec.1.0.3\lib\portable-net45+win8+wp8+wpa81"
#r "FParsec"
#r "FParsecCS"

#I @"..\packages\Npgsql.4.0.0-preview2\lib\net451"
#I @"..\packages\System.Threading.Tasks.Extensions.4.4.0\lib\portable-net45+win8+wp8+wpa81"
#r "Npgsql"
open Npgsql


#load "SL\Geo\Tolerance.fs"
#load "SL\Geo\Coord.fs"
#load "SL\Geo\WellKnownText.fs"
#load "SL\Geo\WebMercator.fs"
#load "SL\PostGIS\AnswerMonad.fs"
#load "SL\PostGIS\SqlUtils.fs"
#load "SL\PostGIS\PGSQLConn.fs"
open SL.Geo


let HelloWorld () = 
    printfn "All loaded"
