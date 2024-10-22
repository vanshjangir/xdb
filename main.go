package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/storage"
)

func main(){
    fmt.Println("Starting...")
    var k []byte
    var v []byte

    tx := new(storage.Transaction)
    tx.Init()
    
    tx.Begin()

    var table storage.Table
    table.Init(tx)
    table.CreateTable("first")

    table.Insert(append(k, 05), map[string][]byte{"firstcol": append(v, 105),"secondcol": append(v, 205)})
    table.Insert(append(k, 15), map[string][]byte{"firstcol": append(v, 115),"secondcol": append(v, 215)})
    table.Insert(append(k, 25), map[string][]byte{"firstcol": append(v, 125),"secondcol": append(v, 225)})
    table.Insert(append(k, 35), map[string][]byte{"firstcol": append(v, 135),"secondcol": append(v, 235)})
    table.Insert(append(k, 45), map[string][]byte{"firstcol": append(v, 145),"secondcol": append(v, 245)})
    table.Insert(append(k, 55), map[string][]byte{"firstcol": append(v, 135),"secondcol": append(v, 245)})

    tx.Commit()
    table.Print()

    fmt.Println(table.Range(append(k, 5), append(k,195)))
    fmt.Println(table.GetPkey("firstcol", append(v,135)))
    fmt.Println(table.RangeIdx("firstcol", append(v,135), append(v,145)))
}
