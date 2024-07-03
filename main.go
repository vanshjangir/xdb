package main

import (
	"fmt"
	"github.com/vanshjangir/xdb/table"
)

func main(){
    fmt.Println("Starting...")
    var k []byte
    var v []byte

    var table table.Table
    table.CreateTable("first")

    table.Insert(append(k, 05), append(v,105))
    table.Insert(append(k, 15), append(v,115))
    table.Insert(append(k, 25), append(v,125))
    table.Insert(append(k, 35), append(v,135))
    table.Insert(append(k, 45), append(v,145))
    table.Insert(append(k, 55), append(v,155))
    table.Insert(append(k, 65), append(v,165))
    table.Insert(append(k, 75), append(v,175))
    table.Insert(append(k, 85), append(v,185))
    table.Insert(append(k, 95), append(v,195))
    table.Print()
}
