package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/storage"
)

func main(){
    fmt.Println("Starting...")
    var k []byte
    var v []byte

    v = append(v, 100)

    var kv storage.KV
    kv.Create("files/xdb")

    kv.Insert(append(k, 05), v)
    kv.Insert(append(k, 15), v)
    kv.Insert(append(k, 25), v)
    kv.Insert(append(k, 35), v)
    kv.Insert(append(k, 45), v)
    kv.Insert(append(k, 55), v)
    kv.Insert(append(k, 65), v)
    kv.Insert(append(k, 75), v)
    kv.Insert(append(k, 85), v)
    kv.Insert(append(k, 95), v)
   
    kv.Print()
}
