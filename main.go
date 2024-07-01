package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/storage"
)

func main(){
    fmt.Println("Starting...")
    var k []byte
    var v []byte

    var kv storage.KV
    kv.Create("files/xdb")

    kv.Insert(append(k, 05), append(v,105))
    kv.Insert(append(k, 15), append(v,115))
    kv.Insert(append(k, 25), append(v,125))
    kv.Insert(append(k, 35), append(v,135))
    kv.Insert(append(k, 45), append(v,145))
    kv.Insert(append(k, 55), append(v,155))
    kv.Insert(append(k, 65), append(v,165))
    kv.Insert(append(k, 75), append(v,175))
    kv.Insert(append(k, 85), append(v,185))
    kv.Insert(append(k, 95), append(v,195))

   
    kv.Print()
    fmt.Println(kv.Range(append(k, 0), append(k, 105)))
}
