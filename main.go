package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/store"
)

func main(){
    fmt.Println("Starting...")
    var k []byte
    var v []byte

    v = append(v, 100)
    
    store.CreateByte()
    store.MmapInit()
    store.Insert(append(k, 05), v)
    store.Insert(append(k, 15), v)
    store.Insert(append(k, 25), v)
    store.Insert(append(k, 35), v)
    store.Insert(append(k, 45), v)
    store.Insert(append(k, 55), v)
    store.Insert(append(k, 65), v)
    store.Insert(append(k, 75), v)
    store.Insert(append(k, 85), v)
    store.Insert(append(k, 95), v)
   
    store.Delete(append(k, 85))
    var s []byte
    store.Update(append(k, 95), append(s, 200))

    store.Print()
}
