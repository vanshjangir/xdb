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
    store.Insert(store.RootByte, append(k, 5), v, 0)
    store.Insert(store.RootByte, append(k, 15), v, 0)
    store.Insert(store.RootByte, append(k, 25), v, 0)
    store.Insert(store.RootByte, append(k, 35), v, 0)
    store.Insert(store.RootByte, append(k, 45), v, 0)
    store.Insert(store.RootByte, append(k, 55), v, 0)
    store.Insert(store.RootByte, append(k, 65), v, 0)
    store.Insert(store.RootByte, append(k, 75), v, 0)
    store.Insert(store.RootByte, append(k, 85), v, 0)
    store.Insert(store.RootByte, append(k, 95), v, 0)

    store.Delete(store.RootByte, append(k, 15), 0)

    store.PrintTree(store.RootByte, 0)
}
