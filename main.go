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
    store.STOP = true
    store.Insert(store.RootByte, append(k, 105), v, 0)

    //for i := 0; i < 1400; i += 50 {
    //    fmt.Println(store.FILE[i:i+50])
    //}

    store.PrintTree(store.RootByte, 0)
}
