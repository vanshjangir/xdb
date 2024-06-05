package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/tree"
)

func main(){
    fmt.Println("Starting...")

    var v []byte

    v = append(v, 100)

    tree.Create()
    tree.Insert(tree.Root, 5, &v)
    tree.Insert(tree.Root, 15, &v)
    tree.Insert(tree.Root, 25, &v)
    tree.Insert(tree.Root, 35, &v)
    tree.Insert(tree.Root, 45, &v)
    tree.Insert(tree.Root, 55, &v)
    tree.Insert(tree.Root, 65, &v)
    tree.Insert(tree.Root, 75, &v)

    tree.Delete(tree.Root, 55)

    tree.PrintTree(tree.Root,1)
}
