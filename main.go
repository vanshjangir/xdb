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
    tree.Insert(tree.Root, 5, &v, 0)
    tree.Insert(tree.Root, 15, &v, 0)
    tree.Insert(tree.Root, 25, &v, 0)
    tree.Insert(tree.Root, 35, &v, 0)
    tree.Insert(tree.Root, 45, &v, 0)
    tree.Insert(tree.Root, 55, &v, 0)
    tree.Insert(tree.Root, 65, &v, 0)
    tree.Insert(tree.Root, 75, &v, 0)
    tree.Insert(tree.Root, 85, &v, 0)
    tree.Insert(tree.Root, 95, &v, 0)

    tree.Delete(tree.Root, 15, 0)

    tree.PrintTree(tree.Root,0)
}
