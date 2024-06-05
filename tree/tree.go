package tree

import "fmt"

const M = 3
const MID = (M/2)

type Node struct{
    leaf bool
    next *Node
    parent *Node
    nkeys uint64
    keys [M]uint64
    values [M][]byte
    children [M+1]*Node
}

var Root *Node

func getNode(leaf bool) *Node {
    var node = new(Node)
    node.leaf = leaf
    return node
}

func Create(){
    Root = getNode(true)
    Root.leaf = true
    Root.nkeys = 0

    fmt.Println("Tree initialized")
}

func makeChild(parent *Node, child *Node, index uint64){
    parent.children[index] = child
    child.parent = parent
}

func Insert(node *Node, key uint64, value *[]byte){
    var index uint64
    
    if(node.leaf == false){
        for index = 0; index < node.nkeys; index++ {
            if(node.keys[index] >= key){
                break
            }
        }
        Insert(node.children[index],key,value)
        return
    }

    for index = 0; index < node.nkeys; index++ {
        if(node.keys[index] > key){
            break
        }
        if(node.keys[index] == key){
            node.values[index] = *value
            return
        }
    }

    node.values[node.nkeys] = *value
    node.keys[node.nkeys] = key
    for i := node.nkeys; i > index; i-- {
        node.keys[i], node.keys[i-1] = node.keys[i-1], node.keys[i]
        node.values[i], node.values[i-1] =
        node.values[i-1], node.values[i]
    }
    node.nkeys += 1

    if(node.nkeys >= M){
        splitLeaf(node)
    }
}

func insertInner(node *Node, key uint64, first *Node, second *Node){
    if(node == nil){
        node = getNode(false)
        node.nkeys = 0
        node.parent = nil
        Root = node
    }

    var index uint64
    for index = 0; index < node.nkeys; index++ {
        if(node.keys[index] >= key){
            break
        }
    }

    node.keys[node.nkeys] = key
    node.children[node.nkeys+1] = second
    for i := node.nkeys; i > index; i-- {
        node.keys[i], node.keys[i-1] = node.keys[i-1], node.keys[i]
        node.children[i+1], node.children[i] =
        node.children[i], node.children[i+1]
    }

    node.nkeys += 1
    node.children[index] = first

    makeChild(node,first,index)
    makeChild(node,second,index+1)

    if(node.nkeys >= M){
        splitInner(node)
    }
}

func splitInner(node *Node){
    first := getNode(false)
    second := getNode(false)

    first.nkeys = MID
    second.nkeys = M-MID-1

    for i := 0; i < MID; i++ {
        first.keys[i] = node.keys[i]
        makeChild(first, node.children[i], uint64(i))
    }
    makeChild(first, node.children[MID], uint64(MID))

    for i := MID+1; i < M; i++ {
        second.keys[i-MID-1] = node.keys[i]
        makeChild(second, node.children[i], uint64(i-MID-1))
    }
    makeChild(second, node.children[M], uint64(M-MID-1))

    insertInner(node.parent, node.keys[MID], first, second)
}

func splitLeaf(node *Node){
    first := getNode(true)
    second := getNode(true)

    first.nkeys = MID
    second.nkeys = M - MID

    for i := 0; i < MID; i++ {
        first.keys[i] = node.keys[i]
        first.values[i] = node.values[i]
    }

    for i := MID; i < M; i++ {
        second.keys[i - MID] = node.keys[i]
        second.values[i - MID] = node.values[i]
    }

    first.next = second
    insertInner(node.parent, node.keys[MID], first, second)
}

func Delete(node *Node, key uint64){
    var index uint64

    if(node.leaf == false){
        var isInner bool = false
        for index = 0; index < node.nkeys; index++ {
            if(node.keys[index] > key){
                break
            }
            if(node.keys[index] == key){
                isInner = true
            }
        }
        Delete(node.children[index],key)
        if(isInner == true){
            deleteInner(Root,key)
        }
        return
    }

    childKey := node.keys[0]

    for index = 0; index < node.nkeys; index++ {
        if(node.keys[index] == key){
            break
        }
    }
    
    for i := index; i < node.nkeys; i++ {
        node.keys[i] = node.keys[i+1]
        node.values[i] = node.values[i+1]
    }
    node.keys[node.nkeys] = 0
    node.values[node.nkeys] = nil
    node.nkeys -= 1

    if(node.nkeys < MID){
        isLeaf := true
        fill(node, childKey, isLeaf)
    }
}

func fill(child *Node, childKey uint64, isLeaf bool){

    var cIndex uint64
    var ocIndex uint64
    parent := child.parent

    for cIndex = 0; cIndex < parent.nkeys; cIndex++ {
        if(parent.keys[cIndex] > childKey){
            break
        }
    }

    if(cIndex == parent.nkeys){
        ocIndex = cIndex-1
    }else{
        ocIndex = cIndex+1
    }

    otherChild := parent.children[ocIndex]
    if(otherChild.nkeys == MID){
        var passIndex uint64
        if(cIndex < ocIndex){
            passIndex = ocIndex
        }else{
            passIndex = cIndex
            child, otherChild = otherChild, child
        }
            
        if(isLeaf == true){
            merge(child,otherChild,passIndex)
        }else{
            mergeInner(child,otherChild,passIndex)
        }
        return
    }

    if(isLeaf == true){
        if(ocIndex > cIndex){
            child.keys[child.nkeys] = otherChild.keys[0]
            child.values[child.nkeys] = otherChild.values[0]
            child.nkeys += 1
            
            Delete(otherChild, otherChild.keys[0])
            parent.keys[cIndex] = otherChild.keys[0]
        }else{
            Insert(
                child,
                otherChild.keys[otherChild.nkeys-1],
                &otherChild.values[otherChild.nkeys-1],
            )
            Delete(otherChild, otherChild.keys[otherChild.nkeys-1])
            parent.keys[cIndex-1] = child.keys[0]
        }
    }else{
        if(ocIndex > cIndex){
            child.keys[child.nkeys] = parent.keys[cIndex]
            makeChild(child, otherChild.children[0], child.nkeys+1)
            child.nkeys += 1

            for i := 0; i < int(otherChild.nkeys)-1; i++ {
                otherChild.keys[i] = otherChild.keys[i+1]
                otherChild.children[i] = otherChild.children[i+1]
            }
            otherChild.keys[otherChild.nkeys-1] = 0
            otherChild.children[otherChild.nkeys-1] =
            otherChild.children[otherChild.nkeys]
            otherChild.children[otherChild.nkeys] = nil
            otherChild.nkeys -= 1

            parent.keys[cIndex] = otherChild.keys[0]

        }else{

            child.keys[child.nkeys] = parent.keys[parent.nkeys-1]
            makeChild(child, otherChild.children[otherChild.nkeys], child.nkeys+1)
            child.nkeys += 1
            
            for i := child.nkeys-1; i > 0; i++ {
                child.keys[i], child.keys[i-1] = child.keys[i-1], child.keys[i]
                child.children[i+1], child.children[i] =
                child.children[i], child.children[i+1]
            }
            child.children[0], child.children[1] =
            child.children[1], child.children[0]

            parent.keys[parent.nkeys-1] = otherChild.keys[otherChild.nkeys-1]

            otherChild.children[otherChild.nkeys] = nil
            otherChild.keys[otherChild.nkeys-1] = 0
            otherChild.nkeys -= 1
        }
    }
    
}

func merge(
    first *Node,
    second *Node,
    sIndex uint64,
){
    parent := first.parent
    childKey := parent.keys[0]
    
    for i := 0; i < int(second.nkeys); i++ {
        first.keys[first.nkeys] = second.keys[i]
        first.values[first.nkeys] = second.values[i]
        first.nkeys += 1
    }

    for i := sIndex-1; i < parent.nkeys-1; i++ {
        parent.keys[i] = parent.keys[i+1]
        parent.children[i+1] = parent.children[i+2]
    }
    parent.keys[parent.nkeys-1] = 0
    parent.children[parent.nkeys] = nil
    parent.nkeys -= 1

    if(parent != Root && parent.nkeys < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf)
    }
    if(parent == Root && parent.nkeys == 0){
        Root = first
    }
}

func mergeInner(
    first *Node,
    second *Node,
    sIndex uint64,
){
    parent := first.parent
    childKey := parent.keys[0]
    first.keys[first.nkeys] = parent.keys[sIndex-1]
    first.nkeys += 1

    for i := 0; i < int(second.nkeys); i++ {
        first.keys[first.nkeys] = second.keys[i]
        makeChild(first, second.children[i], first.nkeys)
        first.nkeys += 1
    }
    makeChild(first, second.children[second.nkeys], first.nkeys)

    for i := sIndex-1; i < parent.nkeys -1; i++ {
        parent.keys[i] = parent.keys[i+1]
        parent.children[i+1] = parent.children[i+2]
    }
    parent.children[parent.nkeys] = nil
    parent.nkeys -= 1

    if(parent != Root && parent.nkeys < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf)
    }
    if(parent == Root && parent.nkeys == 0){
        Root = first
    }
}

func deleteInner(node *Node, key uint64){
    var index uint64
    if(node.leaf == true){
        return
    }
    for index = 0; index < node.nkeys; index++ {
        if(node.keys[index] > key){
            deleteInner(node.children[index], key)
            return
        }
        if(node.keys[index] == key){
            break
        }
    }

    if(index == node.nkeys){
        deleteInner(node.children[node.nkeys], key)
        return
    }

    var newkey uint64
    temp := node.children[index+1]
    for{
        if(temp.leaf == true){
            newkey = temp.keys[0]
            break
        }
        temp = temp.children[0]
    }

    node.keys[index] = newkey
}

func PrintTree(node *Node, level int){
    fmt.Println("Level:", level)
    fmt.Println(node)

    for i := range len(node.children){
        if(node.children[i] != nil){
            PrintTree(node.children[i], level+1)
        }
    }
}
