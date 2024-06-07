package tree

import "fmt"

const M = 3
const MID = (M/2)

type Node struct{
    isRoot bool
    isLeaf bool
    next *Node
    nkeys uint64
    keys [M]uint64
    values [M][]byte
    children [M+1]*Node
}

var Root *Node
var altRoot *Node

func getNode() *Node {
    var node = new(Node)
    return node
}

func Create(){
    Root = getNode()
    Root.isLeaf = true
    Root.isRoot = true
    Root.nkeys = 0

    fmt.Println("Tree initialized")
}

func makeCopy(node *Node) *Node{
    newNode := getNode()
    newNode.isLeaf = node.isLeaf
    newNode.isRoot = node.isRoot
    newNode.nkeys = node.nkeys
    newNode.next = node.next

    if(newNode.isRoot == true){
        altRoot = newNode
    }

    for i := 0; i < int(node.nkeys); i++ {
        newNode.keys[i] = node.keys[i]
    }
    
    if(newNode.isLeaf == true){
        for i := 0; i < int(node.nkeys); i++ {
            newNode.values[i] = node.values[i]
        }
    }else{
        for i := 0; i < int(node.nkeys+1); i++ {
            newNode.children[i] = node.children[i]
        }
    }

    return newNode
}

func findParent(node *Node ,key uint64, curLevel uint32, wLevel uint32) *Node{
    var index uint64
    for index = 0; index < node.nkeys; index++ {
        if(node.keys[index] >= key){
            break
        }
    }
    if(curLevel == wLevel){
        return node
    }else{
        return findParent(node.children[index], key, curLevel+1, wLevel)
    }
}

func changeRoot(){
    Root = altRoot
}

func Insert(node *Node, key uint64, value *[]byte, level uint32){

    var index uint64
    if(node.isRoot == true){
        node = makeCopy(node)
    }
    
    if(node.isLeaf == false){
        for index = 0; index < node.nkeys; index++ {
            if(node.keys[index] >= key){
                break
            }
        }
        newChild := makeCopy(node.children[index])
        node.children[index] = newChild
        Insert(newChild,key,value,level+1)
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
        k, first, second := splitLeaf(node)
        if(node.isRoot == true){
            newRoot := getNode()
            newRoot.isRoot = true
            insertInner(newRoot, k, first, second, level)
            altRoot = newRoot
        }else{
            parent := findParent(altRoot, key, 0, level-1)
            insertInner(parent, k, first, second, level-1)
        }
    }

    changeRoot();
}

func insertInner(
    node *Node,
    key uint64,
    first *Node,
    second *Node,
    pLevel uint32,
){

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

    if(node.nkeys >= M){
        k, first, second := splitInner(node)
        if(node.isRoot == true){
            newRoot := getNode()
            newRoot.isRoot = true
            insertInner(newRoot, k, first, second, pLevel)
            altRoot = newRoot
        }else{
            parent := findParent(altRoot, key, 0, pLevel-1)
            insertInner(parent, k, first, second, pLevel-1)
        }
    }
}

func splitInner(node *Node) (uint64, *Node, *Node){
    first := getNode()
    second := getNode()

    first.isLeaf, second.isLeaf = false, false

    first.nkeys = MID
    second.nkeys = M-MID-1

    for i := 0; i < MID; i++ {
        first.keys[i] = node.keys[i]
        first.children[i] = node.children[i]
    }
    first.children[MID] = node.children[MID]

    for i := MID+1; i < M; i++ {
        second.keys[i-MID-1] = node.keys[i]
        second.children[i-MID-1] = node.children[i]
    }
    second.children[M-MID-1] = node.children[M]

    return node.keys[MID], first, second
}

func splitLeaf(node *Node) (uint64, *Node, *Node){
    first := getNode()
    second := getNode()

    first.isLeaf, second.isLeaf = true, true

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
    return node.keys[MID], first, second
}

func Delete(node *Node, key uint64, level uint32){

    var index uint64
    if(node.isRoot == true){
        node = makeCopy(node)
    }

    if(node.isLeaf == false){
        var isInner bool = false
        for index = 0; index < node.nkeys; index++ {
            if(node.keys[index] > key){
                break
            }
            if(node.keys[index] == key){
                isInner = true
            }
        }

        newChild := makeCopy(node.children[index])
        node.children[index] = newChild
        Delete(newChild,key,level+1)

        if(isInner == true){
            deleteInner(altRoot,key)
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
        if(node.isRoot == true){
            return
        }
        isLeaf := true
        fill(node, childKey, isLeaf, level)
    }

    changeRoot()
}

func fill(child *Node, childKey uint64, isLeaf bool, level uint32){

    var cIndex uint64
    var ocIndex uint64
    parent := findParent(altRoot, childKey, 0, level-1)

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

    otherChild := makeCopy(parent.children[ocIndex])
    parent.children[ocIndex] = otherChild

    if(otherChild.nkeys == MID){
        var passIndex uint64
        if(cIndex < ocIndex){
            passIndex = ocIndex
        }else{
            passIndex = cIndex
            child, otherChild = otherChild, child
        }
            
        if(isLeaf == true){
            merge(child,otherChild,parent,passIndex,level)
        }else{
            mergeInner(child,otherChild,parent,passIndex,level)
        }
        return
    }

    if(isLeaf == true){
        if(ocIndex > cIndex){
            child.keys[child.nkeys] = otherChild.keys[0]
            child.values[child.nkeys] = otherChild.values[0]
            child.nkeys += 1
            
            Delete(otherChild, otherChild.keys[0], 0)
            parent.keys[cIndex] = otherChild.keys[0]
        }else{
            Insert(
                child,
                otherChild.keys[otherChild.nkeys-1],
                &otherChild.values[otherChild.nkeys-1],
                0,
            )
            Delete(otherChild, otherChild.keys[otherChild.nkeys-1], 0)
            parent.keys[cIndex-1] = child.keys[0]
        }
    }else{
        if(ocIndex > cIndex){
            child.keys[child.nkeys] = parent.keys[cIndex]
            child.children[child.nkeys+1] = otherChild.children[0]
            child.nkeys += 1
            
            parent.keys[cIndex] = otherChild.keys[0]

            for i := 0; i < int(otherChild.nkeys)-1; i++ {
                otherChild.keys[i] = otherChild.keys[i+1]
                otherChild.children[i] = otherChild.children[i+1]
            }
            otherChild.keys[otherChild.nkeys-1] = 0
            otherChild.children[otherChild.nkeys-1] =
            otherChild.children[otherChild.nkeys]
            otherChild.children[otherChild.nkeys] = nil
            otherChild.nkeys -= 1

        }else{

            child.keys[child.nkeys] = parent.keys[parent.nkeys-1]
            child.children[child.nkeys+1] =
            otherChild.children[otherChild.nkeys]
            child.nkeys += 1

            parent.keys[parent.nkeys-1] = otherChild.keys[otherChild.nkeys-1]
            
            for i := child.nkeys-1; i > 0; i++ {
                child.keys[i], child.keys[i-1] = child.keys[i-1], child.keys[i]
                child.children[i+1], child.children[i] =
                child.children[i], child.children[i+1]
            }
            child.children[0], child.children[1] =
            child.children[1], child.children[0]

            otherChild.children[otherChild.nkeys] = nil
            otherChild.keys[otherChild.nkeys-1] = 0
            otherChild.nkeys -= 1
        }
    }
    
}

func merge(
    first *Node,
    second *Node,
    parent *Node,
    sIndex uint64,
    level uint32,
){
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

    if(parent.isRoot == false && parent.nkeys < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot == true && parent.nkeys == 0){
        altRoot = first
    }
}

func mergeInner(
    first *Node,
    second *Node,
    parent *Node,
    sIndex uint64,
    level uint32,
){
    childKey := parent.keys[0]
    first.keys[first.nkeys] = parent.keys[sIndex-1]
    first.nkeys += 1

    for i := 0; i < int(second.nkeys); i++ {
        first.keys[first.nkeys] = second.keys[i]
        first.children[first.nkeys] = second.children[i]
        first.nkeys += 1
    }
    first.children[first.nkeys] = second.children[second.nkeys]

    for i := sIndex-1; i < parent.nkeys -1; i++ {
        parent.keys[i] = parent.keys[i+1]
        parent.children[i+1] = parent.children[i+2]
    }
    parent.children[parent.nkeys] = nil
    parent.nkeys -= 1

    if(parent.isRoot == false && parent.nkeys < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot == true && parent.nkeys == 0){
        altRoot = first
    }
}

func deleteInner(node *Node, key uint64){
    var index uint64
    if(node.isLeaf == true){
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
        if(temp.isLeaf == true){
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
