package store

import (
	"bytes"
	"fmt"
)

const M = 3
const MID = (M/2)
const TREE_PAGE_SIZE = 4096
var STOP bool = false


type Node struct{
    isRoot bool
    isLeaf bool
    nkeys uint32
    children [M+1]*Node
    keys [M][]byte
    values [M][]byte
    next *Node
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

func findParent(node *NodeByte ,key []byte, curLevel uint32, wLevel uint32) *NodeByte{
    var index uint16
    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr > 0){
            break
        }
    }
    if(curLevel == wLevel){
        return node
    }else{
        return findParent(node.children(index), key, curLevel+1, wLevel)
    }
}

func changeRoot(){
    Root = altRoot
}

func Insert(node *NodeByte, key []byte, value []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node,_ = makeByteCopy(node)
    }
    
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr >= 0){
                break
            }
        }
        newChild, offset := makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        Insert(newChild, key, value, level+1)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr > 0){
            break
        }
        if(cr == 0){
            // update the kv pair coming soon
            return
        }
    }

    node.addKV(node.nkeys(), uint16(len(key)), key, uint16(len(value)), value)
    for i := node.nkeys(); i > index; i-- {
        off := node.keyOffset(i)
        node.setKeyOffset(i,node.keyOffset(i-1))
        node.setKeyOffset(i-1,off)
    }
    node.setNkeys(node.nkeys()+1)

    if(node.nkeys() >= M){
        k, firstPtr, secondPtr := splitLeaf(node)
        if(node.isRoot() == true){
            newRootByte, off := getNodeByte()
            newRootByte.setType(TYPE_ROOT_I)
            insertInner(newRootByte, k, firstPtr, secondPtr, level)
            altRootByte = newRootByte
            altRootOffset = off
        }else{
            parent := findParent(altRootByte, key, 0, level-1)
            insertInner(parent, k, firstPtr, secondPtr, level-1)
        }
    }

    changeRootByte();
}

func insertInner(
    node *NodeByte,
    key []byte,
    firstPtr uint64,
    secondPtr uint64,
    pLevel uint32,
){

    var index uint16
    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr >= 0){
            break
        }
    }

    node.addKC(node.nkeys(), uint16(len(key)), key, secondPtr)
    for i := node.nkeys(); i > index; i-- {
        off := node.keyOffset(i)
        node.setKeyOffset(i, node.keyOffset(i-1))
        node.setKeyOffset(i-1, off)
    }

    node.setNkeys(node.nkeys()+1)
    node.setCptr(index, firstPtr)

    if(node.nkeys() >= M){
        k, firstPtr, secondPtr := splitInner(node)
        if(node.isRoot() == true){
            newRootByte, off := getNodeByte()
            newRootByte.setType(TYPE_ROOT_I)
            insertInner(newRootByte, k, firstPtr, secondPtr, pLevel)
            altRootByte = newRootByte
            altRootOffset = off
        }else{
            parent := findParent(altRootByte, key, 0, pLevel-1)
            insertInner(parent, k, firstPtr, secondPtr, pLevel-1)
        }
    }
}

func splitInner(node *NodeByte) ([]byte, uint64, uint64){
    first, firstPtr := getNodeByte()
    second, secondPtr := getNodeByte()

    first.setType(TYPE_I)
    second.setType(TYPE_I)

    for i := 0; i < MID; i++ {
        first.addKC(
            uint16(i),
            uint16(len(node.key(uint16(i)))),
            node.key(uint16(i)),
            node.cptr(uint16(i+1)),
        )
        first.setNkeys(first.nkeys()+1)
    }
    first.setCptr(0, node.cptr(0))

    for i := MID+1; i < M; i++ {
        second.addKC(
            uint16(i-MID-1),
            uint16(len(node.key(uint16(i)))),
            node.key(uint16(i)),
            node.cptr(uint16(i+1)),
        )
        second.setNkeys(second.nkeys()+1)
    }
    second.setCptr(0, node.cptr(MID+1))

    return node.key(MID), firstPtr, secondPtr
}

func splitLeaf(node *NodeByte) ([]byte, uint64, uint64){
    first, firstPtr := getNodeByte()
    second, secondPtr := getNodeByte()

    first.setType(TYPE_L)
    second.setType(TYPE_L)

    var i uint16
    for i = 0; i < MID; i++ {
        first.addKV(
            uint16(i),
            uint16(len(node.key(i))),
            node.key(i),
            uint16(len(node.value(i))),
            node.value(i),
        )
        first.setNkeys(first.nkeys()+1)
    }

    for i = MID; i < M; i++ {
        second.addKV(
            i-MID,
            uint16(len(node.key(i))),
            node.key(i),
            uint16(len(node.value(i))),
            node.value(i),
        )
        second.setNkeys(second.nkeys()+1)
    }

    first.setNext(secondPtr)
    return node.key(MID), firstPtr, secondPtr
}

func Delete(node *NodeByte, key []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node, _ = makeByteCopy(node)
    }

    if(node.isLeaf() == false){
        var isInner bool = false
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
            if(cr == 0){
                isInner = true
            }
        }

        newChild, offset := makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        Delete(newChild,key,level+1)

        if(isInner == true){
            deleteInner(altRootByte,key)
        }
        return
    }

    childKey := node.key(0)

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr == 0){
            break
        }
    }
    
    for i := index; i < node.nkeys(); i++ {
        node.setKeyOffset(i, node.keyOffset(i+1))
    }
    node.setNkeys(node.nkeys()-1)

    if(node.nkeys() < MID){
        if(node.isRoot() == true){
            return
        }
        isLeaf := true
        fill(node, childKey, isLeaf, level)
    }

    changeRootByte()
}

func fill(child *NodeByte, childKey []byte, isLeaf bool, level uint32){

    var cIndex uint16
    var ocIndex uint16
    parent := findParent(altRootByte, childKey, 0, level-1)

    for cIndex = 0; cIndex < parent.nkeys(); cIndex++ {
        cr := bytes.Compare(parent.key(cIndex), childKey)
        if(cr > 0){
            break
        }
    }

    if(cIndex == parent.nkeys()){
        ocIndex = cIndex-1
    }else{
        ocIndex = cIndex+1
    }

    otherChild, ocOffset := makeByteCopy(parent.children(ocIndex))
    parent.setCptr(ocIndex, ocOffset)

    if(otherChild.nkeys() == MID){
        var passIndex uint16
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
            //child.keys[child.nkeys] = otherChild.keys[0]
            //child.values[child.nkeys] = otherChild.values[0]
            child.addKV(
                child.nkeys(),
                otherChild.klen(0),
                otherChild.key(0),
                otherChild.vlen(0),
                otherChild.value(0),
            )
            child.setNkeys(child.nkeys()+1)
            
            Delete(otherChild, otherChild.key(0), 0)
            //parent.keys[cIndex] = otherChild.keys[0]
            parent.addKC(
                cIndex,
                otherChild.klen(0),
                otherChild.key(0),
                parent.cptr(cIndex+1),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))
        }else{
            Insert(
                child,
                otherChild.key(otherChild.nkeys()-1),
                otherChild.value(otherChild.nkeys()-1),
                0,
            )
            Delete(otherChild, otherChild.key(otherChild.nkeys()-1), 0)
            //parent.keys[cIndex-1] = child.keys[0]
            parent.addKC(
                cIndex-1,
                child.klen(0),
                child.key(0),
                parent.cptr(cIndex),
            )
        }
    }else{
        if(ocIndex > cIndex){
            //child.keys[child.nkeys] = parent.keys[cIndex]
            //child.children[child.nkeys+1] = otherChild.children[0]
            //child.nkeys += 1

            child.addKC(
                child.nkeys(),
                parent.klen(cIndex),
                parent.key(cIndex),
                otherChild.cptr(0),
            )
            child.setNkeys(child.nkeys()+1)
            
            //parent.keys[cIndex] = otherChild.keys[0]
            parent.addKC(
                cIndex,
                otherChild.klen(0),
                otherChild.key(0),
                parent.cptr(cIndex+1),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))

            otherChild.setCptr(0, otherChild.cptr(1))
            for i := uint16(0); i < otherChild.nkeys()-1; i++ {
                //otherChild.keys[i] = otherChild.keys[i+1]
                //otherChild.children[i] = otherChild.children[i+1]
                otherChild.setKeyOffset(i, otherChild.keyOffset(i+1))
            }
            otherChild.setNkeys(otherChild.nkeys()-1)

        }else{

            //child.keys[child.nkeys] = parent.keys[parent.nkeys-1]
            //child.children[child.nkeys+1] =
            //otherChild.children[otherChild.nkeys]
            //child.nkeys += 1

            child.addKC(
                child.nkeys(),
                parent.klen(parent.nkeys()-1),
                parent.key(parent.nkeys()-1),
                otherChild.cptr(otherChild.nkeys()),
            )
            child.setNkeys(child.nkeys()+1)

            //parent.keys[parent.nkeys-1] = otherChild.keys[otherChild.nkeys-1]
            parent.addKC(
                parent.nkeys()-1,
                otherChild.klen(otherChild.nkeys()-1),
                otherChild.key(otherChild.nkeys()-1),
                parent.cptr(parent.nkeys()),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))
            
            for i := child.nkeys()-1; i > 0; i++ {
                //child.keys[i], child.keys[i-1] = child.keys[i-1], child.keys[i]
                //child.children[i+1], child.children[i] =
                //child.children[i], child.children[i+1]
                off := child.keyOffset(i)
                child.setKeyOffset(i, child.keyOffset(i-1))
                child.setKeyOffset(i-1, off)
            }
            //child.children[0], child.children[1] =
            //child.children[1], child.children[0]
            off := child.keyOffset(0)
            child.setKeyOffset(0, child.keyOffset(1))
            child.setKeyOffset(1, off)

            //otherChild.nkeys -= 1
            otherChild.setNkeys(otherChild.nkeys()-1)
        }
    }
    
}

func merge(
    first *NodeByte,
    second *NodeByte,
    parent *NodeByte,
    sIndex uint16,
    level uint32,
){
    childKey := parent.key(0)
    
    for i := uint16(0); i < second.nkeys(); i++ {
        first.addKV(
            first.nkeys(),
            second.klen(i),
            second.key(i),
            second.vlen(i),
            second.value(i),
        )
        first.setNkeys(first.nkeys()+1)
    }

    for i := sIndex-1; i < parent.nkeys()-1; i++ {
        //parent.keys[i] = parent.keys[i+1]
        //parent.children[i+1] = parent.children[i+2]
        parent.setKeyOffset(i,parent.keyOffset(i+1))
    }
    parent.setNkeys(parent.nkeys()-1)

    if(parent.isRoot() == false && parent.nkeys() < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        altRootByte = first
    }
}

func mergeInner(
    first *NodeByte,
    second *NodeByte,
    parent *NodeByte,
    sIndex uint16,
    level uint32,
){
    childKey := parent.key(0)
    //first.keys[first.nkeys] = parent.keys[sIndex-1]
    first.addKC(
        first.nkeys(),
        parent.klen(sIndex-1),
        parent.key(sIndex-1),
        second.cptr(0),
    )
    first.setNkeys(first.nkeys()+1)

    for i := uint16(0); i < second.nkeys(); i++ {
        //first.keys[first.nkeys] = second.keys[i]
        //first.children[first.nkeys] = second.children[i]
        first.addKC(
            first.nkeys(),
            second.klen(i),
            second.key(i),
            second.cptr(i+1),
        )
        first.setNkeys(first.nkeys()+1)
    }
    //first.children[first.nkeys] = second.children[second.nkeys]

    for i := sIndex-1; i < parent.nkeys()-1; i++ {
        //parent.keys[i] = parent.keys[i+1]
        //parent.children[i+1] = parent.children[i+2]
        parent.setKeyOffset(i, parent.keyOffset(i+1))
    }
    parent.setNkeys(parent.nkeys()-1)

    if(parent.isRoot() == false && parent.nkeys() < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        altRootByte = first
    }
}

func deleteInner(node *NodeByte, key []byte){
    var index uint16
    if(node.isLeaf() == true){
        return
    }
    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr > 0){
            deleteInner(node.children(index), key)
            return
        }
        if(cr == 0){
            break
        }
    }

    if(index == node.nkeys()){
        deleteInner(node.children(node.nkeys()), key)
        return
    }

    var newkey []byte
    temp := node.children(index+1)
    for{
        if(temp.isLeaf() == true){
            newkey = temp.key(0)
            break
        }
        temp = temp.children(0)
    }

    //node.keys[index] = newkey
    node.addKC(
        index,
        uint16(len(newkey)),
        newkey,
        node.cptr(index+1),
    )
    node.setKeyOffset(node.nkeys(), node.keyOffset(node.nkeys()+1))
}

func PrintTree(node *NodeByte, level int){
    fmt.Print(level, "  ")
    fmt.Println(node.data[:OFF_FKL], "\t",node.data[OFF_FKL:80])
    if(node.isLeaf() == true){
        return
    }

    for i := 0; i < int(node.nkeys()+1); i++{
        PrintTree(node.children(uint16(i)), level+1)
    }
}
