package storage

import (
    "bytes"
    "encoding/binary"
    "fmt"
)

const TREE_PAGE_SIZE = 4096
const (
     TYPE_ROOT_I = 1
     TYPE_ROOT_L = 2
     TYPE_I = 3
     TYPE_L = 4
)

type Tree struct{
    kv *KeyValue
    MAX uint16
    MID uint16
    OFF_TYPE uint16
    OFF_NKEYS uint16
    OFF_MAP uint16
    OFF_FCP uint16
    OFF_FKEY uint16
}

type NodeByte struct{
    data []byte
    selfPtr uint64
    tree *Tree
}

type RangeIter struct{
    path []uint16
    children []uint16
    values [][]byte
}

func (tree *Tree) Init(MAX uint16){
    tree.MAX = MAX
    tree.MID = MAX/2
    tree.OFF_TYPE = 0
    tree.OFF_NKEYS = 2
    tree.OFF_MAP = 4
    tree.OFF_FCP = 4 + 2*(tree.MAX+1) + 8
    
    // + 16 in OFF_FKEY is unnecessary as
    // next and prev links are no longer supported
    // but removing it breaks everything
    tree.OFF_FKEY = 4 + 2*(tree.MAX+1) + 16
}

func (tree *Tree) getNodeByte() (*NodeByte, uint64){
    var node = new(NodeByte)
    var offset uint64
    var err error

    node.data, offset, err = tree.kv.newpage()
    if(err != nil){
        fmt.Println("cannot allocate new page")
        return nil, 0
    }
    node.selfPtr = offset
    node.tree = tree
    node.setKeyOffset(0,tree.OFF_FKEY)
    node.setNkeys(0)

    return node, offset
}

func (tree *Tree) createRoot(){
    fmt.Println("root created")
    tree.kv.rootByte, tree.kv.rootOffset = tree.getNodeByte()
    tree.kv.rootByte.setType(TYPE_ROOT_L)
    tree.kv.altRootOffset = tree.kv.rootOffset
    tree.kv.altRootByte = tree.kv.rootByte
    tree.kv.flush()
}

func (tree *Tree) createRootIdx(){
    for col := range(tree.kv.table.Index){
        fmt.Printf("index %v created\n", col)
        tree.kv.rootByte, tree.kv.rootOffset = tree.getNodeByte()
        tree.kv.rootByte.setType(TYPE_ROOT_L)
        tree.kv.altRootByte = tree.kv.rootByte
        tree.kv.altRootOffset = tree.kv.rootOffset
        tree.kv.table.Index[col] = tree.kv.rootOffset
    }
    tree.kv.flush()
}

func (tree *Tree) makeByteCopy(node *NodeByte) (*NodeByte, uint64){
    newByte, offset := tree.getNodeByte()
    if(node.isRoot() == true){
        tree.kv.altRootByte = newByte
        tree.kv.altRootOffset = offset
    }
    copy(newByte.data[:tree.OFF_NKEYS], node.data[:tree.OFF_NKEYS])
    copy(newByte.data[tree.OFF_FCP:tree.OFF_FCP+8], node.data[tree.OFF_FCP:tree.OFF_FCP+8])
    for i := uint16(0);  i < node.nkeys(); i++ {
        if(node.isLeaf() == true){
            newByte.addKV(
                i,
                node.klen(i),
                node.key(i),
                node.vlen(i),
                node.value(i),
            )
        }else{
            newByte.addKC(
                i,
                node.klen(i),
                node.key(i),
                node.cptr(i+1),
            )
        }
        newByte.setNkeys(i+1)
    }

    tree.kv.pushList.push(node.selfPtr)

    return newByte,offset
}

func (node *NodeByte) nodetype() uint16{
    return binary.LittleEndian.Uint16(node.data[node.tree.OFF_TYPE : node.tree.OFF_TYPE+2])
}

func (node *NodeByte) isLeaf() bool{
    if(node.nodetype() == TYPE_ROOT_L || node.nodetype() == TYPE_L){
        return true
    }
    return false
}

func (node *NodeByte) isRoot() bool{
    if(node.nodetype() == TYPE_ROOT_I || node.nodetype() == TYPE_ROOT_L){
        return true
    }
    return false
}

func (node *NodeByte) nkeys() uint16{
    return binary.LittleEndian.Uint16(node.data[node.tree.OFF_NKEYS : node.tree.OFF_NKEYS+2])
}

func (node *NodeByte) keyOffset(index uint16) uint16{
    return binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + 2*index:])
}

func (node *NodeByte) size() uint16{
    return binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + 2*node.nkeys():])
}

func (node *NodeByte) key(index uint16) []byte{
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + index*2:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    keyOffset += 4
    return node.data[keyOffset:keyOffset+klen]
}

func (node *NodeByte) klen(index uint16) uint16{
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + index*2:])
    return uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
}

func (node *NodeByte) value(index uint16) []byte{
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + index*2:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    vlen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset+2:]))
    vOffset := keyOffset + 4 + klen
    return node.data[vOffset:vOffset+vlen]
}

func (node *NodeByte) vlen(index uint16) uint16{
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + index*2:])
    return uint16(binary.LittleEndian.Uint16(node.data[keyOffset+2:]))
}

func (node *NodeByte) cptr(index uint16) uint64{
    if(index == 0){
        return binary.LittleEndian.Uint64(node.data[node.tree.OFF_FCP:])
    }
    index -= 1
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + 2*index:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    keyOffset += 4 + klen
    return binary.LittleEndian.Uint64(node.data[keyOffset:])
}

func (node *NodeByte) children(index uint16) *NodeByte{
    chOffset := node.cptr(index)
    chNode := new(NodeByte)
    chNode.data = node.tree.kv.page(chOffset)
    chNode.selfPtr = chOffset
    chNode.tree = node.tree
    return chNode
}

func (node *NodeByte) setType(TYPE uint16){
    binary.LittleEndian.PutUint16(node.data[node.tree.OFF_TYPE:], TYPE)
}

func (node *NodeByte) setNkeys(nkeys uint16){
    binary.LittleEndian.PutUint16(node.data[node.tree.OFF_NKEYS:], nkeys)
}

func (node *NodeByte) setKeyOffset(index uint16, offset uint16){
    binary.LittleEndian.PutUint16(node.data[node.tree.OFF_MAP + 2*index:], offset)
}

// Add key and value to the end of the kv pair segment
// and update the offset map
func (node *NodeByte) addKV(
    index uint16,
    klen uint16,
    key []byte,
    vlen uint16,
    value []byte,
){
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP+2*node.nkeys():])

    binary.LittleEndian.PutUint16(node.data[keyOffset:],klen)
    binary.LittleEndian.PutUint16(node.data[keyOffset+2:],vlen)
    for i := uint16(0); i < klen; i++ {
        node.data[keyOffset+i+4] = key[i]
    }
    for i := uint16(0); i < vlen; i++ {
        node.data[keyOffset+i+4+klen] = value[i]
    }

    node.setKeyOffset(index, keyOffset)
    node.setKeyOffset(node.nkeys()+1, keyOffset+4+klen+vlen)
}

// Add key and child to the end of the kc pair segment
// and update the offset map,
// the index = actual index of child - 1
func (node *NodeByte) addKC(
    index uint16,
    klen uint16,
    key []byte,
    cptr uint64,
){
    keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP+2*node.nkeys():])

    binary.LittleEndian.PutUint16(node.data[keyOffset:],klen)
    binary.LittleEndian.PutUint16(node.data[keyOffset+2:],8)
    for i := uint16(0); i < klen; i++ {
        node.data[keyOffset+i+4] = key[i]
    }
    binary.LittleEndian.PutUint64(node.data[keyOffset+4+klen:],cptr)

    node.setKeyOffset(index, keyOffset)
    node.setKeyOffset(node.nkeys()+1, keyOffset+4+klen+8)
}

func (node *NodeByte) setCptr(index uint16, ptr uint64){
    if(index == 0){
        binary.LittleEndian.PutUint64(node.data[node.tree.OFF_FCP:], ptr)
    }else{
        keyOffset := binary.LittleEndian.Uint16(node.data[node.tree.OFF_MAP + (index-1)*2:])
        klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
        keyOffset += 4 + klen
        binary.LittleEndian.PutUint64(node.data[keyOffset:], ptr)
    }
}

func (tree *Tree) findParent(
    node *NodeByte,
    key []byte,
    curLevel uint32,
    wLevel uint32,
) *NodeByte{

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
        return tree.findParent(node.children(index), key, curLevel+1, wLevel)
    }
}

func (tree *Tree) insertLeaf(node *NodeByte, key []byte, value []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node,_ = tree.makeByteCopy(node)
    }
    
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
            if(cr == 0){
                return
            }
        }
        newChild, offset := tree.makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        tree.insertLeaf(newChild, key, value, level+1)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr > 0){
            break
        }
        if(cr == 0){
            node.addKV(index, uint16(len(key)), key, uint16(len(value)), value)
            node.setKeyOffset(node.nkeys(), node.keyOffset(node.nkeys()+1))
            tree.kv.changeRoot();
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

    if(node.nkeys() >= tree.MAX){
        k, firstPtr, secondPtr := tree.splitLeaf(node)
        if(node.isRoot() == true){
            newRootByte, off := tree.getNodeByte()
            newRootByte.setType(TYPE_ROOT_I)
            tree.insertInner(newRootByte, k, firstPtr, secondPtr, level)
            tree.kv.altRootByte = newRootByte
            tree.kv.altRootOffset = off
        }else{
            parent := tree.findParent(tree.kv.altRootByte, key, 0, level-1)
            tree.insertInner(parent, k, firstPtr, secondPtr, level-1)
        }
    }

    tree.kv.changeRoot();
}

func (tree *Tree) update(node *NodeByte, key []byte, value []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node,_ = tree.makeByteCopy(node)
    }
    
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
        }
        newChild, offset := tree.makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        tree.update(newChild, key, value, level+1)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr == 0){
            node.addKV(index, uint16(len(key)), key, uint16(len(value)), value)
            node.setKeyOffset(node.nkeys(), node.keyOffset(node.nkeys()+1))
            break
        }
    }
    tree.kv.changeRoot();
}

func (tree *Tree) insertInner(
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

    if(node.nkeys() >= tree.MAX){
        k, firstPtr, secondPtr := tree.splitInner(node)
        if(node.isRoot() == true){
            newRootByte, off := tree.getNodeByte()
            newRootByte.setType(TYPE_ROOT_I)
            tree.insertInner(newRootByte, k, firstPtr, secondPtr, pLevel)
            tree.kv.altRootByte = newRootByte
            tree.kv.altRootOffset = off
        }else{
            parent := tree.findParent(tree.kv.altRootByte, key, 0, pLevel-1)
            tree.insertInner(parent, k, firstPtr, secondPtr, pLevel-1)
        }
    }
}

func (tree *Tree) splitInner(node *NodeByte) ([]byte, uint64, uint64){
    first, firstPtr := tree.getNodeByte()
    second, secondPtr := tree.getNodeByte()

    first.setType(TYPE_I)
    second.setType(TYPE_I)

    for i := uint16(0); i < tree.MID; i++ {
        first.addKC(
            uint16(i),
            uint16(len(node.key(i))),
            node.key(i),
            node.cptr(i+1),
        )
        first.setNkeys(first.nkeys()+1)
    }
    first.setCptr(0, node.cptr(0))

    for i := tree.MID+1; i < tree.MAX; i++ {
        second.addKC(
            uint16(i-tree.MID-1),
            uint16(len(node.key(uint16(i)))),
            node.key(uint16(i)),
            node.cptr(uint16(i+1)),
        )
        second.setNkeys(second.nkeys()+1)
    }
    second.setCptr(0, node.cptr(tree.MID+1))

    return node.key(tree.MID), firstPtr, secondPtr
}

func (tree *Tree) splitLeaf(node *NodeByte) ([]byte, uint64, uint64){
    first, firstPtr := tree.getNodeByte()
    second, secondPtr := tree.getNodeByte()

    first.setType(TYPE_L)
    second.setType(TYPE_L)

    var i uint16
    for i = 0; i < tree.MID; i++ {
        first.addKV(
            uint16(i),
            uint16(len(node.key(i))),
            node.key(i),
            uint16(len(node.value(i))),
            node.value(i),
        )
        first.setNkeys(first.nkeys()+1)
    }

    for i = tree.MID; i < tree.MAX; i++ {
        second.addKV(
            i-tree.MID,
            uint16(len(node.key(i))),
            node.key(i),
            uint16(len(node.value(i))),
            node.value(i),
        )
        second.setNkeys(second.nkeys()+1)
    }

    return node.key(tree.MID), firstPtr, secondPtr
}

func (tree *Tree) deleteLeaf(node *NodeByte, key []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node, _ = tree.makeByteCopy(node)
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

        newChild, offset := tree.makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        tree.deleteLeaf(newChild,key,level+1)

        if(isInner == true){
            tree.deleteInner(tree.kv.altRootByte,key)
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

    if(node.nkeys() < tree.MID){
        if(node.isRoot() == true){
            tree.kv.changeRoot()
            return
        }
        isLeaf := true
        tree.fill(node, childKey, isLeaf, level)
    }

    tree.kv.changeRoot()
}

func (tree *Tree) fill(child *NodeByte, childKey []byte, isLeaf bool, level uint32){

    var cIndex uint16
    var ocIndex uint16
    parent := tree.findParent(tree.kv.altRootByte, childKey, 0, level-1)

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

    otherChild, ocOffset := tree.makeByteCopy(parent.children(ocIndex))
    parent.setCptr(ocIndex, ocOffset)

    if(otherChild.nkeys() == tree.MID){
        var passIndex uint16
        if(cIndex < ocIndex){
            passIndex = ocIndex
        }else{
            passIndex = cIndex
            child, otherChild = otherChild, child
        }
            
        if(isLeaf == true){
            tree.mergeLeaf(child,otherChild,parent,passIndex,level)
        }else{
            tree.mergeInner(child,otherChild,parent,passIndex,level)
        }
        return
    }

    if(isLeaf == true){
        if(ocIndex > cIndex){
            child.addKV(
                child.nkeys(),
                otherChild.klen(0),
                otherChild.key(0),
                otherChild.vlen(0),
                otherChild.value(0),
            )
            child.setNkeys(child.nkeys()+1)
            
            tree.deleteLeaf(otherChild, otherChild.key(0), 0)
            parent.addKC(
                cIndex,
                otherChild.klen(0),
                otherChild.key(0),
                parent.cptr(cIndex+1),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))
        }else{
            tree.insertLeaf(
                child,
                otherChild.key(otherChild.nkeys()-1),
                otherChild.value(otherChild.nkeys()-1),
                0,
            )
            tree.deleteLeaf(otherChild, otherChild.key(otherChild.nkeys()-1), 0)
            parent.addKC(
                cIndex-1,
                child.klen(0),
                child.key(0),
                parent.cptr(cIndex),
            )
        }
    }else{
        if(ocIndex > cIndex){

            child.addKC(
                child.nkeys(),
                parent.klen(cIndex),
                parent.key(cIndex),
                otherChild.cptr(0),
            )
            child.setNkeys(child.nkeys()+1)
            
            parent.addKC(
                cIndex,
                otherChild.klen(0),
                otherChild.key(0),
                parent.cptr(cIndex+1),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))

            otherChild.setCptr(0, otherChild.cptr(1))
            for i := uint16(0); i < otherChild.nkeys()-1; i++ {
                otherChild.setKeyOffset(i, otherChild.keyOffset(i+1))
            }
            otherChild.setNkeys(otherChild.nkeys()-1)

        }else{

            child.addKC(
                child.nkeys(),
                parent.klen(parent.nkeys()-1),
                parent.key(parent.nkeys()-1),
                otherChild.cptr(otherChild.nkeys()),
            )
            child.setNkeys(child.nkeys()+1)

            parent.addKC(
                parent.nkeys()-1,
                otherChild.klen(otherChild.nkeys()-1),
                otherChild.key(otherChild.nkeys()-1),
                parent.cptr(parent.nkeys()),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))
            
            for i := child.nkeys()-1; i > 0; i-- {
                off := child.keyOffset(i)
                child.setKeyOffset(i, child.keyOffset(i-1))
                child.setKeyOffset(i-1, off)
            }
            off := child.keyOffset(0)
            child.setKeyOffset(0, child.keyOffset(1))
            child.setKeyOffset(1, off)

            otherChild.setNkeys(otherChild.nkeys()-1)
        }
    }
}

func (tree *Tree) mergeLeaf(
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
        parent.setKeyOffset(i,parent.keyOffset(i+1))
    }
    parent.setNkeys(parent.nkeys()-1)

    if(parent.isRoot() == false && parent.nkeys() < tree.MID){
        isLeaf := false
        tree.fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        first.setType(TYPE_ROOT_L)
        tree.kv.altRootByte = first
    }
}

func (tree *Tree) mergeInner(
    first *NodeByte,
    second *NodeByte,
    parent *NodeByte,
    sIndex uint16,
    level uint32,
){
    childKey := parent.key(0)
    first.addKC(
        first.nkeys(),
        parent.klen(sIndex-1),
        parent.key(sIndex-1),
        second.cptr(0),
    )
    first.setNkeys(first.nkeys()+1)

    for i := uint16(0); i < second.nkeys(); i++ {
        first.addKC(
            first.nkeys(),
            second.klen(i),
            second.key(i),
            second.cptr(i+1),
        )
        first.setNkeys(first.nkeys()+1)
    }

    for i := sIndex-1; i < parent.nkeys()-1; i++ {
        parent.setKeyOffset(i, parent.keyOffset(i+1))
    }
    parent.setNkeys(parent.nkeys()-1)

    if(parent.isRoot() == false && parent.nkeys() < tree.MID){
        isLeaf := false
        tree.fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        first.setType(TYPE_ROOT_I)
        tree.kv.altRootByte = first
    }
}

func (tree *Tree) deleteInner(node *NodeByte, key []byte){
    var index uint16
    if(node.isLeaf() == true){
        return
    }
    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr > 0){
            tree.deleteInner(node.children(index), key)
            return
        }
        if(cr == 0){
            break
        }
    }

    if(index == node.nkeys()){
        tree.deleteInner(node.children(node.nkeys()), key)
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

    node.addKC(
        index,
        uint16(len(newkey)),
        newkey,
        node.cptr(index+1),
    )
    node.setKeyOffset(node.nkeys(), node.keyOffset(node.nkeys()+1))
}

func (tree *Tree) qget(node *NodeByte, key []byte) []byte{
    var index uint16
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
        }
        return tree.qget(node.children(index), key)
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr == 0){
            return node.value(index)
        }
    }

    return nil
}

func (tree *Tree) qgetIdx(node *NodeByte, key []byte, pkey [][]byte) [][]byte{
    var index uint16
    if(node.isLeaf() == false){
        isEqual := false
        for index = 0; index < node.nkeys(); index++ {
            klen := node.klen(index)
            actualkey := node.key(index)
            secLen := binary.LittleEndian.Uint16(actualkey[klen-2:])
            
            cr := bytes.Compare(actualkey[:secLen], key)
            if(cr == 0){
                pkey = tree.qgetIdx(node.children(index+1), key, pkey)
                isEqual = true
            }
            if(cr > 0){
                break
            }
        }
        if(isEqual == false){
            pkey = tree.qgetIdx(node.children(index), key, pkey)
        }
        return pkey
    }

    for index = 0; index < node.nkeys(); index++ {
        klen := node.klen(index)
        actualkey := node.key(index)
        secLen := binary.LittleEndian.Uint16(actualkey[klen-2:])
        
        cr := bytes.Compare(actualkey[:secLen], key)
        if(cr == 0){
            pkey = append(pkey, node.key(index))
        }
        if(cr > 0){
            break
        }
    }
    return pkey
}

func (tree *Tree) qrange(
    node *NodeByte, keyStart []byte,
    keyEnd []byte, it *RangeIter,
){

    var index uint16
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), keyStart)
            if(cr > 0){
                break
            }
        }
        it.path = append(it.path, index)
        it.children = append(it.children, node.nkeys())
        tree.qrange(node.children(index), keyStart, keyEnd, it)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), keyStart)
        if(cr >= 0){
            break;
        }
    }

    isComplete := tree.travInNode(node, index, keyEnd, it)
    if(isComplete == true){
        return
    }

    tree.iterate(keyEnd, it, false)
}

func (tree *Tree) qrangeIdx(
    node *NodeByte, keyStart []byte,
    keyEnd []byte, it *RangeIter,
){
    
    var index uint16
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            klen := node.klen(index)
            actualKey := node.key(index)
            secLen := binary.LittleEndian.Uint16(actualKey[klen-2:])

            cr := bytes.Compare(actualKey[:secLen], keyStart)
            if(cr > 0){
                break
            }
        }
        it.path = append(it.path, index)
        it.children = append(it.children, node.nkeys())
        tree.qrangeIdx(node.children(index), keyStart, keyEnd, it)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        klen := node.klen(index)
        actualKey := node.key(index)
        secLen := binary.LittleEndian.Uint16(actualKey[klen-2:])

        cr := bytes.Compare(actualKey[:secLen], keyStart)
        if(cr >= 0){
            break;
        }
    }

    isComplete := tree.travInNodeIdx(node, index, keyEnd, it)
    if(isComplete == true){
        return
    }

    tree.iterate(keyEnd, it, true)
}

func (tree *Tree) iterate(keyEnd []byte, it *RangeIter, isIndex bool){
    var numLast = 0
    for i := len(it.path)-1; i >= 0; i-- {
        if(it.path[i] < it.children[i]){
            it.path[i] += 1;
            break;
        }else{
            it.path[i] = 0;
            numLast += 1
        }
    }

    if(numLast == len(it.path)){
        return
    }

    node := tree.kv.rootByte
    for i := range(it.path){
        it.children[i] = node.nkeys()
        node = node.children(uint16(it.path[i]));
    }

    var isComplete bool
    if(isIndex == false){
        isComplete = tree.travInNode(node, 0, keyEnd, it)
    }else{
        isComplete = tree.travInNodeIdx(node, 0, keyEnd, it)
    }
    if(isComplete == true){
        return
    }

    tree.iterate(keyEnd, it, isIndex)
}

func (tree *Tree) travInNode(
    node *NodeByte, index uint16,
    keyEnd []byte, it *RangeIter,
) bool {

    var i uint16
    for i := index; i < node.nkeys(); i++ {
        key := node.key(i)
        cr := bytes.Compare(key, keyEnd)
        if(cr > 0){
            break;
        }

        combined := make([]byte, 2)
        binary.LittleEndian.PutUint16(combined[:], node.klen(i))
        combined = append(combined, node.key(i)...)
        combined = append(combined, node.value(i)...)
        it.values = append(it.values, combined)
    }
    return  i == node.nkeys()
}

func (tree *Tree) travInNodeIdx(
    node *NodeByte, index uint16,
    keyEnd []byte, it *RangeIter,
) bool {

    var i uint16
    for i := index; i < node.nkeys(); i++ {
        klen := node.klen(i)
        actualKey := node.key(i)
        secLen := binary.LittleEndian.Uint16(actualKey[klen-2:])

        cr := bytes.Compare(actualKey[:secLen], keyEnd)
        if(cr > 0){
            break;
        }

        it.values = append(it.values, node.key(i))
    }
    return  i == node.nkeys()
}

func (tree *Tree) printTree(node *NodeByte, level int){

    fmt.Print(level, "  ")
    if(node.isLeaf()){
        fmt.Print("Leaf   ", node.nkeys(), " ")
        for i := uint16(0); i < node.nkeys(); i++ {
            fmt.Print(node.key(i)," ")
        }
        fmt.Print("  ")
        for i := uint16(0); i < node.nkeys(); i++ {
            fmt.Print(node.value(i)," ")
        }
    }else{
        fmt.Print("Inner  ", node.nkeys(), " ")
        for i := uint16(0); i < node.nkeys(); i++ {
            fmt.Print(node.key(i)," ")
        }
        fmt.Print("  ")
        for i := uint16(0); i <= node.nkeys(); i++ {
            fmt.Print(node.cptr(i)," ")
        }
    }
    fmt.Println()
    
    if(node.isLeaf() == true){
        return
    }

    for i := 0; i < int(node.nkeys()+1); i++{
        node.tree = tree
        tree.printTree(node.children(uint16(i)), level+1)
    }
}
