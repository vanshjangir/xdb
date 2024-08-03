package storage

import (
    "bytes"
    "encoding/binary"
    "fmt"
)

const M = 3
const MID = (M/2)
const TREE_PAGE_SIZE = 4096
const (
     TYPE_ROOT_I = 1
     TYPE_ROOT_L = 2
     TYPE_I = 3
     TYPE_L = 4
)

const (
    OFF_TYPE = 0
    OFF_NKEYS = 2
    OFF_MAP = 4
    OFF_NEXT = 4 + 2*(M+1)
    OFF_PREV = 4 + 2*(M+1) + 8
    OFF_FCP = 4 + 2*(M+1) + 8
    OFF_FKEY = 4 + 2*(M+1) + 16
)

type NodeByte struct{
    data []byte
    selfPtr uint64
}

type RangeIter struct{
    path []uint16
    children []uint16
    values [][]byte
}

var self *KV

func getNodeByte() (*NodeByte, uint64){
    var node = new(NodeByte)
    var offset uint64
    var err error

    node.data, offset, err = self.newpage()
    if(err != nil){
        fmt.Println("cannot allocate new page")
        return nil, 0
    }
    node.selfPtr = offset
    node.setKeyOffset(0,OFF_FKEY)
    node.setNkeys(0)

    return node, offset
}

func createRoot(){
    fmt.Println("root created")
    self.rootByte, self.rootOffset = getNodeByte()
    self.rootByte.setType(TYPE_ROOT_L)
    self.altRootOffset = self.rootOffset
    self.altRootByte = self.rootByte
    self.flush()
}

func createRootIdx(index map[string]uint64){
    for col := range(index){
        fmt.Printf("index %v created\n", col)
        self.rootByte, self.rootOffset = getNodeByte()
        self.rootByte.setType(TYPE_ROOT_L)
        self.altRootByte = self.rootByte
        self.altRootOffset = self.rootOffset
        index[col] = self.rootOffset
    }
    self.flush()
}

func makeByteCopy(node *NodeByte) (*NodeByte, uint64){
    newByte, offset := getNodeByte()
    if(node.isRoot() == true){
        self.altRootByte = newByte
        self.altRootOffset = offset
    }
    copy(newByte.data[:OFF_NKEYS], node.data[:OFF_NKEYS])
    copy(newByte.data[OFF_FCP:OFF_FCP+8], node.data[OFF_FCP:OFF_FCP+8])
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

    self.pushList.push(node.selfPtr)

    return newByte,offset
}

func (node *NodeByte) nodetype() uint16{
    return binary.LittleEndian.Uint16(node.data[OFF_TYPE : OFF_TYPE+2])
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
    return binary.LittleEndian.Uint16(node.data[OFF_NKEYS : OFF_NKEYS+2])
}

func (node *NodeByte) keyOffset(index uint16) uint16{
    return binary.LittleEndian.Uint16(node.data[OFF_MAP + 2*index:])
}

func (node *NodeByte) size() uint16{
    return binary.LittleEndian.Uint16(node.data[OFF_MAP + 2*node.nkeys():])
}

func (node *NodeByte) key(index uint16) []byte{
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + index*2:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    keyOffset += 4
    return node.data[keyOffset:keyOffset+klen]
}

func (node *NodeByte) klen(index uint16) uint16{
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + index*2:])
    return uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
}

func (node *NodeByte) value(index uint16) []byte{
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + index*2:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    vlen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset+2:]))
    vOffset := keyOffset + 4 + klen
    return node.data[vOffset:vOffset+vlen]
}

func (node *NodeByte) vlen(index uint16) uint16{
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + index*2:])
    return uint16(binary.LittleEndian.Uint16(node.data[keyOffset+2:]))
}

func (node *NodeByte) cptr(index uint16) uint64{
    if(index == 0){
        return binary.LittleEndian.Uint64(node.data[OFF_FCP:])
    }
    index -= 1
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + 2*index:])
    klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
    keyOffset += 4 + klen
    return binary.LittleEndian.Uint64(node.data[keyOffset:])
}

func (node *NodeByte) children(index uint16) *NodeByte{
    chOffset := node.cptr(index)
    chNode := new(NodeByte)
    chNode.data = self.page(chOffset)
    chNode.selfPtr = chOffset
    return chNode
}

func (node *NodeByte) setType(TYPE uint16){
    binary.LittleEndian.PutUint16(node.data[OFF_TYPE:], TYPE)
}

func (node *NodeByte) setNkeys(nkeys uint16){
    binary.LittleEndian.PutUint16(node.data[OFF_NKEYS:], nkeys)
}

func (node *NodeByte) setKeyOffset(index uint16, offset uint16){
    binary.LittleEndian.PutUint16(node.data[OFF_MAP + 2*index:], offset)
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
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP+2*node.nkeys():])

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
    keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP+2*node.nkeys():])

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
        binary.LittleEndian.PutUint64(node.data[OFF_FCP:], ptr)
    }else{
        keyOffset := binary.LittleEndian.Uint16(node.data[OFF_MAP + (index-1)*2:])
        klen := uint16(binary.LittleEndian.Uint16(node.data[keyOffset:]))
        keyOffset += 4 + klen
        binary.LittleEndian.PutUint64(node.data[keyOffset:], ptr)
    }
}

func findParent(
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
        return findParent(node.children(index), key, curLevel+1, wLevel)
    }
}

func insertLeaf(node *NodeByte, key []byte, value []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node,_ = makeByteCopy(node)
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
        newChild, offset := makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        insertLeaf(newChild, key, value, level+1)
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
            self.changeRoot();
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
            self.altRootByte = newRootByte
            self.altRootOffset = off
        }else{
            parent := findParent(self.altRootByte, key, 0, level-1)
            insertInner(parent, k, firstPtr, secondPtr, level-1)
        }
    }

    self.changeRoot();
}

func update(node *NodeByte, key []byte, value []byte, level uint32){

    var index uint16
    if(node.isRoot() == true){
        node,_ = makeByteCopy(node)
    }
    
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
        }
        newChild, offset := makeByteCopy(node.children(index))
        node.setCptr(index, offset)
        update(newChild, key, value, level+1)
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
    self.changeRoot();
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
            self.altRootByte = newRootByte
            self.altRootOffset = off
        }else{
            parent := findParent(self.altRootByte, key, 0, pLevel-1)
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

    return node.key(MID), firstPtr, secondPtr
}

func deleteLeaf(node *NodeByte, key []byte, level uint32){

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
        deleteLeaf(newChild,key,level+1)

        if(isInner == true){
            deleteInner(self.altRootByte,key)
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

    self.changeRoot()
}

func fill(child *NodeByte, childKey []byte, isLeaf bool, level uint32){

    var cIndex uint16
    var ocIndex uint16
    parent := findParent(self.altRootByte, childKey, 0, level-1)

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
            child.addKV(
                child.nkeys(),
                otherChild.klen(0),
                otherChild.key(0),
                otherChild.vlen(0),
                otherChild.value(0),
            )
            child.setNkeys(child.nkeys()+1)
            
            deleteLeaf(otherChild, otherChild.key(0), 0)
            parent.addKC(
                cIndex,
                otherChild.klen(0),
                otherChild.key(0),
                parent.cptr(cIndex+1),
            )
            parent.setKeyOffset(parent.nkeys(), parent.keyOffset(parent.nkeys()+1))
        }else{
            insertLeaf(
                child,
                otherChild.key(otherChild.nkeys()-1),
                otherChild.value(otherChild.nkeys()-1),
                0,
            )
            deleteLeaf(otherChild, otherChild.key(otherChild.nkeys()-1), 0)
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
            
            for i := child.nkeys()-1; i > 0; i++ {
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
        parent.setKeyOffset(i,parent.keyOffset(i+1))
    }
    parent.setNkeys(parent.nkeys()-1)

    if(parent.isRoot() == false && parent.nkeys() < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        self.altRootByte = first
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

    if(parent.isRoot() == false && parent.nkeys() < MID){
        isLeaf := false
        fill(parent, childKey, isLeaf, level-1)
    }
    if(parent.isRoot() == true && parent.nkeys() == 0){
        self.altRootByte = first
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

    node.addKC(
        index,
        uint16(len(newkey)),
        newkey,
        node.cptr(index+1),
    )
    node.setKeyOffset(node.nkeys(), node.keyOffset(node.nkeys()+1))
}

func qget(node *NodeByte, key []byte) []byte{
    var index uint16
    if(node.isLeaf() == false){
        for index = 0; index < node.nkeys(); index++ {
            cr := bytes.Compare(node.key(index), key)
            if(cr > 0){
                break
            }
        }
        return qget(node.children(index), key)
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), key)
        if(cr == 0){
            return node.value(index)
        }
    }

    return nil
}

func qgetIdx(node *NodeByte, key []byte, pkey [][]byte) [][]byte{
    var index uint16
    if(node.isLeaf() == false){
        isEqual := false
        for index = 0; index < node.nkeys(); index++ {
            klen := node.klen(index)
            actualkey := node.key(index)
            secLen := binary.LittleEndian.Uint16(actualkey[klen-2:])
            
            cr := bytes.Compare(actualkey[:secLen], key)
            if(cr == 0){
                pkey = qgetIdx(node.children(index+1), key, pkey)
                isEqual = true
            }
            if(cr > 0){
                break
            }
        }
        if(isEqual == false){
            pkey = qgetIdx(node.children(index), key, pkey)
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

func qrange(
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
        qrange(node.children(index), keyStart, keyEnd, it)
        return
    }

    for index = 0; index < node.nkeys(); index++ {
        cr := bytes.Compare(node.key(index), keyStart)
        if(cr >= 0){
            break;
        }
    }

    isComplete := travInNode(node, index, keyEnd, it)
    if(isComplete == true){
        return
    }

    iterate(keyEnd, it, false)
}

func qrangeIdx(
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
        qrangeIdx(node.children(index), keyStart, keyEnd, it)
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

    isComplete := travInNodeIdx(node, index, keyEnd, it)
    if(isComplete == true){
        return
    }

    iterate(keyEnd, it, true)
}

func iterate(keyEnd []byte, it *RangeIter, isIndex bool){
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

    node := self.rootByte
    for i := range(it.path){
        it.children[i] = node.nkeys()
        node = node.children(uint16(it.path[i]));
    }

    var isComplete bool
    if(isIndex == false){
        isComplete = travInNode(node, 0, keyEnd, it)
    }else{
        isComplete = travInNodeIdx(node, 0, keyEnd, it)
    }
    if(isComplete == true){
        return
    }

    iterate(keyEnd, it, isIndex)
}

func travInNode(
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

        it.values = append(it.values, node.value(i))
    }
    return  i == node.nkeys()
}

func travInNodeIdx(
    node *NodeByte, index uint16,
    keyEnd []byte, it *RangeIter,
) bool {

    var i uint16
    for i := index; i < node.nkeys(); i++ {
        klen := node.klen(index)
        actualKey := node.key(index)
        secLen := binary.LittleEndian.Uint16(actualKey[klen-2:])

        cr := bytes.Compare(actualKey[:secLen], keyEnd)
        if(cr > 0){
            break;
        }

        it.values = append(it.values, node.key(i))
    }
    return  i == node.nkeys()
}

func printTree(node *NodeByte, level int){

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
            fmt.Print(node.cptr(i+1)," ")
        }
    }
    fmt.Println()
    
    if(node.isLeaf() == true){
        return
    }

    for i := 0; i < int(node.nkeys()+1); i++{
        printTree(node.children(uint16(i)), level+1)
    }
}
