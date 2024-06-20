package store

import (
	"encoding/binary"
	"fmt"
)

var FILE [100000]byte
var curOffset uint64 = 100

const (
     TYPE_ROOT_I = 1
     TYPE_ROOT_L = 2
     TYPE_I = 3
     TYPE_L = 4
)

const (
    OFF_SIZE = 0
    OFF_TYPE = 8
    OFF_NKEYS = 10
    OFF_MAP = 12
    OFF_NEXT = 12 + 2*(M+1)
    OFF_FCP = 12 + 2*(M+1)
    OFF_FKL = 12 + 2*(M+1) + 8
    OFF_FKI = 12 + 2*(M+1) + 8
)

type NodeByte struct{
    data []byte
    selfPtr uint64
}

var RootByte *NodeByte
var altRootByte *NodeByte
var RootOffset uint64 = 0
var altRootOffset uint64 = 0

func getNodeByte() (*NodeByte, uint64){
    var node = new(NodeByte)
    
    node.data = FILE[curOffset:]
    node.selfPtr = curOffset
    node.setKeyOffset(0,OFF_FKL)
    node.setSize(12 + 2*(M+1) + 8)
    node.setNkeys(0)
    
    off := curOffset
    curOffset += 100
    return node, off
}

func tempNode(offset uint16, size uint64) (*NodeByte){
    var node = new(NodeByte)
    node.data = FILE[offset:]
    return node
}

func CreateByte(){
    fmt.Println("root created")
    RootByte, RootOffset = getNodeByte()
    RootByte.setType(TYPE_ROOT_L)
}

func makeByteCopy(node *NodeByte) (*NodeByte, uint64){
    newByte, offset := getNodeByte()
    if(node.isRoot() == true){
        altRootByte = newByte
        altRootOffset = offset
    }
    copy(newByte.data[:node.size()], node.data[:node.size()])
    return newByte,offset
}

func changeRootByte(){
    RootByte = altRootByte
    RootOffset = altRootOffset
    binary.LittleEndian.PutUint64(FILE[:], RootOffset)
}

func (node *NodeByte) size() uint64{
    return binary.LittleEndian.Uint64(node.data[:])
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
    chNode,_ := getNodeByte()
    chNode.data = FILE[chOffset:]
    return chNode
}

func (node *NodeByte) setSize(size uint64){
    binary.LittleEndian.PutUint64(node.data[:], size)
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
    node.setSize(node.size()+4+uint64(vlen)+uint64(klen))
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
    node.setSize(node.size()+4+8+uint64(klen))
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

func (node *NodeByte) setNext(offptr uint64){
    binary.LittleEndian.PutUint64(node.data[OFF_NEXT:], offptr)
}
