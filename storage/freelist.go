package storage

import (
	"encoding/binary"
	"fmt"
)

const (
    FL_OFF_PAGENO = 0
    FL_OFF_NOFFS = 8
    FL_OFF_TOTAL = 16
    FL_OFF_NEXT = 24
    FL_START = 32
    MAX_PER_PAGE = 508
)

type NodeFreeList struct{
    data []byte
}

var freeList *NodeFreeList = nil
var pushList *NodeFreeList = nil
var popList *NodeFreeList = nil

func createFreeList(){
    freeList = new(NodeFreeList)
    freeList.data = self.page(FL_PAGE_OFF)

    freeList.setPageno(1)
    freeList.setNoffs(0)
    freeList.setTotal(0)
}

func makeFreeListCopy(){
    popList = new(NodeFreeList)
    pushList = new(NodeFreeList)
    popList.data = make([]byte, TREE_PAGE_SIZE)
    pushList.data = make([]byte, TREE_PAGE_SIZE)
    copy(popList.data[:TREE_PAGE_SIZE], freeList.data[:TREE_PAGE_SIZE])
}

func (node *NodeFreeList) pageno() uint64{
    return binary.LittleEndian.Uint64(node.data[FL_OFF_PAGENO:])
}

func (node *NodeFreeList) setPageno(pageno uint64){
    binary.LittleEndian.PutUint64(node.data[FL_OFF_PAGENO:], pageno)
}

func (node *NodeFreeList) noffs() uint64{
    return binary.LittleEndian.Uint64(node.data[FL_OFF_NOFFS:])
}

func (node *NodeFreeList) setNoffs(n uint64){
    binary.LittleEndian.PutUint64(node.data[FL_OFF_NOFFS:], n)
}

func (node *NodeFreeList) total() uint64{
    return binary.LittleEndian.Uint64(node.data[FL_OFF_TOTAL:])
}

func (node *NodeFreeList) setTotal(total uint64){
    binary.LittleEndian.PutUint64(node.data[FL_OFF_TOTAL:], total)
}

func (node *NodeFreeList) pop() (uint64, error) {
    if(node.noffs() != 0){
        node.setNoffs(node.noffs()-1)
        return binary.LittleEndian.Uint64(node.data[FL_START+8*node.noffs():]), nil
    }
    return 0, fmt.Errorf("Freelist is empty")
}

func (node *NodeFreeList) push(offset uint64) error {
    if(node.noffs() >= MAX_PER_PAGE){
        return fmt.Errorf("Not enough space in Freelist")
    }
    binary.LittleEndian.PutUint64(node.data[FL_START+8*node.noffs():], offset)
    node.setNoffs(node.noffs()+1)
    return nil
}