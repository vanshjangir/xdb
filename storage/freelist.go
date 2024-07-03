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

func createFreeList(){
    self.freeList = new(NodeFreeList)
    self.freeList.data = self.page(MAIN_FL_OFF)

    self.freeList.setPageno(1)
    self.freeList.setNoffs(0)
    self.freeList.setTotal(0)
}

func makeFreeListCopy(){
    self.popList = new(NodeFreeList)
    self.pushList = new(NodeFreeList)
    self.popList.data = make([]byte, TREE_PAGE_SIZE)
    self.pushList.data = make([]byte, TREE_PAGE_SIZE)
    copy(self.popList.data[:TREE_PAGE_SIZE], self.freeList.data[:TREE_PAGE_SIZE])
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
