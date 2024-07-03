package storage

import (
	"fmt"
	"os"
    "syscall"
    "encoding/binary"
)

const (
    META_TYPE_MAIN = 1
    META_TYPE_IDX = 2

    MAIN_FL_OFF = 4096
    MAIN_ROOT_OFF = 8192

    IDX_FL_OFF = 4096
    IDX_FIRST_OFF = 8192
    IDX_META_FIRST_PAIR = 24
)

type KV struct{
    fp *os.File
    fSize uint64
    mSize uint64
    flushed uint64
    nNewPages uint64
    nReusedPages uint64
    data [][]byte
    allocPages []uint64

    rootByte *NodeByte      // root node of the b+tree
    altRootByte *NodeByte   // alternate root node for copy on write
    rootOffset uint64       // offset of root node in the main db file
    altRootOffset uint64    // alternate offset of root node for copy on write

    freeList *NodeFreeList
    pushList *NodeFreeList
    popList *NodeFreeList
}

func (kv *KV) Create(fileName string){
    self = kv
    kv.mapFile(fileName)
    kv.setMeta()
    createFreeList()
    createRoot()
}

func (kv *KV) Load(fileName string){
    self = kv
    kv.mapFile(fileName)
    kv.loadMeta()
}

func (kv *KV) loadMeta(){
    metaPage := kv.page(0)
    self.rootOffset = binary.LittleEndian.Uint64(metaPage[8:])
    kv.flushed = binary.LittleEndian.Uint64(metaPage[16:])

    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(self.rootOffset)
    self.rootByte.selfPtr = self.rootOffset

    kv.freeList = new(NodeFreeList)
    kv.freeList.data = kv.page(MAIN_FL_OFF)
}

func (kv *KV) setMeta(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_MAIN)
    binary.LittleEndian.PutUint64(metaPage[8:], MAIN_ROOT_OFF)
    binary.LittleEndian.PutUint64(metaPage[16:], 2)
    kv.flushed = 2
}

func (kv *KV) updateMeta(){
    metaPage := kv.page(0)
    self.rootOffset = self.altRootOffset
    self.rootByte = self.altRootByte
    binary.LittleEndian.PutUint64(metaPage[8:], self.rootOffset)
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
}

func (kv *KV) CreateIdx(fileName string, index map[string]uint64){
    self = kv
    selfIdx = index
    kv.mapFile(fileName)
    kv.setMetaIdx()
    createFreeList()
    createRootIdx()
}

func (kv *KV) LoadIdx(fileName string, index map[string]uint64){
    self = kv
    selfIdx = index
    kv.mapFile(fileName)
    kv.loadMetaIdx(index)
}

func (kv *KV) loadMetaIdx(index map[string]uint64){
    metaPage := kv.page(0)
    idxlen := binary.LittleEndian.Uint64(metaPage[8:])
    offset := IDX_META_FIRST_PAIR
    for i := 0; i < int(idxlen); i++ {
        clen := binary.LittleEndian.Uint16(metaPage[offset:])
        col := string(metaPage[offset+2 : offset+2+int(clen)])
        key := binary.LittleEndian.Uint64(metaPage[offset+2+int(clen):])
        index[col] = key
        offset += 2 + int(clen) + 8
    }
}

func (kv *KV) setMetaIdx(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_IDX)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(selfIdx)))
    binary.LittleEndian.PutUint64(metaPage[16:], 2)

    offset := IDX_META_FIRST_PAIR
    for col, rOff := range(selfIdx){
        clen := uint16(len(col))
        binary.LittleEndian.PutUint16(metaPage[offset:], clen)

        for i := 0; i < len(col); i++ {
            metaPage[offset+2+i] = col[i];
        }

        binary.LittleEndian.PutUint64(metaPage[offset+int(clen)+2:], rOff)
        offset += 2 + int(clen) + 8
    }

    kv.flushed = 2
}

func (kv *KV) updateMetaIdx(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(selfIdx)))
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
    
    offset := IDX_META_FIRST_PAIR
    for col, rOff := range(selfIdx){
        clen := len(col)
        offset += 2 + int(clen)
        
        if(rOff == self.rootOffset){
            self.rootByte = self.altRootByte
            self.rootOffset = self.altRootOffset
            selfIdx[col] = self.altRootOffset
            binary.LittleEndian.PutUint64(metaPage[offset:], self.altRootOffset)
            break
        }
        offset += 8
    }
}

func (kv *KV) mapFile(fileName string){
    var file *os.File
    var fileInfo os.FileInfo
    var fileChunk []byte
    var err error

    file,err = os.OpenFile(fileName, os.O_RDWR, 600)
    if(err != nil){
        fmt.Println("ERROR in mapFile os.OpenFile:", err)
        os.Exit(1)
    }
    kv.fp = file

    fileInfo,err = file.Stat()
    if(err != nil){
        fmt.Println("ERROR in mapFile file.Stat:", err)
        os.Exit(1)
    }

    if(fileInfo.Size()%TREE_PAGE_SIZE != 0){
        fmt.Println("file size not a multiple of page size")
        os.Exit(1)
    }

    kv.fSize = max(uint64(fileInfo.Size()), 2*TREE_PAGE_SIZE)
    kv.mSize = 2*kv.fSize

    fileChunk, err = syscall.Mmap(
        int(file.Fd()), 0, int(kv.mSize),
        syscall.PROT_READ | syscall.PROT_WRITE,
        syscall.MAP_SHARED,
    )
    if(err != nil){
        fmt.Println("ERROR in mapFile syscall.Mmap:", err)
        os.Exit(1)
    }

    err = syscall.Fallocate(
        int(kv.fp.Fd()), 0, 0,
        int64(kv.fSize),
    )
    if(err != nil){
        fmt.Println("ERROR in mapFile syscall.Fallocate:", err)
        os.Exit(1)
    }

    kv.data = append(kv.data, fileChunk)
}

func (kv *KV) extendMap() error {
    fileChunk,err := syscall.Mmap(
        int(kv.fp.Fd()), int64(kv.mSize), int(kv.mSize),
        syscall.PROT_READ | syscall.PROT_WRITE,
        syscall.MAP_SHARED,
    )
    if(err != nil){
        return err
    }

    kv.data = append(kv.data, fileChunk)
    kv.mSize += kv.mSize
    return nil
}

func (kv *KV) page(offset uint64) []byte {
    if(offset >= kv.fSize){
        return nil
    }

    start := uint64(0)
    for _, chunk := range kv.data{
        end := start + uint64(len(chunk))
        if(offset < end){
            off := offset - start
            return chunk[off: off+TREE_PAGE_SIZE]
        }
        start = end
    }
    return nil
}

func (kv *KV) newpage() ([]byte, uint64, error){

    var pageByte []byte
    var offset uint64

    if(kv.popList != nil && kv.popList.noffs() > 0){
        var err error
        offset, err = kv.popList.pop()
        kv.nReusedPages += 1
        if(err == nil){
            goto out
        }
    }

    kv.nNewPages += 1
    if((kv.flushed + kv.nNewPages)*TREE_PAGE_SIZE >= kv.fSize){
        allocSize := max(kv.fSize, (kv.nNewPages+kv.flushed)*TREE_PAGE_SIZE)
        if err := syscall.Fallocate(
            int(kv.fp.Fd()), 0, int64(kv.fSize),
            int64(allocSize),
        ); err != nil {

        }else{
            kv.fSize += allocSize
        }
    }
    
    if(kv.fSize >= kv.mSize){
        err := kv.extendMap()
        if(err != nil){
            return nil, 0, err
        }
    }

    offset = (kv.flushed + kv.nNewPages - 1)*TREE_PAGE_SIZE

out:
    pageByte = kv.page(offset)
    return pageByte, offset, nil
}

func (kv *KV) updateFreeList(){
    if(kv.popList == nil || kv.pushList == nil){
        return
    }

    kv.freeList.setNoffs(kv.popList.noffs())
    for{
        if(kv.pushList.noffs() == 0){
            break
        }
        offset, _ := kv.pushList.pop()
        kv.freeList.push(offset)
    }

    kv.popList.setNoffs(0)
    kv.pushList.setNoffs(0)
}

func (kv *KV) updateLeafLink(){
    for i := 0; i < len(kv.allocPages); i++ {
        node := new(NodeByte)
        node.data = kv.page(kv.allocPages[i])

        prev := node.prevNode()
        if(prev != nil){
            prev.setNext(kv.allocPages[i])
        }

        next := node.nextNode()
        if(next != nil){
            next.setPrev(kv.allocPages[i])
        }
    }
    kv.allocPages = kv.allocPages[:0]
}

func (kv *KV) flush(){
    if err := kv.fp.Sync(); err != nil {
        fmt.Println("ERROR in flush/Sync", err)
        os.Exit(1)
    }
    
    kv.updateLeafLink()
    
    metaPage := kv.page(0)
    if(binary.LittleEndian.Uint16(metaPage[:]) == META_TYPE_IDX){
        kv.updateMetaIdx()
    }else{
        kv.updateMeta()
    }

    kv.flushed += kv.nNewPages
    kv.nNewPages = 0

    kv.updateFreeList()

    if err := kv.fp.Sync(); err != nil {
        fmt.Println("ERROR in flush/Sync", err)
        os.Exit(1)
    }
}

func (kv *KV) Insert(key []byte, value []byte){
    self = kv
    kv.nNewPages = 0
    makeFreeListCopy()
    insertLeaf(self.rootByte, key, value, 0)
}

func (kv *KV) InsertIndex(colname string, key []byte, value []byte){
    self = kv
    kv.nNewPages = 0
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(selfIdx[colname])
    self.rootOffset = selfIdx[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    makeFreeListCopy()
    insertLeaf(self.rootByte, key, value, 0)
}

func (kv *KV) Delete(key []byte){
    self = kv
    kv.nNewPages = 0
    makeFreeListCopy()
    deleteLeaf(self.rootByte, key, 0)
}

func (kv *KV) DeleteIndex(colname string, key []byte){
    self = kv
    kv.nNewPages = 0
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(selfIdx[colname])
    self.rootOffset = selfIdx[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    makeFreeListCopy()
    deleteLeaf(self.rootByte, key, 0)
}

func (kv *KV) Update(key []byte, value []byte){
    self = kv
    kv.nNewPages = 0
    makeFreeListCopy()
    update(self.rootByte, key, value, 0)
}

func (kv *KV) UpdateIndex(colname string, key []byte, value []byte){
    self = kv
    kv.nNewPages = 0
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(selfIdx[colname])
    self.rootOffset = selfIdx[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    makeFreeListCopy()
    update(self.rootByte, key, value, 0)
}

func (kv *KV) Get(key []byte) []byte{
    self = kv
    makeFreeListCopy()
    return qget(self.rootByte, key, 0)
}

func (kv *KV) Range(keyStart []byte, keyEnd []byte) [][]byte {
    self = kv
    makeFreeListCopy()
    return qrange(self.rootByte, keyStart, keyEnd)
}

func (kv *KV) Print(){
    self = kv
    printTree(self.rootByte, 0)
}

func (kv *KV) PrintIndex(index map[string]uint64){
    for _, off := range(index){
        node := new(NodeByte)
        node.data = kv.page(off)
        printTree(node, 0)
    }
}
