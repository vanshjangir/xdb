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

    PREV_LINK = 1
    NEXT_LINK = 2
)

type Transaction struct{
    rootOffset uint64
    index map[string]uint64
    oldLeafLinks map[uint64][2]uint64
    afterAllocPages []uint64
}

type KV struct{
    fp *os.File
    fSize uint64
    mSize uint64
    flushed uint64
    nNewPages uint64
    data [][]byte
    uLeafPages []uint64

    rootByte *NodeByte      // root node of the b+tree
    altRootByte *NodeByte   // alternate root node for copy on write
    rootOffset uint64       // offset of root node in the main db file
    altRootOffset uint64    // alternate offset of root node for copy on write

    freeList *NodeFreeList
    pushList *NodeFreeList
    popList *NodeFreeList

    tx Transaction
}

func (kv *KV) Create(fileName string){
    self = kv
    if(kv.tx.index == nil){
        kv.tx.index = make(map[string]uint64)
    }
    if(kv.tx.oldLeafLinks == nil){
        kv.tx.oldLeafLinks = make(map[uint64][2]uint64)
    }
    kv.mapFile(fileName)
    kv.setMeta()
    createFreeList()
    createRoot()
}

func (kv *KV) Load(fileName string){
    self = kv
    kv.mapFile(fileName)
    kv.loadMeta()
    kv.tx.rootOffset = kv.rootOffset
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
    kv.tx.rootOffset = self.rootOffset
    binary.LittleEndian.PutUint64(metaPage[8:], kv.tx.rootOffset)
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
}

func (kv *KV) CreateIdx(fileName string, index map[string]uint64){
    self = kv
    if(kv.tx.index == nil){
        kv.tx.index = make(map[string]uint64)
    }
    if(kv.tx.oldLeafLinks == nil){
        kv.tx.oldLeafLinks = make(map[uint64][2]uint64)
    }
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
    kv.tx.index = make(map[string]uint64)
    for col, off := range(selfIdx){
        kv.tx.index[col] = off
    }
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
    kv.freeList = new(NodeFreeList)
    kv.freeList.data = kv.page(IDX_FL_OFF)
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
        offset += 2 + len(col)
        kv.tx.index[col] = rOff
        binary.LittleEndian.PutUint64(metaPage[offset:], rOff)
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
    kv.tx.afterAllocPages = append(kv.tx.afterAllocPages, offset)

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
    for i := 0; i < len(kv.uLeafPages); i++ {
        node := new(NodeByte)
        node.data = kv.page(kv.uLeafPages[i])

        prev := node.prevNode()
        if(prev != nil){
            _, exists := kv.tx.oldLeafLinks[prev.selfPtr]
            if(exists == false){
                var oldLink [2]uint64
                oldLink[0] = NEXT_LINK
                oldLink[1] = prev.nextPtr()
                kv.tx.oldLeafLinks[prev.selfPtr] = oldLink
            }
            prev.setNext(kv.uLeafPages[i])
        }

        next := node.nextNode()
        if(next != nil){
            _, exists := kv.tx.oldLeafLinks[next.selfPtr]
            if(exists == false){
                var oldLink [2]uint64
                oldLink[0] = PREV_LINK
                oldLink[1] = next.prevPtr()
                kv.tx.oldLeafLinks[next.selfPtr] = oldLink
            }
            next.setPrev(kv.uLeafPages[i])
        }
    }
    kv.uLeafPages = kv.uLeafPages[:0]
}

func (kv *KV) TxBegin(){
    self = kv
    if(kv.tx.index == nil){
        kv.tx.index = make(map[string]uint64)
    }
    if(kv.tx.oldLeafLinks == nil){
        kv.tx.oldLeafLinks = make(map[uint64][2]uint64)
    }
    kv.tx.rootOffset = kv.rootOffset
    for col, off := range(selfIdx){
        kv.tx.index[col] = off
    }
}

func (kv *KV) TxCommit(){
    self = kv
    kv.tx.oldLeafLinks = nil
    kv.tx.afterAllocPages = kv.tx.afterAllocPages[:0]
    kv.flush()
}

func (kv *KV) TxRollback(){
    self = kv
    self.rootOffset = kv.tx.rootOffset
    self.rootByte.data = kv.page(self.rootOffset)
    self.rootByte.selfPtr = self.rootOffset

    self.altRootByte = self.rootByte
    self.altRootOffset = kv.tx.rootOffset

    for col, off := range(kv.tx.index){
        selfIdx[col] = off
    }

    for nodeptr, arr := range(kv.tx.oldLeafLinks){
        node := new(NodeByte)
        node.data = kv.page(nodeptr)
        if(arr[0] == NEXT_LINK){
            node.setNext(arr[1])
        }else{
            node.setPrev(arr[1])
        }
    }

    kv.tx.oldLeafLinks = nil

    for i := 0; i < len(kv.tx.afterAllocPages); i++ {
        kv.pushList.push(kv.tx.afterAllocPages[i])
    }

    kv.tx.afterAllocPages = kv.tx.afterAllocPages[:0]
    kv.flush()
}

func (kv *KV) flush(){
    if err := kv.fp.Sync(); err != nil {
        fmt.Println("ERROR in flush/Sync", err)
        os.Exit(1)
    }
    
    metaPage := kv.page(0)
    if(binary.LittleEndian.Uint16(metaPage[:]) == META_TYPE_IDX){
        kv.updateMetaIdx()
    }else{
        kv.updateMeta()
    }

    kv.updateFreeList()

    if err := kv.fp.Sync(); err != nil {
        fmt.Println("ERROR in flush/Sync", err)
        os.Exit(1)
    }
}

func (kv *KV) changeRoot(){

    kv.flushed += kv.nNewPages
    kv.nNewPages = 0
    kv.updateLeafLink()

    metaPage := kv.page(0)
    if(binary.LittleEndian.Uint16(metaPage[:]) == META_TYPE_IDX){
        offset := IDX_META_FIRST_PAIR
        for col, rOff := range(selfIdx){
            clen := len(col)
            offset += 2 + int(clen)
            
            if(rOff == self.rootOffset){
                self.rootByte = self.altRootByte
                self.rootOffset = self.altRootOffset
                selfIdx[col] = self.altRootOffset
                break
            }
            offset += 8
        }
    }else{
        self.rootOffset = self.altRootOffset
        self.rootByte = self.altRootByte
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
    node := new(NodeByte)
    node.data = kv.page(kv.tx.rootOffset)
    printTree(node, 0)
}

func (kv *KV) PrintIndex(){
    for _, off := range(kv.tx.index){
        node := new(NodeByte)
        node.data = kv.page(off)
        printTree(node, 0)
    }
}
