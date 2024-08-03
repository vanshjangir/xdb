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

    OL_OFF = 4096
    FL_OFF = 8192
    ROOT_OFF = 12288

    IDX_META_FIRST_PAIR = 24

    PREV_LINK = 1
    NEXT_LINK = 2

    LEAF_PAIR_SIZE = 18
)

type Transaction struct{
    rootOffset uint64
    index map[string]uint64
    afterAllocPages []uint64
}

type KV struct{
    fp *os.File
    fSize uint64
    mSize uint64
    flushed uint64
    nNewPages uint64
    data [][]byte

    rootByte *NodeByte
    altRootByte *NodeByte
    rootOffset uint64
    altRootOffset uint64
    index map[string]uint64
    colname string

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
    kv.freeList.data = kv.page(FL_OFF)
}

func (kv *KV) setMeta(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_MAIN)
    binary.LittleEndian.PutUint64(metaPage[8:], ROOT_OFF)
    binary.LittleEndian.PutUint64(metaPage[16:], 2)
    kv.flushed = 2
}

func (kv *KV) updateMeta(){
    metaPage := kv.page(0)
    kv.tx.rootOffset = self.rootOffset
    binary.LittleEndian.PutUint64(metaPage[8:], kv.tx.rootOffset)
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
}

func (kv *KV) CreateIdx(fileName string, index map[string]uint64, columns []string){
    self = kv
    if(kv.tx.index == nil){
        kv.tx.index = make(map[string]uint64)
    }
    kv.index = index
    kv.mapFile(fileName)
    kv.setMetaIdx(columns)
    createFreeList()
    createRootIdx(index)
}

func (kv *KV) LoadIdx(fileName string, index map[string]uint64) []string{
    self = kv
    kv.index = index
    kv.mapFile(fileName)
    columns := kv.loadMetaIdx(index)
    kv.tx.index = make(map[string]uint64)
    for col, off := range(kv.index){
        kv.tx.index[col] = off
    }
    return columns
}

func (kv *KV) loadMetaIdx(index map[string]uint64) []string{
    metaPage := kv.page(0)
    idxlen := binary.LittleEndian.Uint64(metaPage[8:])
    offset := IDX_META_FIRST_PAIR
    var columns []string

    for i := 0; i < int(idxlen); i++ {
        clen := binary.LittleEndian.Uint16(metaPage[offset:])
        col := string(metaPage[offset+2 : offset+2+int(clen)])
        key := binary.LittleEndian.Uint64(metaPage[offset+2+int(clen):])
        index[col] = key
        offset += 2 + int(clen) + 8
        columns = append(columns, col)
    }
    kv.freeList = new(NodeFreeList)
    kv.freeList.data = kv.page(FL_OFF)
    return columns
}

func (kv *KV) setMetaIdx(columns []string){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_IDX)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(kv.index)))
    binary.LittleEndian.PutUint64(metaPage[16:], 2)

    offset := IDX_META_FIRST_PAIR
    for _,col := range(columns){
        clen := uint16(len(col))
        binary.LittleEndian.PutUint16(metaPage[offset:], clen)

        for i := 0; i < len(col); i++ {
            metaPage[offset+2+i] = col[i];
        }

        binary.LittleEndian.PutUint64(metaPage[offset+int(clen)+2:], kv.index[col])
        offset += 2 + int(clen) + 8
    }

    kv.flushed = 2
}

func (kv *KV) updateMetaIdx(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(kv.index)))
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
    
    offset := IDX_META_FIRST_PAIR
    for col, rOff := range(kv.index){
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

    kv.fSize = max(uint64(fileInfo.Size()), 4*TREE_PAGE_SIZE)
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

func (kv *KV) oldLinkToDisk(curPtr uint64, oldLink [2]uint64){
    page := kv.page(OL_OFF)
    npairs := binary.LittleEndian.Uint64(page[:])
    binary.LittleEndian.PutUint16(
        page[8 + npairs*LEAF_PAIR_SIZE:],
        uint16(oldLink[0]),
    )
    
    binary.LittleEndian.PutUint64(
        page[8 + npairs*LEAF_PAIR_SIZE + 2:],
        curPtr,
    )
    
    binary.LittleEndian.PutUint64(
        page[8 + npairs*LEAF_PAIR_SIZE + 10:],
        oldLink[1],
    )

    binary.LittleEndian.PutUint64(page[:], npairs+1)
}

func (kv *KV) TxBegin(){
    self = kv
    if(kv.tx.index == nil){
        kv.tx.index = make(map[string]uint64)
    }
    kv.tx.rootOffset = kv.rootOffset
    for col, off := range(kv.index){
        kv.tx.index[col] = off
    }

    kv.nNewPages = 0
    makeFreeListCopy()
}

func (kv *KV) TxCommit(){
    self = kv
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
        kv.index[col] = off
    }

    kv.pushList.setNoffs(0)

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
    
    kv.flushed += kv.nNewPages
    kv.nNewPages = 0
    
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
    metaPage := kv.page(0)

    if(binary.LittleEndian.Uint16(metaPage[:]) == META_TYPE_IDX){
        self.rootByte = self.altRootByte
        self.rootOffset = self.altRootOffset
        self.rootByte.selfPtr = self.rootOffset
        kv.index[kv.colname] = self.rootOffset
    }else{
        self.rootOffset = self.altRootOffset
        self.rootByte = self.altRootByte
    }
}

func (kv *KV) Insert(key []byte, value []byte){
    self = kv
    insertLeaf(self.rootByte, key, value, 0)
}

func (kv *KV) InsertIndex(colname string, key []byte, value []byte){
    self = kv
    kv.colname = colname
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(kv.index[colname])
    self.rootOffset = kv.index[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    insertLeaf(self.rootByte, key, value, 0)
}

func (kv *KV) Delete(key []byte){
    self = kv
    deleteLeaf(self.rootByte, key, 0)
}

func (kv *KV) DeleteIndex(colname string, key []byte){
    self = kv
    kv.colname = colname
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(kv.index[colname])
    self.rootOffset = kv.index[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    deleteLeaf(self.rootByte, key, 0)
}

func (kv *KV) Update(key []byte, value []byte){
    self = kv
    update(self.rootByte, key, value, 0)
}

func (kv *KV) UpdateIndex(colname string, key []byte, value []byte){
    self = kv
    kv.colname = colname
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(kv.index[colname])
    self.rootOffset = kv.index[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    update(self.rootByte, key, value, 0)
}

func (kv *KV) Get(key []byte) []byte{
    self = kv
    return qget(self.rootByte, key)
}

func (kv *KV) GetPkeyByIndex(colname string, key []byte) [][]byte{
    self = kv
    kv.colname = colname
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(kv.index[colname])
    self.rootOffset = kv.index[colname]
    self.rootByte.selfPtr = self.rootOffset

    var pkey [][]byte
    
    return qgetIdx(self.rootByte, key, pkey)
}

func (kv *KV) Range(keyStart []byte, keyEnd []byte) [][]byte {
    self = kv
    var it RangeIter
    qrange(self.rootByte, keyStart, keyEnd, &it)
    return it.values
}

func (kv *KV) RangeIdx(colname string, keyStart []byte, keyEnd []byte) [][]byte{
    self = kv
    kv.colname = colname
    self.rootByte = new(NodeByte)
    self.rootByte.data = kv.page(kv.index[colname])
    self.rootOffset = kv.index[colname]
    self.rootByte.selfPtr = self.rootOffset
    
    var it RangeIter
    qrangeIdx(self.rootByte, keyStart, keyEnd, &it)
    return it.values
}

func (kv *KV) Print(){
    self = kv
    printTree(kv.rootByte, 0)
}

func (kv *KV) PrintIndex(){
    self = kv
    for col := range(kv.tx.index){
        node := new(NodeByte)
        node.data = kv.page(kv.tx.index[col])
        fmt.Println(col)
        printTree(node, 0)
        fmt.Println()
    }
}
