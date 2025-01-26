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
    IsGoing bool
    rootMap map[string]uint64
    indexMap map[string]map[string]uint64
    afterAllocPages map[string][]uint64
    tables []*Table
}

type KeyValue struct{
    fp *os.File
    fSize uint64
    mSize uint64
    flushed uint64
    nNewPages uint64
    data [][]byte

    // Tree specific fields, can change according
    // to the tree that is being accessed

    rootByte *NodeByte
    altRootByte *NodeByte
    rootOffset uint64
    altRootOffset uint64
    colname string

    freeList *NodeFreeList
    pushList *NodeFreeList
    popList *NodeFreeList

    // Pointer to access Tree functions like
    // creating Root, queries, printing, etc
    tree *Tree

    // Pointer to access FreeList functions like
    // creating NodeFreeList and making NodeFreeList copies

    fl *FreeList

    // Poitner to access the parent
    // Transation and Table struct

    tx *Transaction
    table *Table
}

func (kv *KeyValue) Init(tx *Transaction, table *Table){
    kv.tx = tx
    kv.table = table
    kv.tree = new(Tree)
    kv.fl = new(FreeList)
    kv.fl.kv = kv
    kv.tree.kv = kv
}

func (tx *Transaction) Init(){
    tx.IsGoing = false
    tx.rootMap = make(map[string]uint64)
    tx.indexMap = make(map[string]map[string]uint64)
    tx.afterAllocPages = make(map[string][]uint64)
}

func (kv *KeyValue) Create(fileName string, maxKeys uint16, keyname string) error {
    if err := kv.mapFile(fileName); err != nil {
        return fmt.Errorf("Create: %v", err)
    }
    kv.setMeta(maxKeys, keyname)
    kv.fl.createFreeList()
    kv.tree.createRoot()

    return nil
}

func (kv *KeyValue) Load(fileName string) (uint16, string, error) {
    if err := kv.mapFile(fileName); err != nil {
        return 0, "", fmt.Errorf("Load: %v", err)
    }
    maxSize, keyname := kv.loadMeta()
    return maxSize, keyname, nil
}

func (kv *KeyValue) loadMeta() (uint16, string) {
    metaPage := kv.page(0)
    kv.rootOffset = binary.LittleEndian.Uint64(metaPage[8:])
    kv.flushed = binary.LittleEndian.Uint64(metaPage[16:])

    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.rootOffset)
    kv.rootByte.selfPtr = kv.rootOffset
    kv.rootByte.tree = kv.tree

    kv.freeList = new(NodeFreeList)
    kv.freeList.data = kv.page(FL_OFF)

    keynameLen := binary.LittleEndian.Uint16(metaPage[26:])
    keyname := string(metaPage[28:28+keynameLen])

    return binary.LittleEndian.Uint16(metaPage[24:]), keyname
}

func (kv *KeyValue) setMeta(maxKeys uint16, keyname string){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_MAIN)
    binary.LittleEndian.PutUint64(metaPage[8:], ROOT_OFF)
    binary.LittleEndian.PutUint64(metaPage[16:], 2)
    binary.LittleEndian.PutUint16(metaPage[24:], maxKeys)
    keynameLen := uint16(len(keyname))
    binary.LittleEndian.PutUint16(metaPage[26:], keynameLen)
    copy(metaPage[28:28+keynameLen], []byte(keyname))
    kv.flushed = 2
}

func (kv *KeyValue) updateMeta(){
    metaPage := kv.page(0)
    kv.tx.rootMap[kv.table.name] = kv.rootOffset
    binary.LittleEndian.PutUint64(metaPage[8:], kv.rootOffset)
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
}

func (kv *KeyValue) CreateIdx(fileName string) error {
    if err := kv.mapFile(fileName); err != nil {
        return fmt.Errorf("kv.mapFile: %v", err)
    }
    kv.setMetaIdx()
    kv.fl.createFreeList()
    kv.tree.createRootIdx()

    return nil
}

func (kv *KeyValue) LoadIdx(fileName string) error {
    if err := kv.mapFile(fileName); err != nil {
        return fmt.Errorf("kv.mapFile: %v", err)
    }
    kv.loadMetaIdx()
    return nil
}

func (kv *KeyValue) loadMetaIdx() []string{
    metaPage := kv.page(0)
    idxlen := binary.LittleEndian.Uint64(metaPage[8:])
    offset := IDX_META_FIRST_PAIR
    var columns []string

    for i := 0; i < int(idxlen); i++ {
        clen := binary.LittleEndian.Uint16(metaPage[offset:])
        col := string(metaPage[offset+2 : offset+2+int(clen)])
        key := binary.LittleEndian.Uint64(metaPage[offset+2+int(clen):])
        kv.table.Index[col] = key
        offset += 2 + int(clen) + 8
        columns = append(columns, col)
    }
    kv.freeList = new(NodeFreeList)
    kv.freeList.data = kv.page(FL_OFF)
    return columns
}

func (kv *KeyValue) setMetaIdx(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], META_TYPE_IDX)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(kv.table.Index)))
    binary.LittleEndian.PutUint64(metaPage[16:], 2)

    offset := IDX_META_FIRST_PAIR
    for _,col := range(kv.table.Columns){
        clen := uint16(len(col))
        binary.LittleEndian.PutUint16(metaPage[offset:], clen)

        for i := 0; i < len(col); i++ {
            metaPage[offset+2+i] = col[i];
        }

        binary.LittleEndian.PutUint64(metaPage[offset+int(clen)+2:], kv.table.Index[col])
        offset += 2 + int(clen) + 8
    }

    kv.flushed = 2
}

func (kv *KeyValue) updateMetaIdx(){
    tableName := kv.table.name
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[8:], uint64(len(kv.table.Index)))
    binary.LittleEndian.PutUint64(metaPage[16:], kv.flushed)
   
    index := make(map[string]uint64)
    offset := IDX_META_FIRST_PAIR
    for col, rOff := range(kv.table.Index){
        offset += 2 + len(col)
        index[col] = rOff
        binary.LittleEndian.PutUint64(metaPage[offset:], rOff)
        offset += 8
    }
    kv.tx.indexMap[tableName] = index
}

func (kv *KeyValue) mapFile(fileName string) error {
    var file *os.File
    var fileInfo os.FileInfo
    var fileChunk []byte
    var err error

    file,err = os.OpenFile(fileName, os.O_RDWR, 600)
    if(err != nil){
        return fmt.Errorf("mapFile->os.OpenFile: %v", err)
    }
    kv.fp = file

    fileInfo,err = file.Stat()
    if(err != nil){
        return fmt.Errorf("mapFile->file.Stat: %v", err)
    }

    if(fileInfo.Size()%TREE_PAGE_SIZE != 0){
        return fmt.Errorf("mapFile->file size not a multiple of page size")
    }

    kv.fSize = max(uint64(fileInfo.Size()), 4*TREE_PAGE_SIZE)
    kv.mSize = 2*kv.fSize

    fileChunk, err = syscall.Mmap(
        int(file.Fd()), 0, int(kv.mSize),
        syscall.PROT_READ | syscall.PROT_WRITE,
        syscall.MAP_SHARED,
    )
    if(err != nil){
        return fmt.Errorf("mapFile->syscall.Mmap: %v", err)
    }

    err = syscall.Fallocate(
        int(kv.fp.Fd()), 0, 0,
        int64(kv.fSize),
    )
    if(err != nil){
        return fmt.Errorf("mapFile->syscall.Fallocate: %v", err)
    }

    kv.data = append(kv.data, fileChunk)
    return nil
}

func (kv *KeyValue) extendMap() error {
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

func (kv *KeyValue) page(offset uint64) []byte {
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

func (kv *KeyValue) newpage() ([]byte, uint64, error){

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
    kv.tx.afterAllocPages[kv.table.name] = append(kv.tx.afterAllocPages[kv.table.name], offset)

out:
    pageByte = kv.page(offset)
    return pageByte, offset, nil
}

func (kv *KeyValue) updateFreeList(){
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

func (tx *Transaction) Begin(){
    tx.IsGoing = true
}

func (tx *Transaction) Commit(){
    for _,table := range(tx.tables){
        tx.afterAllocPages[table.name] = tx.afterAllocPages[table.name][:0]
        table.kv.flush()
        table.idxkv.flush()
    }
    tx.IsGoing = false
}

func (tx *Transaction) Rollback(){
    for _, table := range(tx.tables){
        table.kv.rootOffset = tx.rootMap[table.name]
        table.kv.rootByte.data = table.kv.page(table.kv.rootOffset)
        table.kv.rootByte.selfPtr = table.kv.rootOffset
        table.kv.altRootByte = table.kv.rootByte
        table.kv.altRootOffset = table.kv.rootOffset

        for col, off := range(tx.indexMap[table.name]){
            table.Index[col] = off
        }
        
        table.kv.pushList.setNoffs(0)

        afterAllocPages := tx.afterAllocPages[table.name]
        for i := 0; i < len(afterAllocPages); i++ {
            table.kv.pushList.push(afterAllocPages[i])
        }

        tx.afterAllocPages[table.name] = tx.afterAllocPages[table.name][:0]
        table.kv.flush()
    }
    tx.IsGoing = false
}

func (kv *KeyValue) flush(){
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

func (kv *KeyValue) changeRoot(){
    metaPage := kv.page(0)

    if(binary.LittleEndian.Uint16(metaPage[:]) == META_TYPE_IDX){
        kv.altRootOffset = kv.altRootByte.selfPtr
        kv.rootByte = kv.altRootByte
        kv.rootOffset = kv.altRootOffset
        kv.rootByte.selfPtr = kv.rootOffset
        kv.table.Index[kv.colname] = kv.rootOffset
    }else{
        kv.altRootOffset = kv.altRootByte.selfPtr
        kv.rootOffset = kv.altRootOffset
        kv.rootByte = kv.altRootByte
    }
}

func (kv *KeyValue) Insert(key []byte, value []byte){
    kv.tree.insertLeaf(kv.rootByte, key, value, 0)
}

func (kv *KeyValue) InsertIndex(colname string, key []byte, value []byte){
    kv.colname = colname
    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.table.Index[colname])
    kv.rootOffset = kv.table.Index[colname]
    kv.rootByte.selfPtr = kv.rootOffset
    kv.rootByte.tree = kv.tree
    
    kv.tree.insertLeaf(kv.rootByte, key, value, 0)
}

func (kv *KeyValue) Delete(key []byte){
    kv.tree.deleteLeaf(kv.rootByte, key, 0)
}

func (kv *KeyValue) DeleteIndex(colname string, key []byte){
    kv.colname = colname
    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.table.Index[colname])
    kv.rootOffset = kv.table.Index[colname]
    kv.rootByte.selfPtr = kv.rootOffset
    kv.rootByte.tree = kv.tree
    
    kv.tree.deleteLeaf(kv.rootByte, key, 0)
}

func (kv *KeyValue) Update(key []byte, value []byte){
    kv.tree.update(kv.rootByte, key, value, 0)
}

func (kv *KeyValue) UpdateIndex(colname string, key []byte, value []byte){
    kv.colname = colname
    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.table.Index[colname])
    kv.rootOffset = kv.table.Index[colname]
    kv.rootByte.selfPtr = kv.rootOffset
    kv.rootByte.tree = kv.tree
    
    kv.tree.update(kv.rootByte, key, value, 0)
}

func (kv *KeyValue) Get(key []byte) []byte{
    kv.rootByte.tree = kv.tree
    return kv.tree.qget(kv.rootByte, key)
}

func (kv *KeyValue) GetPkeyByIndex(colname string, key []byte) [][]byte{
    kv.colname = colname
    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.table.Index[colname])
    kv.rootOffset = kv.table.Index[colname]
    kv.rootByte.selfPtr = kv.rootOffset

    var pkey [][]byte
   
    kv.rootByte.tree = kv.tree
    return kv.tree.qgetIdx(kv.rootByte, key, pkey)
}

func (kv *KeyValue) Range(keyStart []byte, keyEnd []byte) [][]byte {
    var it RangeIter
    kv.rootByte.tree = kv.tree
    kv.tree.qrange(kv.rootByte, keyStart, keyEnd, &it)
    return it.values
}

func (kv *KeyValue) RangeIdx(colname string, keyStart []byte, keyEnd []byte) [][]byte{
    kv.colname = colname
    kv.rootByte = new(NodeByte)
    kv.rootByte.data = kv.page(kv.table.Index[colname])
    kv.rootOffset = kv.table.Index[colname]
    kv.rootByte.selfPtr = kv.rootOffset
    
    var it RangeIter
    kv.rootByte.tree = kv.tree
    kv.tree.qrangeIdx(kv.rootByte, keyStart, keyEnd, &it)
    return it.values
}

func (kv *KeyValue) Print(){
    kv.rootByte.tree = kv.tree
    kv.tree.printTree(kv.rootByte, 0)
}

func (kv *KeyValue) PrintIndex(){
    for col := range(kv.table.Index){
        node := new(NodeByte)
        node.data = kv.page(kv.table.Index[col])
        node.tree = kv.tree
        fmt.Println(col)
        kv.tree.printTree(node, 0)
        fmt.Println()
    }
}
