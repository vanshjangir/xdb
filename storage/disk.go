package storage

import (
	"fmt"
	"os"
    "syscall"
    "encoding/binary"
)

const (
    INITIAL_ROOT_OFF = 8192
    FL_PAGE_OFF = 4096
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

func (kv *KV) switchTree(){
    self = kv
    metaPage := kv.page(0)
    rootOffset = binary.LittleEndian.Uint64(metaPage[:])
    rootByte.data = kv.page(rootOffset)
    rootByte.selfPtr = rootOffset
    
    altRootByte = rootByte
    altRootOffset = rootOffset
    altRootByte.selfPtr = rootOffset

    kv.nNewPages = 0
}

func (kv *KV) loadMeta(){
    metaPage := kv.page(0)
    rootOffset = binary.LittleEndian.Uint64(metaPage[:])
    kv.flushed = binary.LittleEndian.Uint64(metaPage[8:])

    rootByte = new(NodeByte)
    rootByte.data = kv.page(rootOffset)
    rootByte.selfPtr = rootOffset

    freeList = new(NodeFreeList)
    freeList.data = kv.page(FL_PAGE_OFF)
}

func (kv *KV) setMeta(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], INITIAL_ROOT_OFF)
    binary.LittleEndian.PutUint64(metaPage[8:], 2)
    kv.flushed = 2
}

func (kv *KV) updateMeta(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], rootOffset)
    binary.LittleEndian.PutUint64(metaPage[8:], kv.flushed)
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

    if(popList != nil && popList.noffs() > 0){
        var err error
        offset, err = popList.pop()
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
    if(popList == nil || pushList == nil){
        return
    }

    freeList.setNoffs(popList.noffs())
    for{
        if(pushList.noffs() == 0){
            break
        }
        offset, _ := pushList.pop()
        freeList.push(offset)
    }

    popList.setNoffs(0)
    pushList.setNoffs(0)
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

    rootOffset = altRootOffset
    rootByte = altRootByte
    kv.flushed += kv.nNewPages
    kv.nNewPages = 0

    kv.updateLeafLink()
    kv.updateMeta()
    kv.updateFreeList()

    if err := kv.fp.Sync(); err != nil {
        fmt.Println("ERROR in flush/Sync", err)
        os.Exit(1)
    }
}

func (kv *KV) Insert(key []byte, value []byte){
    kv.switchTree()
    makeFreeListCopy()
    insertLeaf(rootByte, key, value, 0)
}

func (kv *KV) Delete(key []byte){
    kv.switchTree()
    makeFreeListCopy()
    deleteLeaf(rootByte, key, 0)
}

func (kv *KV) Update(key []byte, value []byte){
    kv.switchTree()
    makeFreeListCopy()
    update(rootByte, key, value, 0)
}

func (kv *KV) Get(key []byte) []byte{
    kv.switchTree()
    makeFreeListCopy()
    return qget(rootByte, key, 0)
}

func (kv *KV) Range(keyStart []byte, keyEnd []byte) [][]byte {
    kv.switchTree()
    makeFreeListCopy()
    return qrange(rootByte, keyStart, keyEnd)
}

func (kv *KV) Print(){
    kv.switchTree()
    printTree(rootByte, 0)
}
