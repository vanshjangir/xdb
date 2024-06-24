package store

import (
	"fmt"
	"os"
    "syscall"
    "encoding/binary"
)

type KV struct{
    fp *os.File
    fSize uint64
    mSize uint64
    data [][]byte
}

func (kv *KV) Create(fileName string){
    self = kv
    kv.mapFile(fileName)
    kv.setMeta()
    createRoot()
}

func (kv *KV) Load(fileName string){
    self = kv
    kv.mapFile(fileName)
    kv.loadMeta()
}

func (kv *KV) loadMeta(){
    metaPage := kv.page(0)
    rootOffset = binary.LittleEndian.Uint64(metaPage[:])
    rootByte = new(NodeByte)
    rootByte.data = kv.page(rootOffset)
    rootByte.selfPtr = rootOffset
}

func (kv *KV) setMeta(){
    metaPage := kv.page(0)
    binary.LittleEndian.PutUint64(metaPage[:], 4096)
    binary.LittleEndian.PutUint16(metaPage[8:], TREE_PAGE_SIZE)
}

func setRootDisk(metaPage []byte, ptr uint64){
    binary.LittleEndian.PutUint64(metaPage[:], ptr)
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

    kv.fSize = max(uint64(fileInfo.Size()), TREE_PAGE_SIZE)
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
    if(kv.fSize == kv.mSize){
        err := kv.extendMap()
        if(err != nil){
            return nil, 0, err
        }
    }

    err := syscall.Fallocate(
        int(kv.fp.Fd()), 0, int64(kv.fSize),
        TREE_PAGE_SIZE,
    )
    if(err != nil){
        return nil, 0, err
    }

    kv.fSize += TREE_PAGE_SIZE
    binary.LittleEndian.PutUint64(kv.data[0][8:], kv.fSize)
    pageByte := kv.page(kv.fSize-TREE_PAGE_SIZE)

    return pageByte, kv.fSize-TREE_PAGE_SIZE, nil
}

func (kv *KV) Insert(key []byte, value []byte){
    insertLeaf(rootByte, key, value, 0)
}

func (kv *KV) Delete(key []byte){
    deleteLeaf(rootByte, key, 0)
}

func (kv *KV) Update(key []byte, value []byte){
    update(rootByte, key, value, 0)
}

func (kv *KV) Print(){
    printTree(rootByte, 0)
}
