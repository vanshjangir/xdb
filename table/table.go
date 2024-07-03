package table

import (
	"fmt"
	"os"
	"github.com/vanshjangir/xdb/storage"
)

type Table struct{
    fp *os.File
    idxfp *os.File
    kv *storage.KV
    idxkv *storage.KV
    name string
    idxname string
    index map[string]uint64
}

func (table *Table) CreateTable(tname string) error {
    fullFilePath := "files/"+tname
    fullIndexPath := "files/"+tname+".idx"
    table.name = tname
    table.idxname = tname+".idx"

    if _,err := os.Create(fullFilePath); err != nil{
        return fmt.Errorf("ERROR creating table file: %v", err)
    }
    
    if _,err := os.Create(fullIndexPath); err != nil{
        return fmt.Errorf("ERROR creating index file: %v", err)
    }

    table.kv = new(storage.KV)
    table.kv.Create(fullFilePath)
    
    table.idxkv = new(storage.KV)
    table.index = make(map[string]uint64)
    table.index["firstcol"] = 0
    table.idxkv.CreateIdx(fullIndexPath, table.index)

    return nil
}

func (table *Table) LoadTable(tname string){
    fullFilePath := "files/"+tname
    table.name = tname
    table.idxname = tname+".idx"
    fullIndexPath := "files/"+tname+".idx"
    
    table.kv = new(storage.KV)
    table.kv.Load(fullFilePath)

    table.idxkv = new(storage.KV)
    table.index = make(map[string]uint64)
    table.idxkv.LoadIdx(fullIndexPath, table.index)
}

func (table *Table) Insert(key []byte, value []byte){
    table.kv.Insert(key, value)
    for colname := range(table.index){
        table.idxkv.InsertIndex(colname, value, key)
    }
}

func (table *Table) Update(key []byte, value []byte){
    table.kv.Update(key, value)
    // when updating a value, the corresponding sec. index
    // has to be first deleted and new index has to be inserted
    // has to create some way to extract sec. index from the value
}

func (table *Table) Delete(key []byte){
    table.kv.Delete(key)
}

func (table *Table) Get(key []byte) []byte {
    return table.kv.Get(key)
}

func (table *Table) Range(keyStart []byte, keyEnd []byte) [][]byte {
    return table.kv.Range(keyStart, keyEnd)
}

func (table *Table) Print(){
    table.kv.Print()
    fmt.Println()
    table.idxkv.PrintIndex(table.index)
}
