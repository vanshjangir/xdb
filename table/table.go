package table

import (
	"fmt"
	"os"
	"github.com/vanshjangir/xdb/storage"
)

type Table struct{
    fp *os.File
    kv *storage.KV
    name string
    nfields int64
    pkey []byte
    cols []string
}

func (table *Table) CreateTable(tname string) error {
    fullFilePath := "../files/xdb/"+tname
    table.name = tname

    if _,err := os.Create(fullFilePath); err != nil{
        return fmt.Errorf("ERROR creating table file: %v", err)
    }

    table.kv = new(storage.KV)
    table.kv.Create(fullFilePath)

    return nil
}

func (table *Table) LoadTable(tname string){
    fullFilePath := "../files/xdb/"+tname
    table.name = tname
    
    table.kv = new(storage.KV)
    table.kv.Load(fullFilePath)
}

func (table *Table) Insert(key []byte, value []byte){
    table.kv.Insert(key, value)
}

func (table *Table) Update(key []byte, value []byte){
    table.kv.Update(key, value)
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
