package table

import (
    "encoding/binary"
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

func (table *Table) encode(secMap map[string][]byte) []byte{
    var value []byte
    for _, data := range(secMap){
        tmp := make([]byte, 2)
        binary.LittleEndian.PutUint16(tmp, uint16(len(data)))
        value = append(value, tmp[0])
        value = append(value, tmp[1])
        for i := 0; i < len(data); i++ {
            value = append(value, data[i])
        }
    }
    return value
}

func (table *Table) decode(value []byte) map[string][]byte{
    secMap := make(map[string][]byte)
    var i int = 0
    for colname := range(table.index){
        if(i >= len(value)){
            break
        }
        dataLen := binary.LittleEndian.Uint16(value[i:])
        i += 2
        
        data := make([]byte, dataLen)
        for j := 0; j < int(dataLen); j++ {
            data[j] = value[i]
            i++
        }
        secMap[colname] = data
    }

    return secMap
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
    table.index["secondcol"] = 0
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

func (table *Table) Insert(key []byte, secMap map[string][]byte){
    value := table.encode(secMap)
    table.kv.Insert(key, value)
    for colname := range(secMap){
        table.secInsert(colname, secMap[colname], key)
    }
}

func (table *Table) secInsert(colname string, data []byte, key []byte){
    klen := len(data) + len(key) + 2
    actualKey := make([]byte, klen)
    for i := 0; i < len(data); i++ {
        actualKey[i] = data[i]
    }
    for i := 0; i < len(key); i++ {
        actualKey[len(data)+i] = key[i]
    }
    binary.LittleEndian.PutUint16(
        actualKey[klen-2:],
        uint16(len(data)),
    )
    table.idxkv.InsertIndex(colname, actualKey, nil)
}

func (table *Table) Update(key []byte, secMap map[string][]byte){
    value := table.encode(secMap)
    oldValue := table.kv.Get(key)
    table.kv.Update(key, value)
    table.secUpdate(key, oldValue)
}

func (table *Table) secUpdate(key []byte, oldValue []byte){
    secMap := table.decode(oldValue)
    for colname, data := range(secMap){
        table.secDelete(colname, data)
        table.secInsert(colname, data, key)
    }
}

func (table *Table) Delete(key []byte){
    value := table.Get(key)
    table.kv.Delete(key)
    secMap := table.decode(value)
    for colname, data := range(secMap){
        table.secDelete(colname, data)
    }
}

func (table *Table) secDelete(colname string, data []byte){
    actualData := table.idxkv.GetIndex(colname, data)
    table.idxkv.DeleteIndex(colname, actualData)
}

func (table *Table) Get(key []byte) []byte {
    return table.kv.Get(key)
}

func (table *Table) Range(keyStart []byte, keyEnd []byte) [][]byte {
    return table.kv.Range(keyStart, keyEnd)
}

func (table *Table) BEGIN(){
    table.kv.TxBegin()
    table.idxkv.TxBegin()
}

func (table *Table) COMMIT(){
    table.kv.TxCommit()
    table.idxkv.TxCommit()
}

func (table *Table) ROLLBACK(){
    table.kv.TxRollback()
    table.idxkv.TxRollback()
}

func (table *Table) Print(){
    table.kv.Print()
    fmt.Println()
    table.idxkv.PrintIndex()
}
