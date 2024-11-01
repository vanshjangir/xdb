package storage

import (
    "encoding/binary"
    "fmt"
    "os"
)

type Table struct{
    fp *os.File
    idxfp *os.File
    kv *KeyValue
    idxkv *KeyValue
    tx *Transaction

    name string
    idxname string
    Index map[string]uint64
    Keyname string
    Columns []string
}

const (
    SEC_RANGE_GREATER = 1
    SEC_RANGE_LESS = 2
)

func (table *Table) Init(tx *Transaction){
    table.tx = tx
    table.kv = new(KeyValue)
    table.kv.Init(tx, table)
    table.idxkv = new(KeyValue)
    table.idxkv.Init(tx, table)
    table.Index = make(map[string]uint64)
}

func (table *Table) encode(secMap map[string][]byte) []byte{
    var value []byte
    for _, colname := range(table.Columns){
        tmp := make([]byte, 2)
        binary.LittleEndian.PutUint16(tmp, uint16(len(secMap[colname])))
        value = append(value, tmp[0])
        value = append(value, tmp[1])
        for i := 0; i < len(secMap[colname]); i++ {
            value = append(value, secMap[colname][i])
        }
    }
    return value
}

func (table *Table) decode(value []byte) map[string][]byte{
    secMap := make(map[string][]byte)
    var i int = 0
    for _,colname := range(table.Columns){
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

func (table *Table) CreateTable(tname string, cols []string, colSize []int) error {
    fullFilePath := "files/"+tname
    fullIndexPath := "files/"+tname+".idx"
    table.name = tname
    table.idxname = tname+".idx"

    if _,err := os.Create(fullFilePath); err != nil {
        return fmt.Errorf("os.Create file %v: %v", fullFilePath, err)
    }
    
    if _,err := os.Create(fullIndexPath); err != nil {
        return fmt.Errorf("os.Create index %v: %v", fullIndexPath, err)
    }

    table.Keyname = cols[0]
    totalSize := 0
    for i := range colSize {
        totalSize += colSize[i]
        if(i > 0){
            table.Index[cols[i]] = 0
            table.Columns = append(table.Columns, cols[i])
        }
    }
    totalSize += 4
   
    // Max keys to store in a page/node
    var maxKeys uint16
    maxKeys = uint16((TREE_PAGE_SIZE - 20 - 2)/(totalSize)) - 1
    
    table.kv.Create(fullFilePath, maxKeys, cols[0])
    table.idxkv.CreateIdx(fullIndexPath)

    if(table.tx.isGoing == true){
        table.tx.rootMap[table.name] = table.kv.rootOffset
        for idxname, offset := range(table.Index){
            table.tx.indexMap[table.name][idxname] = offset
        }
        table.kv.nNewPages = 0
        table.idxkv.nNewPages = 0
        table.kv.fl.makeFreeListCopy()
        table.idxkv.fl.makeFreeListCopy()
        table.tx.tables = append(table.tx.tables, table)
    }

    // Setting the same max keys for main tree and sec tree
    // can be changed acc to column size in sec tree
    table.kv.tree.Init(maxKeys)
    table.idxkv.tree.Init(maxKeys)

    return nil
}

func (table *Table) LoadTable(tname string) error {
    fullFilePath := "files/"+tname
    table.name = tname
    table.idxname = tname+".idx"
    fullIndexPath := "files/"+tname+".idx"
    var maxKeys uint16
    
    if keys, name, err := table.kv.Load(fullFilePath); err != nil {
        return fmt.Errorf("table.kv.Load: %v", err)
    } else {
        maxKeys = keys
        table.Keyname = name
    }
    
    if err := table.idxkv.LoadIdx(fullIndexPath); err != nil {
        return fmt.Errorf("table.idxkv.LoadIdx: %v", err)
    }

    table.tx.indexMap[table.name] = make(map[string]uint64)

    if(table.tx.isGoing == true){
        _, ok := table.tx.rootMap[table.name]
        if(ok == false){
            table.tx.rootMap[table.name] = table.kv.rootOffset
            for idxname, offset := range(table.Index){
                table.tx.indexMap[table.name][idxname] = offset
            }
            table.kv.nNewPages = 0
            table.idxkv.nNewPages = 0
            table.kv.fl.makeFreeListCopy()
            table.idxkv.fl.makeFreeListCopy()
            table.tx.tables = append(table.tx.tables, table)
        }
    }
    
    // Setting the same max keys for main tree and sec tree
    // can be changed acc to column size in sec tree
    table.kv.tree.Init(maxKeys)
    table.idxkv.tree.Init(maxKeys)

    return nil
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
        table.secDelete(colname, data, key)
        table.secInsert(colname, data, key)
    }
}

func (table *Table) Delete(key []byte){
    value := table.Get(key)
    table.kv.Delete(key)
    secMap := table.decode(value)
    for colname, data := range(secMap){
        table.secDelete(colname, data, key)
    }
}

func (table *Table) secDelete(colname string, data []byte, key []byte){
    klen := len(data) + len(key) + 2
    actualData := make([]byte, klen)
    for i := 0; i < len(data); i++ {
        actualData[i] = data[i]
    }
    for i := 0; i < len(key); i++ {
        actualData[len(data)+i] = key[i]
    }
    binary.LittleEndian.PutUint16(
        actualData[klen-2:],
        uint16(len(data)),
    )
    table.idxkv.DeleteIndex(colname, actualData)
}

func (table *Table) Get(key []byte) []byte {
    return table.kv.Get(key)
}

func (table *Table) GetPkey(colname string, secKey []byte) [][]byte{
    var pkey [][]byte
    for _,data := range(table.idxkv.GetPkeyByIndex(colname, secKey)){
        klen := len(data)
        secLen := binary.LittleEndian.Uint16(data[klen-2:])
        pkey = append(pkey, data[secLen:klen-2])
    }
    return pkey
}

func (table *Table) Range(keyStart []byte, keyEnd []byte) [][]byte {
    return table.kv.Range(keyStart, keyEnd)
}

func (table *Table) RangeIdx(colname string, keyStart []byte, keyEnd []byte) [][]byte{
    return table.idxkv.RangeIdx(colname, keyStart, keyEnd)
}

func (table *Table) Print(){
    table.kv.Print()
    fmt.Println()
    table.idxkv.PrintIndex()
}
