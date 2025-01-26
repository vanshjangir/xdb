package database

import (
    "encoding/binary"
    "fmt"
    "os"
    "regexp"
    "strings"
    "github.com/vanshjangir/xdb/storage"
)

type Xdb struct{
    Name string
    tables map[string]*storage.Table
    tx *storage.Transaction
}

func (db *Xdb) Init(name string) error {
    homeDir, _ := os.UserHomeDir()
    _, err := os.Stat(homeDir + "/" + name+"-xdb")
    if err != nil || os.IsNotExist(err) {
        return err
    }

    db.Name = name

    return nil
}

func createMetaTable(dbname string) error {
    db := new(Xdb)
    if err := db.Init(dbname); err != nil {
        return fmt.Errorf("createMetaTable: %v", err)
    }

    db.BeginTxn()

    columns := []string{"tablename", "columns", "size"}
    colSize := []int{50, 150, 100}
    
    if err := db.CreateTable(dbname + "_meta", columns, colSize);
    err != nil {
        return fmt.Errorf("Error in create table: %v", err)
    }

    db.CommitTxn()

    return nil
}

func (db *Xdb) updateMetaTable(
    tableName string, tColumns []string, tColSize []int,
) error {
    
    var colSize []string
    for i := range tColSize {
        colSize = append(colSize, fmt.Sprint(tColSize[i]))
    }

    tableNameByte := []byte(tableName)
    aggCols := []byte(strings.Join(tColumns, ","))
    aggSize := []byte(strings.Join(colSize, ","))

    metaCols := []string{"tablename", "columns", "size"}
    metaVals := [][]byte{tableNameByte, aggCols, aggSize}

    metaTableName := db.Name + "_meta"
    if err := db.Insert(metaTableName, metaCols, metaVals); err != nil {
        return err
    }

    return nil
}

func CreateDatabase(dbname string) error {
    if dbname == "xdb" {
        return fmt.Errorf("Cannot create a database xdb, choose different name")
    }
    homeDir, _ := os.UserHomeDir()
    if err := os.Mkdir(homeDir + "/" + dbname + "-xdb", 0755); err != nil {
        return err
    }

    if err := createMetaTable(dbname); err != nil {
        return err
    }

    return nil
}

func (db *Xdb) BeginTxn(){
    db.tx = new(storage.Transaction)
    db.tables = make(map[string]*storage.Table)
    db.tx.Init()
    db.tx.Begin()
}

func (db *Xdb) CommitTxn(){
    db.tx.Commit()
    db.tx = nil
    db.tables = nil
}

func (db *Xdb) RollbackTxn(){
    db.tx.Rollback()
    db.tx = nil
    db.tables = nil
}

func ListDB(){
    homeDir, _ := os.UserHomeDir()
    entries, err := os.ReadDir(homeDir)
    if err != nil {
        fmt.Println("Error listing databases: " ,err)
        return
    }

    for _, v := range entries {
        if v.IsDir() {
            if ok, _ := regexp.MatchString(`.*-xdb$`,v.Name()); ok {
                fmt.Println(v.Name())
            }
        }
    }

}

func (db *Xdb) Opentable(tableName string) error {
    _, ok := db.tables[tableName]
    if !ok {
        table := new(storage.Table)
        table.Init(db.tx)
        if err := table.LoadTable(db.Name + "-xdb/" + tableName); err != nil {
            return fmt.Errorf("table.LoadTable: %v", err)
        }
        db.tables[tableName] = table
    }

    return nil
}

func (db *Xdb) CreateTable(tableName string, columns []string, colSize []int) error {
    if err := db.Opentable(tableName); err == nil {
        return fmt.Errorf("Table %v already exists", tableName)
    }

    var table storage.Table
    db.tables[tableName] = &table
    
    table.Init(db.tx)
    if err := table.CreateTable(db.Name + "-xdb/" + tableName, columns, colSize);
    err != nil {
        return fmt.Errorf("table.CreateTable: %v", err)
    }
    
    if err := db.updateMetaTable(tableName, columns, colSize); err != nil {
        return err
    }

    return nil
}

func (db *Xdb) Insert(tableName string, columns []string, values [][]byte) error {
    if err := db.Opentable(tableName); err != nil {
        return fmt.Errorf("Table %v does not exists", tableName)
    }

    table := db.tables[tableName]
    secMap := make(map[string][]byte)
    for i := range values {
        if(i > 0){
            secMap[columns[i]] = values[i];
        }
    }
    table.Insert(values[0], secMap)

    return nil
}

func (db *Xdb) Delete(tableName string, key []byte) error {
    if err := db.Opentable(tableName); err != nil {
        return fmt.Errorf("Table %v does not exists", tableName)
    }

    table := db.tables[tableName]
    table.Delete(key)

    return nil
}

func (db* Xdb) Update(tableName string, key []byte, value []byte) error {
    return nil
}

func (db *Xdb) Select(tableName string) error {
    if err := db.Opentable(tableName); err != nil {
        return fmt.Errorf("Table %v does not exists", tableName)
    }
    
    table := db.tables[tableName]
    end := make([]byte, 100)
    for i := range end {
        end[i] = 255
    }

    var title [][]byte
    title = append(title, []byte(db.tables[tableName].Keyname))
    cols := db.tables[tableName].Columns
    for j := range cols {
        title = append(title, []byte(cols[j]))
    }

    var data [][][]byte
    data = append(data, title)
    res := table.Range([]byte{0}, end)

    for i := range res {
        idx := 0
        var rows [][]byte
        for range len(cols)+1 {
            dlen := binary.LittleEndian.Uint16(res[i][idx:idx+2])
            row := res[i][idx + 2 : idx + 2 + int(dlen)]
            rows =append(rows, row)
            idx += int(dlen) + 2
        }
        data = append(data, rows)
    }

    PrintTableStyle(data)
    
    return nil
}

func PrintTableStyle(data [][][]byte) {
    maxlen := make([]int, len(data[0]))
    for i := range data {
        for j := range data[i] {
            maxlen[j] = max(maxlen[j], len(data[i][j]))
        }
    }

    for j := range data[0] {
        dlen := maxlen[j] + 4 - maxlen[j]%4
        
        fmt.Print("+")
        for range dlen {
            fmt.Print("-")
        }
    }
    fmt.Print("+")
    fmt.Println()

    for j := range data[0] {
        fmt.Print("|")
        dlen := maxlen[j] + 4 - maxlen[j]%4
        nspace := dlen - len(data[0][j])

        fmt.Print(string(data[0][j]))
        for range nspace {
            fmt.Print(" ")
        }
        if j == len(data[0]) - 1 {
            fmt.Print("|")
        }
    }
    fmt.Println()
    
    for j := range data[0] {
        dlen := maxlen[j] + 4 - maxlen[j]%4
        
        fmt.Print("+")
        for range dlen {
            fmt.Print("-")
        }
    }
    fmt.Print("+")
    fmt.Println()
    
    for i := range data {
        if i == 0 {
            continue
        }
        for j := range data[i] {
            fmt.Print("|")
            dlen := maxlen[j] + 4 - maxlen[j]%4
            nspace := dlen - len(data[i][j])

            fmt.Print(string(data[i][j]))
            for range nspace {
                fmt.Print(" ")
            }
            if j == len(data[i]) - 1 {
                fmt.Print("|")
            }
        }
        fmt.Println()
    }
    
    for j := range data[0] {
        dlen := maxlen[j] + 4 - maxlen[j]%4
        
        fmt.Print("+")
        for range dlen {
            fmt.Print("-")
        }
    }
    fmt.Print("+\n\n")
}

func (db *Xdb) Print(tableName string) error {
    if err := db.Opentable(tableName); err != nil {
        return fmt.Errorf("Table %v does not exists", tableName)
    }

    table := db.tables[tableName]
    table.Print()

    return nil
}
