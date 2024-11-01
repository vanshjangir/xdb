package database

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
    "os"

	"github.com/vanshjangir/xdb/storage"
	"github.com/xwb1989/sqlparser"
)

func Parse(db *Xdb, query string){
    cmds := strings.Split(query, " ")
    if(cmds[0] == "db"){
        parseDB(db, cmds)
    } else if(cmds[0] == "exit") {
        os.Exit(0)
    } else {
        parseSQL(db, query)
    }
}

func parseSQL(db *Xdb, query string){
    stmt, err := sqlparser.Parse(query)
    if(err != nil){
        fmt.Println("Syntax Error in sql query: ", err)
        return
    }

    switch stmt := stmt.(type) {
    case *sqlparser.DDL:
        if(stmt.Action == "create"){
            execCreateStmt(db, stmt)
        }
    case *sqlparser.Insert:
        execInsertStmt(db, stmt)
    case *sqlparser.Delete:
        execDeleteStmt(db, stmt)
    case *sqlparser.Update:
        execUpdateStmt(db, stmt)
    case *sqlparser.Select:
        execSelectStmt(db, stmt)
    }
}

func parseDB(db *Xdb, cmds []string){
    switch cmd := cmds[1]; cmd {
    case "use":
        if err := db.Init(cmds[2]); err != nil {
            fmt.Println(err)
        }

    case "create":
        if err := CreateDatabase(cmds[2]); err != nil {
            fmt.Println(err)
        }

    case "begin":
        db.BeginTxn()
        fmt.Println("New transaction started")

    case "rollback":
        db.RollbackTxn()
        fmt.Println("Transaction rolled back")

    case "commit":
        db.CommitTxn()
        fmt.Println("Transaction committed")
    }
}

func execCreateStmt(db *Xdb, stmt *sqlparser.DDL){
    var colSize []int
    var columns []string
    for _, col := range stmt.TableSpec.Columns {
        columns = append(columns, col.Name.String())
        if csize, err := strconv.Atoi(string(col.Type.Length.Val)); err != nil {

        } else {
            colSize = append(colSize, csize)
        }
    }

    fmt.Println(columns, colSize)
    if err := db.CreateTable(stmt.NewName.Name.String(), columns, colSize);
    err != nil {
        fmt.Println("Error in create table: ", err)
    }
}

func execInsertStmt(db *Xdb, stmt *sqlparser.Insert){
    var columns []string
    for i := range stmt.Columns {
        columns = append(columns, sqlparser.String(stmt.Columns[i]))
    }

    if rows, ok := stmt.Rows.(sqlparser.Values); ok {
        for _, row := range rows {
            var values [][]byte
            for _, val := range row {
                values = append(values, []byte(sqlparser.String(val)));
            }
            if err := db.Insert(stmt.Table.Name.String(), columns, values);
            err != nil {
                fmt.Println("Error in insert: ", err)
            }
        }
    }
}

func execDeleteStmt(db *Xdb, stmt *sqlparser.Delete){
    var tablename string
    if len(stmt.TableExprs) > 0 {
        if alias, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr); ok {
            tablename = sqlparser.String(sqlparser.GetTableName(alias.Expr))
        }
    }

    pkeys := evalWhere(db.tables[tablename], *stmt.Where)
    fmt.Println("PKEYS:",pkeys)
    for _, key := range pkeys {
        fmt.Println(key)
        db.Delete(tablename, key)
    }
}

func execUpdateStmt(db *Xdb, stmt *sqlparser.Update){
}

func execSelectStmt(db *Xdb, stmt *sqlparser.Select){
    tableExpr := stmt.From[0].(*sqlparser.AliasedTableExpr)
    tableName := tableExpr.Expr.(sqlparser.TableName).Name.String()
    db.Select(tableName)
}

func evalWhere(table *storage.Table, stmt sqlparser.Where) [][]byte {
    var keys [][]byte

    switch stmt := stmt.Expr.(type) {
    case *sqlparser.AndExpr:
        fmt.Println("it is an And Expr", stmt)

    case *sqlparser.ComparisonExpr:
        colname := sqlparser.String(stmt.Left)
        data := sqlparser.String(stmt.Right)
        op := stmt.Operator

        if op == ">" {
            // do something
        } else if op == "<" {
            if(colname == table.Keyname){
                for _, row := range table.Range([]byte{0}, []byte(data)){
                    klen := binary.LittleEndian.Uint16(row[:2])
                    keys = append(keys, row[2:klen])
                }
            } else {
                for _, row := range table.RangeIdx(colname, []byte{0}, []byte(data)){
                    klen := len(row)
                    secLen := binary.LittleEndian.Uint16(row[klen-2:])
                    keys = append(keys, row[secLen:klen-2])
                }
            }
        } else {
            if(colname == table.Keyname){
                keys = append(keys, []byte(data))
            } else {
                keys = append(keys, table.GetPkey(colname, []byte(data))...)
            }
        }
    }
    return keys
}
