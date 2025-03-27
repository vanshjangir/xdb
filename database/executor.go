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

func Parse(dbp **Xdb, query string){
    cmds := strings.Split(query, " ")
    if(cmds[0] == "exit"){
        os.Exit(0)

    } else if(cmds[0] == "db") {
        parsexdb(dbp, cmds)

    } else if(cmds[0] == "help"){
        fmt.Println()
        fmt.Println("Database commands")
        fmt.Println()
        fmt.Println("db ls                  - List all databases")
        fmt.Println("db use <db_name>       - Use an existing databases")
        fmt.Println("db create <db_name>    - Create a existing databases")
        fmt.Println("db show                - Show all tables in current database")
        fmt.Println("db begin               - Begin a transaction")
        fmt.Println("db commit              - Commit a transaction")
        fmt.Println("db rollback            - Rollback a transaction")
        fmt.Println()
        fmt.Println("Table commands")
        fmt.Println("Same sql commands, not all are supported yet")
        fmt.Println()
        fmt.Println("help                   - List of available commands")

    } else {
        if *dbp == nil {
            fmt.Println("No database initialized")
            return
        }
        parsesql(*dbp, query)
    }
}

func parsesql(db *Xdb, query string){
    if db.tx == nil || db.tx.IsGoing == false {
        fmt.Println("No ongoing transaction")
        return
    }

    stmt, err := sqlparser.Parse(query)
    if(err != nil){
        fmt.Println("Syntax Error in sql query: ", err)
        return
    }

    switch stmt := stmt.(type) {
    case *sqlparser.DDL:
        if(stmt.Action == "create"){
            createStmt(db, stmt)
        }

    case *sqlparser.Insert:
        insertStmt(db, stmt)

    case *sqlparser.Delete:
        deleteStmt(db, stmt)

    case *sqlparser.Update:
        updateStmt(db, stmt)

    case *sqlparser.Select:
        selectStmt(db, stmt)

    }
}

func parsexdb(dbp **Xdb, cmds []string){
    switch cmd := cmds[1]; cmd {
    case "use":
        if *dbp != nil && (*dbp).tx != nil {
            fmt.Println("An ongoing transaction already exists")
            fmt.Print("[rs (rollback previous and start new), c (cancel)] : ")
            
            input := ""
            fmt.Scanf("%v", input)
            if input == "rb" {
                (*dbp).RollbackTxn()
                return
            }
        }

        *dbp = new(Xdb)
        if err := (*dbp).Init(cmds[2]); err != nil {
            fmt.Println(err)
            *dbp = nil
            return
        }
        fmt.Println("Now using database:", cmds[2])

    case "create":
        if err := CreateDatabase(cmds[2]); err != nil {
            fmt.Println(err)
        }

    case "show":
        if dbp == nil {
            fmt.Println("No database in use")
            return
        }

        if (*dbp).tx == nil {
            fmt.Println("No ongoing transaction")
            return
        }

        (*dbp).Select((*dbp).Name + "_meta")

    case "ls":
        ListDB()

    case "begin":
        if dbp == nil {
            fmt.Println("No database selected")
            return
        }
        
        if (*dbp).tx != nil {
            fmt.Println("An ongoing transaction already exists")
            fmt.Print("[rs (rollback previous and start new), c (cancel)] : ")
            
            input := ""
            fmt.Scanf("%v", input)
            if input == "rb" {
                (*dbp).RollbackTxn()
                return
            }
        }

        (*dbp).BeginTxn()
        fmt.Println("New transaction started")

    case "rollback":
        if (*dbp) == nil || (*dbp).tx == nil {
            fmt.Println("No ongoing transaction")
            return
        }
        (*dbp).RollbackTxn()
        fmt.Println("Transaction rolled back")

    case "commit":
        if (*dbp) == nil || (*dbp).tx == nil {
            fmt.Println("No ongoing transaction")
            return
        }
        (*dbp).CommitTxn()
        fmt.Println("Transaction committed")
    }
}

func createStmt(db *Xdb, stmt *sqlparser.DDL){
    var colSize []int
    var columns []string
    for _, col := range stmt.TableSpec.Columns {
        columns = append(columns, col.Name.String())
        if csize, err := strconv.Atoi(string(col.Type.Length.Val)); err != nil {

        } else {
            colSize = append(colSize, csize)
        }
    }

    tableName := stmt.NewName.Name.String()
    if err := db.CreateTable(tableName, columns, colSize);
    err != nil {
        fmt.Println("Error in create table: ", err)
    }
}

func insertStmt(db *Xdb, stmt *sqlparser.Insert){
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

func deleteStmt(db *Xdb, stmt *sqlparser.Delete){
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

func updateStmt(db *Xdb, stmt *sqlparser.Update){
}

func selectStmt(db *Xdb, stmt *sqlparser.Select){
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
