package main

import (
    "fmt"
    "bufio"
    "os"
    "github.com/vanshjangir/xdb/database"
)

func main(){
    fmt.Println("Starting...")
    var db *database.Xdb
    for {
        name := "xdb"
        if db != nil {
            name = db.Name
        }

        txStatus := ""
        if ok := db.TxStatus(); ok {
            txStatus = "*"
        }

        fmt.Printf("%v%v -> ", name, txStatus)
        scanner := bufio.NewScanner(os.Stdin)
        scanner.Scan()
        text := scanner.Text()
        database.Parse(&db, text)
    }
}
