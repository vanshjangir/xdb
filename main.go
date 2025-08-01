package main

import (
    "fmt"
    "bufio"
    "os"
    "github.com/vanshjangir/xdb/database"
	"github.com/common-nighthawk/go-figure"
)

func main(){
	figure.NewFigure("XDB", "doom", true).Print()
    fmt.Printf("\n\n")
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
