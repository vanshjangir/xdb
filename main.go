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

        fmt.Printf("%v -> ", name)
        scanner := bufio.NewScanner(os.Stdin)
        scanner.Scan()
        text := scanner.Text()
        database.Parse(&db, text)
    }
}
