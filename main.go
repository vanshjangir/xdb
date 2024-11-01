package main

import (
    "fmt"
    "bufio"
    "os"
    "github.com/vanshjangir/xdb/database"
)

func main(){
    fmt.Println("Starting...")
    var db *database.Xdb = new(database.Xdb)
    db.Init("first")
    for {
        fmt.Printf("\n[xdb]:: ")
        scanner := bufio.NewScanner(os.Stdin)
        scanner.Scan()
        text := scanner.Text()
        database.Parse(db, text)
    }
}
