package main

import (
    "fmt"
    "github.com/vanshjangir/xdb/database"
)

func main(){
    fmt.Println("Starting...")
    var db database.Xdb
    db.Init("first")
    db.BeginTxn()
    database.Parse(&db, "create table vansh (sno INT(100), name INT(100), age VARCHAR(100))")
    database.Parse(&db, "insert into vansh (sno, name, age) values(05, vansh, 205)")
    database.Parse(&db, "insert into vansh (sno, name, age) values(15, devansh, 215)")
    database.Parse(&db, "insert into vansh (sno, name, age) values(25, harshit, 225)")
    database.Parse(&db, "insert into vansh (sno, name, age) values(35, jatin, 235)")
    database.Parse(&db, "insert into vansh (sno, name, age) values(45, arman, 245)")
    database.Parse(&db, "insert into vansh (sno, name, age) values(55, naman, 255)")
    database.Parse(&db, "select * from vansh")
    db.CommitTxn()
}
