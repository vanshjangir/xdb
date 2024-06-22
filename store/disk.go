package store

import (
	"fmt"
	"os"
    "syscall"
)

func MmapInit(){
    file, err := os.OpenFile("files/xdb", os.O_RDWR, 600)
    if(err != nil){
        fmt.Println("error opening db file", err)
        os.Exit(1)
    }

    defer file.Close()

    fileData, err := syscall.Mmap(
        int(file.Fd()),
        0,
        1000,
        syscall.PROT_READ | syscall.PROT_WRITE,
        syscall.MAP_SHARED,
    )
    if(err != nil){
        fmt.Println("error in syscall mmap", err)
        os.Exit(1)
    }

    fmt.Println(fileData[:1000])
}
