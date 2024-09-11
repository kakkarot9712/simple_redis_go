package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"slices"
)

var activeConfig = map[config]string{"dir": "/tmp/redis-files", "dbfilename": "dump.rdb"}

func main() {
	dirIndex := slices.Index(os.Args, "--dir")
	fileNameIndex := slices.Index(os.Args, "--dbfilename")
	if dirIndex != -1 {
		if len(os.Args) < dirIndex+2 {
			log.Fatal("--dir requires path as argument!")
		}
		activeConfig[DIR] = os.Args[dirIndex+1]
	}
	if fileNameIndex != -1 {
		if len(os.Args) < fileNameIndex+2 {
			log.Fatal("--dbfilename requires name as argument!")
		}
		activeConfig[DBFILENAME] = os.Args[fileNameIndex+1]
	}
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn, &activeConfig)
	}
}
