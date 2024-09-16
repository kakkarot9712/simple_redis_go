package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	activeConfig := proccessArgs()
	storedKeys := loadRedisDB(activeConfig[DIR], activeConfig[DBFILENAME])
	url := "0.0.0.0:" + activeConfig[PORT]
	l, err := net.Listen("tcp", url)
	if err != nil {
		fmt.Println("Failed to bind to port", activeConfig[PORT])
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn, &activeConfig, &storedKeys, &infoMap)
	}
}
