package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	activeConfig := proccessArgs()
	storedKeys := loadRedisDB(activeConfig[DIR], activeConfig[DBFILENAME])
	if infoMap[REPLICATION]["role"] == "slave" {
		replicaConfig := strings.Split(activeConfig[ReplicaOf], " ")
		url := replicaConfig[0] + ":" + replicaConfig[1]
		conn, err := net.Dial("tcp", url)
		if err != nil {
			fmt.Println("Failed to connect to master", url)
			os.Exit(1)
		}
		buff := make([]byte, 512)
		// Perform Handshak process
		conn.Write(Encode([]string{"PING"}, ARRAYS))
		for {
			size, err := conn.Read(buff)
			if err != nil {
				fmt.Println("[HANDSHAKE] Error reading buffer: ", err.Error())
				os.Exit(1)
			}
			if size == 0 {
				continue
			}
			fmt.Println(string(buff[:size]))
			break
		}
		go handleConn(conn, &activeConfig, &storedKeys, &infoMap)
	}
	l, err := net.Listen("tcp", "0.0.0.0:"+activeConfig[PORT])
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
