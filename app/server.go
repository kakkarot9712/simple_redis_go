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
	activeReplicaConn := []RedisConn{}

	l, err := net.Listen("tcp", "0.0.0.0:"+activeConfig[PORT])
	if err != nil {
		fmt.Println("Failed to bind to port", activeConfig[PORT])
		os.Exit(1)
	}

	if infoMap[REPLICATION]["role"] == "slave" {
		replicaConfig := strings.Split(activeConfig[ReplicaOf], " ")
		url := replicaConfig[0] + ":" + replicaConfig[1]
		go handleReplicaConnection(url, activeConfig[PORT], &storedKeys)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		if infoMap[REPLICATION]["role"] == "slave" {
			go handleConn(RedisConn{conn}, &activeConfig, &storedKeys, &infoMap, nil)
		} else {
			go handleConn(RedisConn{conn}, &activeConfig, &storedKeys, &infoMap, &activeReplicaConn)
		}
	}
}
