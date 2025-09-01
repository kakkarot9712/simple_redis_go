package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/credis"
	"github.com/codecrafters-io/redis-starter-go/app/credis/server"
)

func main() {
	h := credis.NewHub()
	h.Start()
	port := flag.Int("port", 6379, "Port number")
	replicaOf := flag.String("replicaof", "", "Replica URL")
	rdbDir := flag.String("dir", "", "RDB File Directory")
	rdbFileNme := flag.String("dbfilename", "", "RDB File Name")
	flag.Parse()
	opts := []server.ConfigOption{
		server.WithPort(*port),
	}
	isSlave := false
	if replicaOf != nil && *replicaOf != "" {
		replicaConnInfo := strings.Split(*replicaOf, " ")
		if len(replicaConnInfo) != 2 {
			fmt.Println("Invalid args passed for --replicaof")
			os.Exit(1)
		}
		masterHost := replicaConnInfo[0]
		masterPort, err := strconv.ParseUint(replicaConnInfo[1], 10, 64)
		if err != nil {
			fmt.Println("Invalid args passed for --replicaof")
			os.Exit(1)
		}
		opts = append(opts, server.WithSlave(
			server.WithMasterHost(masterHost),
			server.WithMasterPort(int(masterPort)),
		))
		isSlave = true
	}
	if rdbDir != nil {
		opts = append(opts, server.WithRDBDir(*rdbDir))
	}
	if rdbFileNme != nil {
		opts = append(opts, server.WithRDBFileName(*rdbFileNme))
	}
	srv := server.New(h, opts...)
	if isSlave {
		go srv.StartSlave()
	}
	err := srv.StartMaster()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
