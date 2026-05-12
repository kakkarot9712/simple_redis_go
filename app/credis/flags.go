package credis

import "flag"

type Flags struct {
	Port      int
	Host      string
	ReplicaOf string
	Dir       string
	RDB       struct {
		FileName string
	}
	AOF struct {
		Enabled       bool
		FileName      string
		SyncFrequency AOFSyncFreq
		Dir           string
	}
}

func NewFlags() *Flags {
	return &Flags{}
}

func (c *Flags) Parse() error {
	port := flag.Int("port", 6379, "Port number")
	replicaOf := flag.String("replicaof", "", "Replica URL")
	dir := flag.String("dir", "", "RDB File Directory")
	rdbFileNme := flag.String("dbfilename", "", "RDB File Name")
	appendOnly := flag.String("appendonly", "no", "Enable AOF Persistance")
	appenddirname := flag.String("appenddirname", "appendonlydir", "AOF Directory")
	appendfilename := flag.String("appendfilename", "appendonly.aof", "AOF File name")
	appendFsync := flag.String("appendfsync", "everysec", "AOF Sync Frequency")
	flag.Parse()

	c.Port = *port
	c.ReplicaOf = *replicaOf
	c.Dir = *dir
	c.RDB.FileName = *rdbFileNme
	if appenddirname != nil && *appenddirname != "" {
		c.AOF.Dir = *appenddirname
	}
	c.AOF.Enabled = *appendOnly == "yes"
	c.AOF.FileName = *appendfilename
	c.AOF.SyncFrequency = SyncFreq(*appendFsync)
	return nil
}
