package credis

import (
	"log"
)

type deps struct {
	KVStore        KVStore
	StreamStore    Stream
	ListStore      ListStore[string]
	SubManager     Subscription
	AOF            AOF
	RDB            RDBStore
	Info           *serverInfo
	Auth           *Auth
	Watcher        Watcher
	SortedSet      SortedSet
	Cfg            *Config
	ReplicaManager ReplicaManager
}

func BuildDeps(flgs *Flags) *deps {
	d := &deps{
		KVStore:        NewStore(),
		StreamStore:    NewStream(),
		RDB:            NewRDB(flgs.Dir, flgs.RDB.FileName),
		ListStore:      NewListStore[string](),
		SubManager:     NewSubscriptionManager(),
		Info:           NewInfo(),
		Auth:           NewAuth(),
		Cfg:            NewCfg(flgs),
		Watcher:        NewWatcher(),
		AOF:            NewAOFConfig(flgs.Dir, flgs.AOF.Dir, flgs.AOF.FileName, flgs.AOF.SyncFrequency, flgs.AOF.Enabled),
		SortedSet:      NewSortedSet(),
		ReplicaManager: NewReplManager(),
	}
	if flgs.ReplicaOf != "" {
		d.Info.set("replication", "role", "slave")
	} else {
		d.Info.set("replication", "role", "master")
		d.Info.set("replication", "master_repl_offset", "0")
		d.Info.set("replication", "master_replid", GenerateString(40))
	}
	if flgs.AOF.Enabled {
		d.Info.set("persistence", "aof_enabled", "1")
	} else {
		d.Info.set("persistence", "aof_enabled", "0")
	}
	return d
}

func (d *deps) Initialize() {
	// Restore RDB if exists
	if d.RDB.GetRDBFileName() != "" && d.RDB.GetRDBDir() != "" {
		d.RDB.Load()
		if d.RDB.Error() != nil {
			log.Printf("RDB Restore aborted: %v", d.RDB.Error().Error())
		} else {
			d.RDB.Restore(d.KVStore)
		}
	}

	// Restore from AOF if exists
	d.AOF.Initialize(d)

	// Start replica handler
	go d.ReplicaManager.Start()

	// Set server info
}
