package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/redis-starter-go/app/credis"
)

func main() {
	flags := credis.NewFlags()
	err := flags.Parse()
	if err != nil {
		log.Fatal(err)
	}
	deps := credis.BuildDeps(flags)
	deps.Initialize()
	h := credis.NewHub(deps.ListStore, deps.Watcher, deps.ReplicaManager)
	srv := credis.New(h, flags)
	if deps.Info.IsSlave() {
		go srv.StartReplica(flags, deps)
	}
	exec := credis.NewExec(deps)

	// Setup middlewares
	exec.Use(credis.AuthMiddleware)
	exec.Use(credis.ReplicaCommandGuard)
	exec.Use(credis.SubscriptionCommandGuardMiddleware)
	exec.Use(credis.WatchCommandGuardMiddleware)
	exec.Use(credis.TransactionMiddleware)

	exec.Use(credis.ExecutorMiddleware)

	exec.Use(credis.AOFWriterMiddleware)
	exec.Use(credis.SubListenerMiddleware)

	go h.Start(exec)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)
	go func() {
		<-c
		fmt.Println("Shutting down Gracefully")
		srv.Hub().Shutdown()
		deps.ReplicaManager.Stop()
		srv.Shutdown()
		os.Exit(0)
	}()
	err = srv.StartMaster()
	// defer srv.Shutdown()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
