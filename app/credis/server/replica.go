package server

import (
	"github.com/codecrafters-io/redis-starter-go/app/credis"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
)

func (srv *Server) AddToReplicaGroup(conn credis.Conn) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.replicas[conn.Id] = &conn
}

func (srv *Server) PropagateToReplicaGroup(tokens ...tokens.Token) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	for _, repl := range srv.replicas {
		enc := encoder.New()
		enc.Array(tokens...)
		enc.Commit()
		repl.Conn.Write(enc.Bytes())
	}
}

func (srv *Server) RemoveFromReplicaLGroup(id string) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	delete(srv.replicas, id)
}

func (srv *Server) IsPartOfReplicaGroup(id string) bool {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	return srv.replicas[id] != nil
}
