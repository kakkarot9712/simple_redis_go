package store

import (
	"fmt"
	"iter"
	"maps"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/credis/helpers"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type Value struct {
	exists    bool
	data      tokens.Token
	createdAt time.Time
	exp       *time.Time
}

type BaseStore interface {
	ID() string
	Error() error
	Get(key string) tokens.Token
	Set(key string, data tokens.Token, exp *time.Time)
	Keys() iter.Seq[string]
	Update(key string, data tokens.Token)
}

type store struct {
	id    string
	mu    sync.RWMutex
	err   error
	store map[string]Value
}

func New() BaseStore {
	return &store{
		id:    helpers.GenerateString(6),
		store: make(map[string]Value),
	}
}

func (s *store) ID() string {
	return s.id
}

func (s *store) Error() error {
	return s.err
}

func (s *store) Get(key string) tokens.Token {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val := s.store[key]
	if !val.exists {
		return tokens.New(types.BULK_STRING, "")
	}
	if val.exp != nil {
		// Value with expiry
		if time.Until(*val.exp) < 0 {
			// value is expired
			delete(s.store, key)
			return tokens.New(types.BULK_STRING, "")
		}

	}
	return s.store[key].data
}

func (s *store) Set(key string, data tokens.Token, exp *time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val := Value{
		data:      data,
		createdAt: time.Now(),
		exists:    true,
	}
	if exp != nil {
		val.exp = exp
	}
	s.store[key] = val
}

func (s *store) Update(key string, data tokens.Token) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val := s.store[key]
	if !val.exists {
		s.err = fmt.Errorf("can not update non existent key: %v", key)
		return
	}
	val.data = data
	s.store[key] = val
}

func (s *store) Keys() iter.Seq[string] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return maps.Keys(s.store)
}
