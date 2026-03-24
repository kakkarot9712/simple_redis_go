package credis

import (
	"fmt"
	"iter"
	"maps"
	"sync"
	"time"
)

type Value struct {
	exists    bool
	data      Token
	createdAt time.Time
	exp       *time.Time
}

type KVStore interface {
	ID() string
	Error() error
	Get(key string, currentTime time.Time) Token
	Set(key string, data Token, exp *time.Time)
	Keys() iter.Seq[string]
	Update(key string, data Token)
}

type store struct {
	id    string
	mu    sync.RWMutex
	err   error
	store map[string]Value
}

func NewStore() KVStore {
	return &store{
		id:    GenerateString(6),
		store: make(map[string]Value),
	}
}

func (s *store) ID() string {
	return s.id
}

func (s *store) Error() error {
	return s.err
}

func (s *store) Get(key string, currentTime time.Time) Token {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val := s.store[key]
	if !val.exists {
		return NewToken(BULK_STRING, "")
	}
	if val.exp != nil {
		// Value with expiry
		if currentTime.After(*val.exp) {
			// value is expired
			delete(s.store, key)
			return NewToken(BULK_STRING, "")
		}
	}
	return s.store[key].data
}

func (s *store) Set(key string, data Token, exp *time.Time) {
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

func (s *store) Update(key string, data Token) {
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
