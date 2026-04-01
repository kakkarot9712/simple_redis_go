package credis

import (
	"fmt"
	"sync"
	"time"
)

type Stream interface {
	CreateOrUpdateStream(key string, values []KeyValue, opts ...AddStreamOpts) (string, error)
	IsStreamKey(key string) bool
}

type streamStore struct {
	mu        sync.RWMutex
	store     map[string]map[int]map[int][]KeyValue
	ids       map[string][]int
	sequences map[string]map[int][]int
	lastId    int
	lastSeq   int
}

type KeyValue struct {
	Key    string
	Value  string
	Exists bool
}

func NewStream() Stream {
	return &streamStore{
		store:     make(map[string]map[int]map[int][]KeyValue),
		sequences: make(map[string]map[int][]int),
		ids:       make(map[string][]int),
		// store: map[string]map[string][]KeyValue{
		// 	"123132121212": {
		// 		"1": []KeyValue{
		// 			{"", ""},
		// 		},
		// 	},
		// },
		// ids: []string{
		// 	"123132121212",
		// },
		// sequences: map[string][]string{
		// 	"123132121212": {
		// 		"1",
		// 	},
		// },
	}
}

type addStreamOpts struct {
	id  *int
	seq *int
}

type AddStreamOpts func(opts *addStreamOpts)

func WithPredefinedId(id int) AddStreamOpts {
	return func(opts *addStreamOpts) {
		opts.id = &id
	}
}

func WithPredefinedIdAndSequence(id int, seq int) AddStreamOpts {
	return func(opts *addStreamOpts) {
		opts.id = &id
		opts.seq = &seq
	}
}

func (s *streamStore) CreateOrUpdateStream(key string, values []KeyValue, opts ...AddStreamOpts) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	options := addStreamOpts{}
	for _, opt := range opts {
		opt(&options)
	}
	var id int
	var seq int
	if options.id != nil {
		id = *options.id
	} else {
		// TODO: Generate new Id
		if s.lastId == 0 && s.lastSeq == 0 {
			id = int(time.Now().UnixMilli())
		} else {
			id = s.lastId + 1
		}
	}
	if options.seq != nil {
		seq = *options.seq
	} else {
		if id == s.lastId {
			seq = s.lastSeq + 1
		}
	}
	if id == 0 && seq == 0 {
		return "", &ErrInvalidStreamId{}
	}

	if id < s.lastId {
		// TODO: Validate
		return "", &ErrIdLessThenStreamTop{}
	}

	if s.lastId == id && s.lastSeq >= seq {
		return "", &ErrIdLessThenStreamTop{}
	}

	// Add or update stream
	s.ids[key] = append(s.ids[key], id)
	if s.sequences[key] == nil {
		s.sequences[key] = make(map[int][]int)
	}
	s.sequences[key][id] = append(s.sequences[key][id], seq)
	if s.store[key] == nil {
		s.store[key] = make(map[int]map[int][]KeyValue)
	}
	if s.store[key][id] == nil {
		s.store[key][id] = make(map[int][]KeyValue)
	}
	for _, kv := range values {
		s.store[key][id][seq] = append(s.store[key][id][seq], KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}
	s.lastId = id
	s.lastSeq = seq
	return fmt.Sprintf("%v-%v", id, seq), nil
}

func (s *streamStore) IsStreamKey(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	streams := s.ids[key]
	return len(streams) > 0
}
