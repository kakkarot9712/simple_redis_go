package credis

import (
	"fmt"
	"sync"
	"time"
)

type Stream interface {
	CreateOrUpdateStream(key string, values []KeyValue, opts ...AddStreamOpts) (string, error)
	IsStreamKey(key string) bool
	GetStream(key string, start string, startInclusive bool, end string) (uint8, []Token, error)
	ReadSingleStream(ids []string, startInclusive bool) (uint8, []Token, error)
	GetLatestStreamId() string
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
		// store: map[string]map[int]map[int][]KeyValue{
		// 	"key": {
		// 		1232121: {
		// 			1: []KeyValue{
		// 				{"", ""},
		// 			}
		// 		},
		// 	},
		// },
		// ids: {
		// 		"key": []int{id1, id2}
		// }
		// sequences: map[string]map[int][]int{
		// 	"key": {
		// 		id: [seq1, seq2]
		//	}
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

func (s *streamStore) GetLatestStreamId() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("%v-%v", s.lastId, s.lastSeq)
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
	lastId := len(s.ids) - 1
	if !(lastId != -1 &&
		len(s.ids[key]) > 0 &&
		s.ids[key][lastId] == id) {
		s.ids[key] = append(s.ids[key], id)
	}

	if s.sequences[key] == nil {
		s.sequences[key] = make(map[int][]int)
	}
	lastSeq := len(s.sequences[key][id]) - 1
	if !(lastSeq != -1 &&
		len(s.sequences[key][id]) > 0 &&
		s.sequences[key][id][lastSeq] == seq) {
		s.sequences[key][id] = append(s.sequences[key][id], seq)
	}
	if s.store[key] == nil {
		s.store[key] = make(map[int]map[int][]KeyValue)
	}
	if s.store[key][id] == nil {
		s.store[key][id] = make(map[int][]KeyValue)
	}
	for _, kv := range values {
		s.store[key][id][seq] = append(s.store[key][id][seq], KeyValue{
			Key:    kv.Key,
			Value:  kv.Value,
			Exists: true,
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

func (s *streamStore) getStream(key string,
	start string,
	startInclusive bool,
	end string,
) (uint8, []Token, error) {
	var stream []Token
	var startTs int64
	var startSeq *int64

	var endTs int64
	var endSeq *int64

	var err error
	var found uint8

	// TODO: What if stream does not exists yet?
	if start == "-" {
		startTs = int64(s.ids[key][0])
		first := int64(s.sequences[key][int(startTs)][0])
		startSeq = &first
	} else {
		startTs, startSeq, err = GetTsAndSeq(start)
		if err != nil {
			return found, []Token{}, err
		}
	}

	if end == "+" {
		endTs = int64(s.ids[key][len(s.ids[key])-1])
		last := int64(s.sequences[key][int(endTs)][len(s.sequences[key][int(endTs)])-1])
		endSeq = &last
	} else {
		endTs, endSeq, err = GetTsAndSeq(end)
		if err != nil {
			return found, []Token{}, err
		}
	}

	var ids []int
	sequencies := map[int][]int{}
	for _, id := range s.ids[key] {
		if id >= int(startTs) && id <= int(endTs) {
			ids = append(ids, id)
			seqs := s.sequences[key][id]
			start := int64(seqs[0])
			end := int64(seqs[len(seqs)-1])
			if startSeq != nil && id == int(startTs) {
				start = *startSeq
			}
			if endSeq != nil && id == int(endTs) {
				end = *endSeq
			}
			sequencies[id] = make([]int, 0)
			for _, s := range seqs {
				if (s > int(start) || (startInclusive && int(start) == s)) && s <= int(end) {
					sequencies[id] = append(sequencies[id], s)
				}
				if s > int(end) {
					break
				}
			}
		}
		if id > int(endTs) {
			break
		}
	}

	for _, id := range ids {
		seqs := sequencies[id]
		for _, seq := range seqs {
			found = 1
			kvPairs := s.store[key][id][seq]
			kvTokens := []Token{}
			for _, kv := range kvPairs {
				kvTokens = append(kvTokens,
					NewToken(BULK_STRING, kv.Key),
					NewToken(BULK_STRING, kv.Value),
				)
			}
			kvs := NewToken(ARRAY, []Token{
				NewToken(BULK_STRING, fmt.Sprintf("%v-%v", id, seq)),
				NewToken(ARRAY, kvTokens),
			})
			stream = append(stream, kvs)
		}
	}
	return found, stream, nil
}

func (s *streamStore) GetStream(
	key string,
	start string,
	startIncluded bool,
	end string,
) (uint8, []Token, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getStream(key, start, startIncluded, end)
}

func (s *streamStore) ReadSingleStream(ids []string, startInclusive bool) (uint8, []Token, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var found uint8
	if len(ids)%2 != 0 {
		return found, nil, fmt.Errorf("ERR invalid args passed for XREAD command.")
	}
	center := len(ids) / 2
	streams := []Token{}
	for i := range center {
		streamKey := ids[i]
		streamId := ids[i+center]
		dataFound, s, err := s.getStream(streamKey, streamId, startInclusive, "+")
		if err != nil {
			return found, nil, err
		}
		found |= dataFound
		// TODO: What will happen if key does not exists?
		// TODO: Improve logic, repetetive mutex aquires will slow this down.

		stream := []Token{
			NewToken(BULK_STRING, streamKey),
			NewToken(ARRAY, s),
		}
		streams = append(streams, NewToken(ARRAY, stream))
	}
	return found, streams, nil
}
