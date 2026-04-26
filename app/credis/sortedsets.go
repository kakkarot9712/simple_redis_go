package credis

import (
	"fmt"
	"sync"
)

// const MAX_DEPTH = 32

type SetNode struct {
	value string
	score float64
	next  *SetNode
}

type setVal struct {
	score  float64
	exists bool
}

type SortedSet interface {
	Rank(key string, value string) int
	Range(key string, start int64, end int64) []string
	Add(key string, value string, score float64) uint64
	Cardinality(key string) int
	Remove(key string, value string) int
	Get(key string, value string) *string
}

type skipList struct {
	mu     sync.RWMutex
	hasmap map[string]map[string]setVal
	roots  map[string]*SetNode
	length uint64
}

func NewSortedSet() SortedSet {
	return &skipList{
		hasmap: make(map[string]map[string]setVal),
		roots:  make(map[string]*SetNode),
	}
}

func (s *skipList) Add(key string, value string, score float64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasmap[key] == nil {
		s.hasmap[key] = make(map[string]setVal)
	}
	elemtsAdded := 1
	if s.hasmap[key][value].exists {
		elemtsAdded = 0
	}

	// TODO: What to do if element is duplicate? remove from list???
	s.hasmap[key][value] = setVal{
		score:  score,
		exists: true,
	}
	// TODO: Insert into express lanes
	node := SetNode{
		value: value,
		score: score,
	}
	root := s.roots[key]
	if root == nil {
		s.roots[key] = &node
		root = s.roots[key]
	} else {
		current := root
		var prev *SetNode
		for {
			if score < current.score || (score == current.score && value < current.value) {
				break
			} else {
				prev = current
			}
			if current.next == nil {
				break
			}
			current = current.next
		}
		if prev == nil {
			if current.score > score || (score == current.score && value < current.value) {
				node.next = current
				s.roots[key] = &node
			} else {
				s.roots[key].next = &node
			}
		} else {
			temp := prev.next
			node.next = temp
			prev.next = &node
		}
	}
	s.length++
	return uint64(elemtsAdded)
}

func (s *skipList) Rank(key string, value string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rank := -1
	root := s.roots[key]
	if root == nil {
		return rank
	}
	current := root
	found := false
	for current != nil {
		rank++
		if current.value == value {
			found = true
			break
		}
		current = current.next
	}
	if !found {
		rank = -1
	}
	return rank
}

func (s *skipList) Range(key string, start int64, end int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	elems := []string{}
	root := s.roots[key]
	if root == nil {
		return elems
	}
	startInd := start
	endInd := end
	lastInd := int64(len(s.hasmap[key]))
	if startInd < 0 {
		startInd = lastInd + startInd
	}
	if endInd < 0 {
		endInd = lastInd + endInd
	}
	if endInd > lastInd {
		endInd = lastInd - 1
	}
	if startInd < 0 {
		startInd = 0
	}
	current := root
	currentInd := 0
	for {
		if currentInd >= int(startInd) && currentInd <= int(endInd) {
			elems = append(elems, current.value)
		}
		if currentInd > int(endInd) || current.next == nil {
			break
		}
		current = current.next
		currentInd++
	}
	return elems
}

func (s *skipList) Cardinality(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.hasmap[key])
}

func (s *skipList) Get(key string, value string) *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasmap[key][value].exists {
		return nil
	}
	val := fmt.Sprintf("%v", s.hasmap[key][value].score)
	return &val
}

func (s *skipList) Remove(key string, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasmap[key][value].exists {
		return 0
	}
	delete(s.hasmap[key], value)

	// Remove from linked list
	current := s.roots[key]
	var prev *SetNode
	for {
		if current.value == value || current.next == nil {
			break
		}
		prev = current
		current = current.next
	}
	if prev == nil {
		s.roots[key] = current.next
	} else {
		prev.next = current.next
	}
	return 1
}
