package credis

import (
	"sync"
)

type ListStore[T any] interface {
	Push(key string, values []T) int
	Get(key string, start int64, end int64) []T
	Prepend(key string, values []T) int
	Len(key string) int
	Pop(key string) *T
}

type list[T any] struct {
	// key -> List of values
	data map[string]*LinkedList[T]
	mu   sync.RWMutex
}

func NewListStore[T any]() ListStore[T] {
	return &list[T]{
		data: make(map[string]*LinkedList[T]),
	}
}

func (l *list[T]) Push(key string, values []T) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.data[key] == nil {
		l.data[key] = NewList[T]()
	}
	for _, d := range values {
		l.data[key].Append(d)
	}
	return l.data[key].Len()
}

func (l *list[T]) Get(key string, start int64, end int64) []T {
	elements := []T{}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.data[key] == nil {
		return elements
	}
	startInd := start
	endInd := end
	lastInd := int64(l.data[key].Len())
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
	if startInd >= 0 && endInd >= 0 && startInd < endInd && lastInd > 0 && lastInd > startInd {
		elements = l.data[key].Get(startInd, endInd)
	}
	return elements
}

func (l *list[T]) Prepend(key string, values []T) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.data[key] == nil {
		l.data[key] = NewList[T]()
	}
	for _, d := range values {
		l.data[key].Prepend(d)
	}
	return l.data[key].Len()
}

func (l *list[T]) Len(key string) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.data[key] == nil {
		return 0
	}
	return l.data[key].Len()
}

func (l *list[T]) Pop(key string) *T {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.data[key] == nil {
		return nil
	}
	return l.data[key].Pop()
}
