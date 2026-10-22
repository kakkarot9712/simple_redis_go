package credis

import (
	"math/bits"
	"sync"
	"time"
)

type BitmapStore interface {
	Set(key string, offset int, flag byte) (byte, error)
	Get(key string, offset int) (byte, error)
	GetStr(key string) string
	StrLen(key string) int
	BitCount(key string, startIndex *int64, endIndex *int64) int
	OpAnd(dest string, key1 string, key2 string) int
	OpOr(dest string, key1 string, key2 string) int
}

type bitmapStore struct {
	// key [string] -> value [string/bitfield]
	_map   sync.Map
	_kvstr KVStore
}

func NewBitmapStore(kvs KVStore) BitmapStore {
	return &bitmapStore{
		_map:   sync.Map{},
		_kvstr: kvs,
	}
}

func (bmap *bitmapStore) Set(key string, offset int, flag byte) (byte, error) {
	if flag > 1 {
		return 0, &ErrInavlidFlagForBitmapOffset{
			val: flag,
		}
	}
	mapVal, _ := bmap._map.LoadOrStore(key, string([]byte{0}))
	bitfield := []byte(mapVal.(string))
	byteLoc := offset / 8
	bitLoc := offset % 8
	for len(bitfield) <= byteLoc {
		bitfield = append(bitfield, 0)
	}
	targetByte := bitfield[byteLoc]
	updated, orig := SetBit(targetByte, byte(bitLoc), flag)
	bitfield[byteLoc] = updated
	bmap._map.Store(key, string(bitfield))
	return orig, nil
}

func (bmap *bitmapStore) Get(key string, offset int) (byte, error) {
	var mapVal any
	var ok bool
	if mapVal, ok = bmap._map.Load(key); !ok {
		// Find if it is in regular KV store
		val := bmap._kvstr.Get(key, time.Now())
		if !val.IsString() {
			return 0, &ErrKeyNotExists{
				key: key,
			}
		}
		mapVal = val.Literal
	}
	bitfield := []byte(mapVal.(string))
	byteLoc := offset / 8
	bitLoc := offset % 8
	if len(bitfield) <= byteLoc {
		return 0, &ErrBitmapOffsetOutofBound{
			key:    key,
			offset: offset,
		}
	}
	b := bitfield[byteLoc]
	orig := GetBit(b, byte(bitLoc))
	return orig, nil
}

func (bmap *bitmapStore) GetStr(key string) string {
	val, ok := bmap._map.Load(key)
	if !ok {
		return ""
	}
	return val.(string)
}

func (bmap *bitmapStore) StrLen(key string) int {
	val := bmap.GetStr(key)
	return len(val)
}

func (bmap *bitmapStore) BitCount(key string, startIndex *int64, endIndex *int64) int {
	val := []byte(bmap.GetStr(key))
	countOnes := 0
	start := 0
	end := 0
	length := len(val)
	if length == 0 {
		return 0
	}
	if startIndex == nil {
		end = length
	} else {
		start = int(*startIndex)
		end = int(*endIndex) + 1
	}
	if start > end || start >= length {
		return 0
	}
	if end > length {
		end = length
	}
	for _, b := range val[start:end] {
		countOnes += bits.OnesCount8(uint8(b))
	}
	return countOnes
}

func (bmap *bitmapStore) OpAnd(dest string, key1 string, key2 string) int {
	bf1 := []byte(bmap.GetStr(key1))
	bf2 := []byte(bmap.GetStr(key2))
	bf1Len := len(bf1)
	bf2Len := len(bf2)
	maxLen := max(bf1Len, bf2Len)
	destVal := make([]byte, maxLen)
	for i := range maxLen {
		var op1, op2 byte
		if i >= bf1Len {
			op1 = 0
		} else {
			op1 = bf1[i]
		}
		if i >= bf2Len {
			op2 = 0
		} else {
			op2 = bf2[i]
		}
		destVal[i] = op1 & op2
	}
	bmap._map.Store(dest, string(destVal))
	return maxLen
}

func (bmap *bitmapStore) OpOr(dest string, key1 string, key2 string) int {
	bf1 := []byte(bmap.GetStr(key1))
	bf2 := []byte(bmap.GetStr(key2))
	bf1Len := len(bf1)
	bf2Len := len(bf2)
	maxLen := max(bf1Len, bf2Len)
	destVal := make([]byte, maxLen)
	for i := range maxLen {
		var op1, op2 byte
		if i >= bf1Len {
			op1 = 0
		} else {
			op1 = bf1[i]
		}
		if i >= bf2Len {
			op2 = 0
		} else {
			op2 = bf2[i]
		}
		destVal[i] = op1 | op2
	}
	bmap._map.Store(dest, string(destVal))
	return maxLen
}
