package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
	"github.com/codecrafters-io/redis-starter-go/app/credis/store"
)

const (
	MAGIC_STRING = "REDIS"
)

const (
	EOF          = 0xFF
	EXPIRETIME   = 0xFD
	EXPIRETIMEMS = 0xFC
	SELECTDB     = 0xFE
	AUX          = 0xFA
	RESIZEDB     = 0xFB
)

const (
	UNSET = iota - 1
	STRING_VALUE
	LIST_VALUE
	SET_VALUE
	SORTED_SET_VALUE
	HASHMAP_VALUE
	ZIPMAP_VALUE
	ZIPLIST_VALUE
	INTSET_VALUE
	SORTED_SET_ZIPLIST_VALUE
	HASHMAP_ZIPLIST_VALUE
	LIST_QUICKLIST_VALUE
)

type rdbStore struct {
	dir                 string
	dbFilename          string
	reader              *bufio.Reader
	err                 error
	version             int
	auxFields           map[string]string
	hasTableSize        int
	expiryHashTableSize int
}

type RDBStore interface {
	GetRDBDir() string
	GetRDBFileName() string
	Load()
	Restore(str store.BaseStore)
	Version() int
	Error() error
	GetAuxField(key string) string
}

type StoreProvider interface {
	Keys() iter.Seq[string]
	Set(key string, data tokens.Token, expInMs *uint64)
}

func New(dir string, dbfilename string) RDBStore {
	return &rdbStore{
		dir:        dir,
		dbFilename: dbfilename,
		auxFields:  map[string]string{},
	}
}

func (r *rdbStore) Error() error {
	return r.err
}

func (r *rdbStore) Load() {
	buff, err := os.ReadFile(path.Join(r.dir, r.dbFilename))
	if err != nil {
		r.err = err
		return
	}

	if buff[len(buff)-1] != EOF {
		// Verify CRC
		contentBuffer := buff[:len(buff)-8]
		expectedCrc := binary.LittleEndian.Uint64(buff[len(buff)-8:])
		currentCrc := ChecksumJones(contentBuffer)
		if currentCrc != expectedCrc {
			r.err = fmt.Errorf("rdb checksum verification failed")
			return
		}
	}

	r.reader = bufio.NewReader(bytes.NewBuffer(buff))
}

func (r *rdbStore) GetRDBFileName() string {
	return r.dbFilename
}

func (r *rdbStore) GetRDBDir() string {
	return r.dir
}

func (r *rdbStore) Version() int {
	return r.version
}

func (cfg *rdbStore) Restore(str store.BaseStore) {
	if cfg.reader == nil {
		cfg.err = fmt.Errorf("RDB file not loaded! restore skipped")
		return
	}
	reader := cfg.reader
	magicStr := make([]byte, 5)
	_, err := reader.Read(magicStr)
	if err != nil {
		cfg.err = err
		return
	}

	// Check magic string
	if string(magicStr) != MAGIC_STRING {
		cfg.err = fmt.Errorf("not rdb file! restore skipped")
		return
	}

	// Find version
	versionBytes := make([]byte, 4)
	_, err = reader.Read(versionBytes)
	if err != nil {
		cfg.err = err
		return
	}

	version, err := strconv.ParseUint(string(versionBytes), 10, 64)
	if err != nil {
		cfg.err = err
		return
	}
	cfg.version = int(version)
	isEOF := false
	for !isEOF {
		b, err := reader.ReadByte()
		if err != nil {
			cfg.err = err
			return
		}

		switch b {
		case AUX:
			// Auxiliary Data
			key := cfg.string()
			if cfg.err != nil {
				return
			}
			value := cfg.string()
			switch key {
			case "redis-ver", "redis-bits", "ctime", "used-mem":
				cfg.auxFields[key] = value
			default:
				// Ignored
			}
		case EOF:
			isEOF = true

		case SELECTDB:
			// Database selector
			_ = cfg.length()
			var expiry uint64
			key := ""
			value := ""
			valueType := UNSET

			for !isEOF {
				b, err := cfg.reader.ReadByte()
				if err != nil {
					cfg.err = err
					return
				}
				switch b {
				case RESIZEDB:
					cfg.hasTableSize = cfg.length()
					if cfg.err != nil {
						return
					}
					cfg.expiryHashTableSize = cfg.length()
					if cfg.err != nil {
						return
					}
				case EXPIRETIME:
					// Expiry time in seconds
					etBytes := make([]byte, 4)
					_, err := reader.Read(etBytes)
					if err != nil {
						cfg.err = err
						return
					}
					expiry = uint64(binary.LittleEndian.Uint32(etBytes)) * 1000

				case EXPIRETIMEMS:
					// Expiry time in ms
					etBytes := make([]byte, 8)
					_, err := reader.Read(etBytes)
					if err != nil {
						cfg.err = err
						return
					}
					expiry = binary.LittleEndian.Uint64(etBytes)

				case EOF:
					isEOF = true

				case SELECTDB:
					// TODO: DB is changed. maybe do some old db cleanup?
					expiry = 0
					key = ""
					value = ""
					valueType = UNSET

				default:
					// value type check
					if valueType == UNSET {
						if b > 15 {
							cfg.err = fmt.Errorf("invalid value type found")
							return
						}
						valueType = int(b)
					} else if key == "" {
						reader.UnreadByte()
						key = cfg.string()
					} else {
						reader.UnreadByte()
						// Value
						switch valueType {
						case STRING_VALUE:
							value = cfg.string()
						default:
							cfg.err = fmt.Errorf("to be implemented")
							return
						}
					}
				}
				if key != "" && value != "" {
					if expiry != 0 {
						sec := expiry / 1000
						usec := expiry % 1000
						expTime := time.Unix(int64(sec), int64(usec)*1_000_000)
						if time.Until(
							expTime,
						) > 0 {
							str.Set(
								key,
								tokens.New(types.BULK_STRING, value),
								&expTime,
							)
						}
					} else {
						str.Set(key, tokens.New(types.BULK_STRING, value), nil)
					}
					expiry = 0
					key = ""
					value = ""
					valueType = UNSET
				}
			}
		}
	}

}

func (cfg *rdbStore) GetAuxField(key string) string {
	return cfg.auxFields[key]
}
