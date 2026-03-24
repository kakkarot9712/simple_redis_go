package credis

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
)

func WithRDBDir(dir string) ConfigOption {
	return func(r *config) {
		r.rdbDir = dir
	}
}

func WithRDBFileName(fileName string) ConfigOption {
	return func(r *config) {
		r.rdbFileName = fileName
	}
}

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
	Restore(str KVStore)
	Version() int
	Error() error
	GetAuxField(key string) string
}

type StoreProvider interface {
	Keys() iter.Seq[string]
	Set(key string, data Token, expInMs *uint64)
}

func (cfg *rdbStore) string() string {
	strLen := cfg.length()
	if strLen == -1 {
		// Special encoding
		b, err := cfg.reader.ReadByte()
		if err != nil {
			cfg.err = err
			return ""
		}
		switch b & 63 { // 0b00111111
		case 0:
			// 8 bit integer
			num, err := cfg.reader.ReadByte()
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", num)
		case 1:
			// 16 bit integer
			numBytes := make([]byte, 2)
			_, err := cfg.reader.Read(numBytes)
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", binary.BigEndian.Uint16(numBytes))
		case 2:
			// 32 bit integer
			numBytes := make([]byte, 4)
			_, err := cfg.reader.Read(numBytes)
			if err != nil {
				cfg.err = err
				return ""
			}
			return fmt.Sprintf("%v", binary.BigEndian.Uint32(numBytes))
		default:
			cfg.err = fmt.Errorf("unsupported encoding format")
			return ""
		}
	}
	if cfg.err != nil {
		return ""
	}
	strBytes := make([]byte, strLen)
	_, err := cfg.reader.Read(strBytes)
	if err != nil {
		cfg.err = err
		return ""
	}
	return string(strBytes)
}

func (cfg *rdbStore) length() int {
	length := 0
	reader := cfg.reader
	b, err := reader.ReadByte()
	if err != nil {
		cfg.err = err
		return 0
	}
	switch b >> 6 {
	case 0b00:
		length = int(b)
	case 0b01:
		nextByte, err := reader.ReadByte()
		if err != nil {
			cfg.err = err
			return 0
		}
		lenBytes := make([]byte, 2)
		lenBytes[0] = b & 63 // 0b00111111
		lenBytes[1] = nextByte
		length = int(binary.LittleEndian.Uint16(lenBytes))
	case 0b10:
		lenBytes := make([]byte, 4)
		length = int(binary.BigEndian.Uint32(lenBytes))
	case 0b11:
		length = -1
		reader.UnreadByte()
	}
	return length
}

func NewRDB(dir string, dbfilename string) RDBStore {
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

func (cfg *rdbStore) Restore(str KVStore) {
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
								NewToken(BULK_STRING, value),
								&expTime,
							)
						}
					} else {
						str.Set(key, NewToken(BULK_STRING, value), nil)
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
