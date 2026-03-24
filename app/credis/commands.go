package credis

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ECHO         = "echo"
	COMMAND      = "command"
	PING         = "ping"
	SET          = "set"
	GET          = "get"
	INCR         = "incr"
	MULTI        = "multi"
	EXEC         = "exec"
	DISCARD      = "discard"
	INFO         = "info"
	REPLCONF     = "replconf"
	PSYNC        = "psync"
	CONFIG       = "config"
	KEYS         = "keys"
	XADD         = "xadd"
	TYPE         = "type"
	RPUSH        = "rpush"
	LRANGE       = "lrange"
	LPUSH        = "lpush"
	LLEN         = "llen"
	LPOP         = "lpop"
	BLPOP        = "blpop"
	WAIT         = "wait"
	SUBSCRIBE    = "subscribe"
	UNSUBSCRIBE  = "unsubscribe"
	PSUBSCRIBE   = "psubscribe"
	PUNSUBSCRIBE = "punsubscribe"
	QUIT         = "quit"
)

type Cmd interface {
	Parse(args ...Token) error
	Execute(e *executor, req Request) []byte
	String() string
}

type GenericSpec struct {
	MinArgs int
	MaxArgs int
}

var commandRegistry = map[string]GenericSpec{
	ECHO: {
		MinArgs: 1,
		MaxArgs: -1,
	},
	PING: {
		MinArgs: 0,
		MaxArgs: 0,
	},
	SET: {
		MinArgs: 2,
		MaxArgs: 4,
	},
	GET: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	INCR: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	INFO: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	REPLCONF: {
		MinArgs: 2,
		MaxArgs: 2,
	},
	PSYNC: {
		MinArgs: 2,
		MaxArgs: 2,
	},
	KEYS: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	CONFIG: {
		MinArgs: 2,
		MaxArgs: 2,
	},
	COMMAND: {
		MinArgs: 1,
		MaxArgs: -1,
	},
	MULTI: {
		MinArgs: 0,
		MaxArgs: 0,
	},
	EXEC: {
		MinArgs: 0,
		MaxArgs: 0,
	},
	DISCARD: {
		MinArgs: 0,
		MaxArgs: 0,
	},
	TYPE: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	XADD: {
		MinArgs: 4,
		MaxArgs: -1,
	},
	RPUSH: {
		MinArgs: 2,
		MaxArgs: -1,
	},
	LRANGE: {
		MinArgs: 3,
		MaxArgs: 3,
	},
	LPUSH: {
		MinArgs: 2,
		MaxArgs: -1,
	},
	LLEN: {
		MinArgs: 1,
		MaxArgs: 1,
	},
	LPOP: {
		MinArgs: 1,
		MaxArgs: 2,
	},
	BLPOP: {
		MinArgs: 1,
		MaxArgs: -1,
	},
	WAIT: {
		MinArgs: 2,
		MaxArgs: 2,
	},
	SUBSCRIBE: {
		MinArgs: 1,
		MaxArgs: 1,
	},
}

func GetGenericSpec(cmd string) GenericSpec {
	return commandRegistry[cmd]
}

type ECHOSpecs struct {
	Data string
}

func (s *ECHOSpecs) String() string {
	return ECHO
}

func (s *ECHOSpecs) Parse(args ...Token) error {
	var data strings.Builder
	for _, arg := range args {
		switch arg.Type {
		case BULK_STRING, SIMPLE_STRING:
			data.WriteString(arg.Literal.(string))
		default:
			return fmt.Errorf("unsupported type as a string conv: %v", arg.Literal)
			// TODO: Maybe need to convert other data to string?
		}
	}
	s.Data = data.String()
	return nil
}

func (s *ECHOSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	enc.BulkString(s.Data)
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type PINGSpecs struct {
}

func (s *PINGSpecs) String() string {
	return PING
}

func (s *PINGSpecs) Parse(args ...Token) error {
	return nil
}

func (s *PINGSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	enc.SimpleString("PONG")
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type CONFIGSpecs struct {
	Action string
	Key    string
}

func (s *CONFIGSpecs) String() string {
	return CONFIG
}

func (spec *CONFIGSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for CONFIG", invalidIndex)
	}
	spec.Action = args[0].Literal.(string)
	spec.Key = args[1].Literal.(string)
	return nil
}

func (s *CONFIGSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	action := s.Action
	switch action {
	case "GET", "get":
		key := s.Key
		switch key {
		case "dir":
			enc.Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, e.rdbConfig.GetRDBDir()),
			)
		case "dbfilename":
			enc.Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, e.rdbConfig.GetRDBFileName()),
			)
		default:
			if hasErr, data := e.parseError(fmt.Errorf("key unsupoorted for command"), enc); hasErr {
				return data
			}
			return nil
		}
	default:
		if hasErr, data := e.parseError(fmt.Errorf("config action unsupported: %v", action), enc); hasErr {
			return data
		}
		return nil
	}
	enc.Commit()
	return enc.Bytes()
}

type GETSpecs struct {
	Key         string
	CurrentTime time.Time
}

func (s *GETSpecs) String() string {
	return GET
}

func (spec *GETSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for XADD", invalidIndex)
	}
	key := args[0].Literal.(string)
	spec.Key = key
	spec.CurrentTime = time.Now()
	return nil
}

func (spec *GETSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	val := e.store.KV.Get(spec.Key, spec.CurrentTime)
	switch val.Type {
	case BULK_STRING, SIMPLE_STRING:
		data := val.Literal.(string)
		enc.BulkString(data)
		if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
			return data
		}
		enc.Commit()
		return enc.Bytes()
	default:
		// TODO: support other type of values
		if hasErr, data := e.parseError(fmt.Errorf("unsupported data as value for GET: %v", val.Literal), enc); hasErr {
			return data
		}
		return nil
	}
}

type INCRSpecs struct {
	Key         string
	CurrentTime time.Time
}

func (s *INCRSpecs) String() string {
	return INCR
}

func (spec *INCRSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for INCR", invalidIndex)
	}
	spec.Key = args[0].Literal.(string)
	spec.CurrentTime = time.Now()
	return nil
}

func (spec *INCRSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	key := spec.Key
	val := e.store.KV.Get(key, spec.CurrentTime)

	// Check if value is integer
	switch val.Type {
	case BULK_STRING, SIMPLE_STRING:
		var updaredNum int
		if val.Literal.(string) == "" {
			// Value does not exists, create one
			updaredNum = 1
			value := NewToken(BULK_STRING, fmt.Sprintf("%v", 1))
			e.store.KV.Set(key, value, nil)
			if hasErr, data := e.parseError(e.store.KV.Error(), enc); hasErr {
				return data
			}
		} else {
			num, err := strconv.ParseInt(val.Literal.(string), 10, 64)
			if err != nil {
				if hasErr, data := e.parseError(&ErrNotInteger{
					data: num,
				}, enc); hasErr {
					return data
				}
				return nil
			}
			updaredNum = int(num) + 1
			updatedValue := NewToken(BULK_STRING, fmt.Sprintf("%v", updaredNum))
			e.store.KV.Update(key, updatedValue)
			if hasErr, data := e.parseError(e.store.KV.Error(), enc); hasErr {
				return data
			}
		}
		enc.Integer(updaredNum)
		if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
			return data
		}
		enc.Commit()
		return enc.Bytes()
	default:
		if hasErr, data := e.parseError(fmt.Errorf("unsupported value for command INCR: %v", val.Literal), enc); hasErr {
			return data
		}
		return nil
	}
}

type INFOSpecs struct {
	Section string
}

func (s *INFOSpecs) String() string {
	return INFO
}

func (spec *INFOSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for INFO", invalidIndex)
	}
	spec.Section = args[0].Literal.(string)
	return nil
}

func (spec *INFOSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	section := spec.Section
	var resp strings.Builder
	sectionInfo := e.serverInfo.Section(section)
	for key, value := range sectionInfo {
		fmt.Fprintf(&resp, "%v:%v\r\n", key, value)
	}
	enc.BulkString(resp.String())
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type KEYSpecs struct {
	Filter string
}

func (s *KEYSpecs) String() string {
	return KEYS
}

func (spec *KEYSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for INCR", invalidIndex)
	}
	spec.Filter = args[0].Literal.(string)
	return nil
}

func (spec *KEYSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	filter := spec.Filter
	if filter == "*" {
		keys := []Token{}
		for k := range e.store.KV.Keys() {
			keys = append(keys, NewToken(BULK_STRING, k))
		}
		enc.Array(keys...)
	} else {
		if hasErr, data := e.parseError(fmt.Errorf("unknown subcommand for KEYS: %v", filter), enc); hasErr {
			return data
		}
		return nil
	}
	enc.Commit()
	return enc.Bytes()
}

type LLENSpecs struct {
	Key string
}

func (s *LLENSpecs) String() string {
	return LLEN
}

func (specs *LLENSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for LLEN", invalidIndex)
	}
	specs.Key = args[0].Literal.(string)
	return nil
}

func (spec *LLENSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	enc.Integer(e.store.List.Len(spec.Key))
	enc.Commit()
	return enc.Bytes()
}

type LRANGESpecs struct {
	Key   string
	Start int64
	End   int64
}

func (s *LRANGESpecs) String() string {
	return LRANGE
}

func (specs *LRANGESpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for RPUSH", invalidIndex)
	}
	specs.Key = args[0].Literal.(string)
	if parsed, err := strconv.ParseInt(args[1].Literal.(string), 10, 64); err != nil {
		return err
	} else {
		specs.Start = parsed
	}
	if parsed, err := strconv.ParseInt(args[2].Literal.(string), 10, 64); err != nil {
		return err
	} else {
		specs.End = parsed
	}
	return nil
}

func (spec *LRANGESpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	data := e.store.List.Get(spec.Key, spec.Start, spec.End)
	dataTokens := []Token{}
	for _, el := range data {
		dataTokens = append(dataTokens, NewToken(BULK_STRING, el))
	}
	enc.Array(dataTokens...)
	enc.Commit()
	return enc.Bytes()
}

type PSYNCSpecs struct {
	ReplicaId string
	Offset    string
}

func (s *PSYNCSpecs) String() string {
	return PSYNC
}

func (spec *PSYNCSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for PSYNC", invalidIndex)
	}
	spec.ReplicaId = args[0].Literal.(string)
	spec.Offset = args[1].Literal.(string)
	return nil
}

func (spec *PSYNCSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	replicaId := spec.ReplicaId
	offset := spec.Offset
	data := "FULLRESYNC "
	if replicaId == "?" {
		replicaId = e.serverInfo.Get("replication", "master_replid")
		data += replicaId
	}
	if offset == "-1" {
		offset = e.serverInfo.Get("replication", "master_repl_offset")
		data += " "
		data += offset
	}
	emptyRdbBuff := [88]uint8{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xFA, 0x09, 0x72,
		0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x32,
		0x2E, 0x30, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69,
		0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65, 0xC2,
		0x6D, 0x08, 0xBC, 0x65, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D,
		0x65, 0x6D, 0xC2, 0xB0, 0xC4, 0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F, 0x66,
		0x2D, 0x62, 0x61, 0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE,
		0xC0, 0xFF, 0x5A, 0xA2}

	enc.SimpleString(data)
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.RawBytes([]byte(fmt.Sprintf("$%v\r\n", len(emptyRdbBuff))))
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.RawBytes(emptyRdbBuff[:])
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
	// TODO: Fix verified replica logic
	// e.verifiedReplica = true
}

type REPLCONFSpecs struct {
	ListeningPort *string
	Capability    *string
	GetAck        *string
}

func (s *REPLCONFSpecs) String() string {
	return REPLCONF
}

func (spec *REPLCONFSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for INFO", invalidIndex)
	}
	subCmd := args[0].Literal.(string)
	value := args[1].Literal.(string)
	switch subCmd {
	case "listening-port":
		spec.ListeningPort = &value
	case "capa":
		spec.Capability = &value
	case "GETACK", "getack":
		spec.GetAck = &value
	default:
		return fmt.Errorf("unsupported args for REPLCONF")
	}
	return nil
}

func (spec *REPLCONFSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	if spec.ListeningPort != nil {
		enc.SimpleString("OK")
	} else if spec.Capability != nil {
		enc.SimpleString("OK")
	} else if spec.GetAck != nil {
		arg := spec.GetAck
		isSlave := e.serverInfo.Get("replication", "role") == "slave"
		if *arg == "*" && isSlave {
			bytesCount := e.processed.Load()
			// REPLCONF ACK 0
			enc.Array(
				NewToken(BULK_STRING, "REPLCONF"),
				NewToken(BULK_STRING, "ACK"),
				NewToken(BULK_STRING, fmt.Sprintf("%v", bytesCount)),
			)
		}
	}
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type RPUSHSpecs struct {
	Element []string
	Key     string
}

func (s *RPUSHSpecs) String() string {
	return RPUSH
}

func (specs *RPUSHSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for RPUSH", invalidIndex)
	}
	specs.Key = args[0].Literal.(string)
	specs.Element = make([]string, 0)
	for _, el := range args[1:] {
		specs.Element = append(specs.Element, el.Literal.(string))
	}
	return nil
}

func (spec *RPUSHSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	enc.Integer(e.store.List.Push(spec.Key, spec.Element))
	enc.Commit()
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return enc.Bytes()
}

type LPUSHSpecs struct {
	Element []string
	Key     string
}

func (s *LPUSHSpecs) String() string {
	return LPUSH
}

func (specs *LPUSHSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for RPUSH", invalidIndex)
	}
	specs.Key = args[0].Literal.(string)
	specs.Element = make([]string, 0)
	for _, el := range args[1:] {
		specs.Element = append(specs.Element, el.Literal.(string))
	}
	return nil
}

func (spec *LPUSHSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	enc.Integer(e.store.List.Prepend(spec.Key, spec.Element))
	enc.Commit()
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return enc.Bytes()
}

type SETSpecs struct {
	Px    int
	Key   string
	Value Token
}

func (s *SETSpecs) String() string {
	return SET
}

func (spec *SETSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for XADD", invalidIndex)
	}
	key := args[0].Literal.(string)
	val := args[1]
	spec.Key = key
	spec.Value = val
	subArgs := args[2:]
	index := 0

	for index < len(subArgs)-1 {
		subCmd := strings.ToLower(subArgs[index].Literal.(string))
		switch subCmd {
		case "px":
			if len(subArgs) <= index+1 {
				return fmt.Errorf("no value for px option in SET command")
			}
			subCmdVal := subArgs[index+1].Literal.(string)

			pxVal, err := strconv.ParseUint(subCmdVal, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid value for px option in SET command")
			}
			spec.Px = int(pxVal)
		default:
			return fmt.Errorf("invalid option %v passed for SET", subCmd)
		}
		index += 2
	}
	return nil
}

func (spec *SETSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	if spec.Px > 0 {
		exp := time.Now().Add(time.Duration(uint64(spec.Px) * uint64(time.Millisecond)))
		e.store.KV.Set(spec.Key, spec.Value, &exp)
	} else {
		e.store.KV.Set(spec.Key, spec.Value, nil)
	}

	if hasErr, data := e.parseError(e.store.KV.Error(), enc); hasErr {
		// TODO: what could go wrong here?
		return data
	}
	enc.SimpleString("OK")
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type TYPESpces struct {
	Key         string
	CurrentTime time.Time
}

func (s *TYPESpces) String() string {
	return TYPE
}

func (spec *TYPESpces) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for TYPE", invalidIndex)
	}
	spec.Key = args[0].Literal.(string)
	spec.CurrentTime = time.Now()
	return nil
}

func (spec *TYPESpces) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	key := spec.Key
	if e.store.Stream.IsStreamKey(key) {
		enc.SimpleString("stream")
	} else if e.store.KV.Get(key, spec.CurrentTime).Literal.(string) != "" {
		enc.SimpleString("string")
	} else {
		enc.SimpleString("none")
	}
	if hasErr, data := e.parseError(enc.Error(), enc); hasErr {
		return data
	}
	enc.Commit()
	return enc.Bytes()
}

type XADDSpecs struct {
	KVs []KeyValue
	Id  *int64
	Seq *int64
	Key string
}

func (s *XADDSpecs) String() string {
	return XADD
}

func (spec *XADDSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for XADD", invalidIndex)
	}
	if len(args[2:])%2 != 0 {
		return fmt.Errorf("value for key %v is not provided", args[len(args)-1])
	}
	// validate stream ID
	// Possible values
	// - number-number
	// - number-*
	// - *
	spec.Key = args[0].Literal.(string)
	streamId := args[1].Literal.(string)
	if streamId == "*" {
		return nil
	}
	ids := strings.Split(streamId, "-")
	if len(ids) == 2 {
		for index, id := range ids {
			if index == 1 && id == "*" {
				continue
			}
			if val, err := strconv.ParseInt(id, 10, 64); err != nil {
				return fmt.Errorf("invalid stream id for command XADD")
			} else {
				if index == 0 {
					spec.Id = &val
				} else {
					spec.Seq = &val
				}
			}
		}
	}
	i := 2
	for i < len(args[2:])-1 {
		key := args[i].Literal.(string)
		value := args[i+1].Literal.(string)
		spec.KVs = append(spec.KVs, KeyValue{
			Key:   key,
			Value: value,
		})
		i += 2
	}
	return nil
}

func (spec *XADDSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	createStreamOpts := []AddStreamOpts{}
	if spec.Id == nil && spec.Seq == nil {
		// TODO: Generate new id and seq
	} else if spec.Seq == nil {
		// TODO: Generate new Seq
		createStreamOpts = append(
			createStreamOpts,
			WithPredefinedId(int(*spec.Id)),
		)
	} else {
		// Both Provided
		createStreamOpts = append(
			createStreamOpts,
			WithPredefinedIdAndSequence(
				int(*spec.Id), int(*spec.Seq),
			),
		)
	}
	generatedId := e.store.Stream.CreateOrUpdateStream(spec.Key, spec.KVs, createStreamOpts...)
	if hasErr, data := e.parseError(e.store.Stream.Error(), enc); hasErr {
		return data
	}
	enc.BulkString(generatedId)
	enc.Commit()
	return enc.Bytes()
}

type POPSpecs struct {
	Key            string
	AmountToRemove int64
}

func (s *POPSpecs) String() string {
	return LPOP
}

func (specs *POPSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for LLEN", invalidIndex)
	}
	specs.Key = args[0].Literal.(string)
	if len(args) > 1 {
		if parsed, err := strconv.ParseInt(args[1].Literal.(string), 10, 64); err != nil {
			return err
		} else {
			specs.AmountToRemove = parsed
		}
	}
	return nil
}

func (spec *POPSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	if spec.AmountToRemove > 0 {
		elements := []Token{}
		for range spec.AmountToRemove {
			popped := e.store.List.Pop(spec.Key)
			if popped == nil {
				break
			}
			elements = append(elements, NewToken(BULK_STRING, *popped))
		}
		enc.Array(elements...)
	} else {
		popped := e.store.List.Pop(spec.Key)
		if popped == nil {
			enc.BulkString("")
		} else {
			enc.BulkString(*popped)
		}
	}
	enc.Commit()
	return enc.Bytes()
}

type BLPOPSpecs struct {
	Keys      []string
	Lifetime  *float64 // in seconds
	Concluded bool
}

func (s *BLPOPSpecs) String() string {
	return BLPOP
}

func (specs *BLPOPSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for LLEN", invalidIndex)
	}
	length := len(args)
	for _, key := range args[:length-1] {
		specs.Keys = append(specs.Keys, key.Literal.(string))
	}
	if parsed, err := strconv.ParseFloat(args[length-1].Literal.(string), 64); err != nil {
		specs.Keys = append(specs.Keys, args[length-1].Literal.(string))
	} else if parsed != 0 {
		specs.Lifetime = &parsed
	}
	return nil
}

func (spec *BLPOPSpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	removedElements := []string{}
	for i := 0; i < len(spec.Keys); i++ {
		key := spec.Keys[0]
		popped := e.store.List.Pop(key)
		if popped == nil {
			waitingArea.mu.Lock()
			waitingArea.queue[key] = append(waitingArea.queue[key], BLPOPHold{
				req:  req,
				resp: removedElements,
				keys: spec.Keys[i:],
			})
			waitingArea.mu.Unlock()
			return nil
		}
		removedElements = append(removedElements, key, *popped)
	}
	tokens := []Token{}
	for _, data := range removedElements {
		tokens = append(tokens, NewToken(BULK_STRING, data))
	}
	spec.Concluded = true
	enc.Array(tokens...)
	enc.Commit()
	return enc.Bytes()
}

type COMMANDSpecs struct {
}

func (s *COMMANDSpecs) String() string {
	return COMMAND
}

func (spec *COMMANDSpecs) Parse(args ...Token) error {
	return nil
}

func (spec *COMMANDSpecs) Execute(e *executor, req Request) []byte {
	return NewEncoder().Array().Commit().Bytes()
}

type MULTISpecs struct {
}

func (s *MULTISpecs) String() string {
	return MULTI
}

func (s *MULTISpecs) Parse(args ...Token) error {
	return nil
}

func (s *MULTISpecs) Execute(e *executor, req Request) []byte {
	return req.GetTX().Multi()
}

type EXECSpecs struct {
}

func (s *EXECSpecs) String() string {
	return EXEC
}

func (s *EXECSpecs) Parse(args ...Token) error {
	return nil
}

func (s *EXECSpecs) Execute(e *executor, req Request) []byte {
	return nil
}

type DISCARDSpecs struct {
}

func (s *DISCARDSpecs) String() string {
	return DISCARD
}

func (s *DISCARDSpecs) Parse(args ...Token) error {
	return nil
}

func (s *DISCARDSpecs) Execute(e *executor, req Request) []byte {
	return req.GetTX().Discard()
}

type WAITSpecs struct {
	NumReplicas uint64
	Timeout     uint64 // in milliseconds
}

func (s *WAITSpecs) String() string {
	return WAIT
}

func (s *WAITSpecs) Parse(args ...Token) error {
	if parsed, err := strconv.ParseUint(args[0].Literal.(string), 10, 64); err != nil {
		return err
	} else {
		s.NumReplicas = parsed
	}
	if parsed, err := strconv.ParseUint(args[1].Literal.(string), 10, 64); err != nil {
		return err
	} else {
		s.Timeout = parsed
	}
	return nil
}

func (s *WAITSpecs) Execute(e *executor, req Request) []byte { return nil }

type SUBSCRIBESpecs struct {
	Key string
	SubscriptionHandler
}

func (s *SUBSCRIBESpecs) String() string {
	return SUBSCRIBE
}

func (s *SUBSCRIBESpecs) Parse(args ...Token) error {
	s.Key = args[0].Literal.(string)
	return nil
}

func (s *SUBSCRIBESpecs) Execute(e *executor, req Request) []byte {
	enc := NewEncoder()
	req.GetSub().Subscribe(s.Key)
	response := []Token{
		NewToken(BULK_STRING, "subscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, req.GetSub().Count()),
	}
	return enc.Array(response...).Commit().Bytes()
}

// SubscriptionHandler
func ParseCmd(tkns ...Token) (specs Cmd, err error) {
	cmd := strings.ToLower(tkns[0].Literal.(string))
	args := tkns[1:]
	spec := GetGenericSpec(cmd)
	if len(args) < spec.MinArgs || (spec.MaxArgs >= 0 && len(args) > spec.MaxArgs) {
		err = fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
		return
	}
	switch cmd {
	case MULTI:
		specs = &MULTISpecs{}
	case EXEC:
		specs = &EXECSpecs{}
	case DISCARD:
		specs = &DISCARDSpecs{}
	case ECHO:
		specs = &ECHOSpecs{}
	case PING:
		specs = &PINGSpecs{}
	case SET:
		specs = &SETSpecs{}
	case GET:
		specs = &GETSpecs{}
	case INCR:
		specs = &INCRSpecs{}
	case INFO:
		specs = &INFOSpecs{}
	case REPLCONF:
		specs = &REPLCONFSpecs{}
	case PSYNC:
		specs = &PSYNCSpecs{}
	case CONFIG:
		specs = &CONFIGSpecs{}
	case KEYS:
		specs = &KEYSpecs{}
	case COMMAND:
		specs = &COMMANDSpecs{}
	case XADD:
		specs = &XADDSpecs{}
	case TYPE:
		specs = &TYPESpces{}
	case RPUSH:
		specs = &RPUSHSpecs{}
	case LRANGE:
		specs = &LRANGESpecs{}
	case LPUSH:
		specs = &LPUSHSpecs{}
	case LLEN:
		specs = &LLENSpecs{}
	case LPOP:
		specs = &POPSpecs{}
	case BLPOP:
		specs = &BLPOPSpecs{}
	case WAIT:
		specs = &WAITSpecs{}
	case SUBSCRIBE:
		specs = &SUBSCRIBESpecs{}
	}
	if specs != nil {
		specs.Parse(args...)
	}
	return
}
