package executor

import (
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/credis/commands"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/encoder"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type StoreProvider interface {
	ID() string
	Get(key string) tokens.Token
	Set(key string, data tokens.Token, exp *time.Time)
	Update(key string, data tokens.Token)
	Error() error
	Keys() iter.Seq[string]
}

type InfoProvider interface {
	Get(section string, key string) string
	Section(section string) map[string]string
}

type RDBConfigProvider interface {
	GetRDBFileName() string
	GetRDBDir() string
}

type executor struct {
	err             error
	store           StoreProvider
	out             []byte
	multiMode       bool
	txQueue         *TXQueue
	serverInfo      InfoProvider
	verifiedReplica bool
	rdbConfig       RDBConfigProvider
}

type Executor interface {
	Exec(cmd string, args []tokens.Token, opts ...ExecOption)
	Error() error
	Out() []byte
	VerifiedReplica() bool
}

type ExecOpts struct {
	processedBytes *atomic.Uint64
}

type ExecOption func(*ExecOpts)

func WithProcessedBytesAtomic(
	processedBytes *atomic.Uint64,
) ExecOption {
	return func(cfg *ExecOpts) {
		cfg.processedBytes = processedBytes
	}
}

func New(
	str StoreProvider,
	srvinfo InfoProvider,
	rdbConfig RDBConfigProvider,
) Executor {
	queue := NewTXQueue()
	return &executor{
		store:      str,
		txQueue:    queue,
		serverInfo: srvinfo,
		rdbConfig:  rdbConfig,
	}
}

func (e *executor) Error() error {
	return e.err
}

func (e *executor) Exec(cmd string, args []tokens.Token, opts ...ExecOption) {
	// TODO: perform args checks
	cfg := ExecOpts{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if e.multiMode && !slices.Contains(multiExcludedCommands(), cmd) {
		// ExecMulti
		e.appendTx(cmd, args)
	} else {
		switch cmd {
		case commands.ECHO:
			e.echo(args)
		case commands.PING:
			e.ping()
		case commands.SET:
			e.set(args)
		case commands.GET:
			e.get(args)
		case commands.INCR:
			e.incr(args)
		case commands.MULTI:
			e.multi()
		case commands.EXEC:
			e.execMulti()
		case commands.DISCARD:
			e.discard()
		case commands.INFO:
			e.info(args)
		case commands.REPLCONF:
			e.replconf(args, cfg.processedBytes)
		case commands.PSYNC:
			e.psync(args)
		case commands.CONFIG:
			e.config(args)
		case commands.KEYS:
			e.keys(args)
		case commands.COMMAND:
			e.command(args)
		default:
			// TODO: Support other commands
			e.err = &UnsupportedCommandForExecution{
				cmd: cmd,
			}
			return
		}
	}
}

func (e *executor) handleError(err error, enc encoder.Encoder) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*ErrNotInteger); ok {
		enc.SimpleError("ERR value is not an integer or out of range")
	} else if _, ok := err.(*ErrExecWithoutMulti); ok {
		enc.SimpleError("ERR EXEC without MULTI")
	} else if _, ok := err.(*ErrDiscardWithoutMulti); ok {
		enc.SimpleError("ERR DISCARD without MULTI")
	} else {
		enc.SimpleError("ERR something went wrong")
	}
	enc.Commit()
	e.out = enc.Bytes()
	e.err = err
	return true
}

// Read
func (e *executor) echo(args []tokens.Token) {
	enc := encoder.New()
	var data string
	for _, arg := range args {
		switch arg.Type {
		case types.BULK_STRING, types.SIMPLE_STRING:
			data += arg.Literal.(string)
		default:
			e.handleError(fmt.Errorf("unsupported type as a string conv: %v", arg.Literal), enc)
			// TODO: Maybe need to convert other data to string?
			return
		}
	}
	enc.BulkString(data)
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

// Read
func (e *executor) ping() {
	enc := encoder.New()
	enc.SimpleString("PONG")
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

// Write
func (e *executor) set(args []tokens.Token) {
	// Set value in store first
	enc := encoder.New()
	if len(args) < 2 || (len(args) > 2 && len(args) != 4) {
		e.handleError(fmt.Errorf("not enough args sent for SET"), enc)
		return
	}
	key := args[0]
	value := args[1]
	if key.Type != types.BULK_STRING && key.Type != types.SIMPLE_STRING {
		// TODO: maybe support other type of key in map?
		e.handleError(fmt.Errorf("unsupported type as a key: %v", key.Literal), enc)
		return
	}
	if len(args) > 2 {
		opt := strings.ToLower(args[2].Literal.(string))
		switch opt {
		case "px":
			// Exp in Milisecond
			optValue := args[3].Literal.(string)
			expMsFromNow, err := strconv.ParseUint(optValue, 10, 64)
			if e.handleError(err, enc) {
				return
			}
			exp := time.Now().Add(time.Duration(expMsFromNow * uint64(time.Millisecond)))
			e.store.Set(key.Literal.(string), value, &exp)
		default:
			e.err = fmt.Errorf("invalid option passed for SET: %v", args[2].Literal)
			return
		}
	} else {
		e.store.Set(key.Literal.(string), value, nil)
	}

	if e.handleError(e.store.Error(), enc) {
		// TODO: what could go wrong here?
		return
	}
	enc.SimpleString("OK")
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

// Read
func (e *executor) get(args []tokens.Token) {
	enc := encoder.New()
	key := args[0]
	if key.Type != types.BULK_STRING && key.Type != types.SIMPLE_STRING {
		// TODO: maybe support other type of key in map
		e.handleError(fmt.Errorf("unsupported type as a key for GET: %v", key.Literal), enc)
		return
	}
	val := e.store.Get(key.Literal.(string))
	switch val.Type {
	case types.BULK_STRING, types.SIMPLE_STRING:
		data := val.Literal.(string)
		enc.BulkString(data)
		if e.handleError(enc.Error(), enc) {
			return
		}
		enc.Commit()
		e.out = enc.Bytes()
	default:
		// TODO: support other type of values
		e.handleError(fmt.Errorf("unsupported data as value for GET: %v", val.Literal), enc)
	}
}

// Write
func (e *executor) incr(args []tokens.Token) {
	enc := encoder.New()
	switch args[0].Type {
	case types.BULK_STRING, types.SIMPLE_STRING:
		key := args[0].Literal.(string)
		val := e.store.Get(key)

		// Check if value is integer
		switch val.Type {
		case types.BULK_STRING, types.SIMPLE_STRING:
			var updaredNum int
			if val.Literal.(string) == "" {
				// Value does not exists, create one
				updaredNum = 1
				value := tokens.New(types.BULK_STRING, fmt.Sprintf("%v", 1))
				e.store.Set(key, value, nil)
				if e.handleError(e.store.Error(), enc) {
					return
				}
			} else {
				num, err := strconv.ParseInt(val.Literal.(string), 10, 64)
				if err != nil {
					e.handleError(&ErrNotInteger{
						data: num,
					}, enc)
					return
				}
				updaredNum = int(num) + 1
				updatedValue := tokens.New(types.BULK_STRING, fmt.Sprintf("%v", updaredNum))
				e.store.Update(key, updatedValue)
				if e.handleError(e.store.Error(), enc) {
					return
				}
			}
			enc.Integer(updaredNum)
			if e.handleError(enc.Error(), enc) {
				return
			}
			enc.Commit()
			e.out = enc.Bytes()
		default:
			e.handleError(fmt.Errorf("unsupported value for command INCR: %v", val.Literal), enc)
		}
	}
}

// Read
func (e *executor) info(args []tokens.Token) {
	enc := encoder.New()
	section := args[0].Literal.(string)
	resp := ""
	sectionInfo := e.serverInfo.Section(section)
	for key, value := range sectionInfo {
		resp += fmt.Sprintf("%v:%v\r\n", key, value)
	}
	enc.BulkString(resp)
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

// Read?
func (e *executor) replconf(args []tokens.Token, processedBytes *atomic.Uint64) {
	enc := encoder.New()
	subCmd := args[0]
	switch subCmd.Literal.(string) {
	case "listening-port":
		_ = args[1].Literal.(string)
		enc.SimpleString("OK")
	case "capa":
		_ = args[1].Literal.(string)
		enc.SimpleString("OK")
	case "GETACK", "getack":
		arg := args[1].Literal.(string)
		isSlave := e.serverInfo.Get("replication", "role") == "slave"
		bytesCount := processedBytes.Load()
		if arg == "*" && isSlave {
			// REPLCONF ACK 0
			enc.Array(
				tokens.New(types.BULK_STRING, "REPLCONF"),
				tokens.New(types.BULK_STRING, "ACK"),
				tokens.New(types.BULK_STRING, fmt.Sprintf("%v", bytesCount)),
			)
		}
	default:
		e.handleError(fmt.Errorf("unsupported args for REPLCONF"), enc)
		return
	}

	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

// Read?
func (e *executor) psync(args []tokens.Token) {
	enc := encoder.New()
	replicaId := args[0].Literal.(string)
	offset := args[1].Literal.(string)
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
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.RawBytes([]byte(fmt.Sprintf("$%v\r\n", len(emptyRdbBuff))))
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.RawBytes(emptyRdbBuff[:])
	if e.handleError(enc.Error(), enc) {
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
	e.verifiedReplica = true
}

func (e *executor) keys(args []tokens.Token) {
	enc := encoder.New()
	subArg := args[0].Literal.(string)
	if subArg == "*" {
		keys := []tokens.Token{}
		for k := range e.store.Keys() {
			keys = append(keys, tokens.New(types.BULK_STRING, k))
		}
		enc.Array(keys...)
	} else {
		e.handleError(fmt.Errorf("unknown subcommand for KEYS: %v", subArg), enc)
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

func (e *executor) Out() []byte {
	return e.out
}

func (e *executor) VerifiedReplica() bool {
	return e.verifiedReplica
}

func (e *executor) config(args []tokens.Token) {
	action := args[0].Literal.(string)
	enc := encoder.New()
	switch action {
	case "GET", "get":
		key := args[1].Literal.(string)
		switch key {
		case "dir":
			enc.Array(
				tokens.New(types.BULK_STRING, "dir"),
				tokens.New(types.BULK_STRING, e.rdbConfig.GetRDBDir()),
			)
		case "dbfilename":
			enc.Array(
				tokens.New(types.BULK_STRING, "dir"),
				tokens.New(types.BULK_STRING, e.rdbConfig.GetRDBFileName()),
			)

		default:
			e.handleError(fmt.Errorf("key un supoorted"), enc)
			return
		}
	default:
		e.handleError(fmt.Errorf("config action unsupported: %v", action), enc)
		return
	}
	enc.Commit()
	e.out = enc.Bytes()
}

func (e *executor) command(_ []tokens.Token) {
	// TODO: Add proper response. currently only added to prevent issues with redis-cli
	enc := encoder.New()
	enc.Array()
	enc.Commit()
	e.out = enc.Bytes()
}
