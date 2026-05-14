package credis

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"strings"
	"sync"
)

type AOFSyncFreq string

const (
	EVERY_SEC AOFSyncFreq = "everysec"
	ALAWYS    AOFSyncFreq = "always"
	NO        AOFSyncFreq = "no"
)

func SyncFreq(val string) AOFSyncFreq {
	switch val {
	case string(EVERY_SEC), string(ALAWYS), string(NO):
		return AOFSyncFreq(val)
	default:
		return EVERY_SEC
	}
}

func NewAOFConfig(rootDir, dir, fileName string, freq AOFSyncFreq, enabled bool) AOF {
	return &aofConfig{
		dir:      dir,
		fileName: fileName,
		freq:     freq,
		enabled:  enabled,
		rootDir:  rootDir,
	}
}

type aofConfig struct {
	rootDir  string
	mu       sync.Mutex
	enabled  bool
	dir      string
	fileName string
	freq     AOFSyncFreq
	incrFile *os.File
}

type AOF interface {
	GetLatestIncrFile() (string, error)
	Enabled() bool
	Freq() AOFSyncFreq
	FileName() string
	Dir() string
	Initialize(deps *deps) error
	FlushToAOF() error
	WriteToAOF(data []byte) (n int, e error)
	WriteAndFlushToAOF(data []byte) (int, error)
}

func (aof *aofConfig) GetLatestIncrFile() (string, error) {
	manifestFileName := fmt.Sprintf("%v.manifest", path.Join(aof.rootDir, aof.dir, aof.fileName))
	buff, err := os.ReadFile(manifestFileName)
	if err != nil {
		return "", err
	}
	incrFiles := strings.Split(strings.Trim(string(buff), "\r\n"), "\r\n")
	activeIncrFileName := strings.Split(strings.TrimPrefix(incrFiles[len(incrFiles)-1], "file "), " ")[0]
	return activeIncrFileName, nil
}

func (aof *aofConfig) Enabled() bool {
	return aof.enabled
}

func (aof *aofConfig) Freq() AOFSyncFreq {
	return aof.freq
}

func (aof *aofConfig) FileName() string {
	return aof.fileName
}

func (aof *aofConfig) Dir() string {
	return aof.dir
}

// TODO: fix function
// Restores data or initializes AOF manifest if enabled
func (aof *aofConfig) Initialize(deps *deps) error {
	if !aof.enabled {
		return nil
	}
	err := os.MkdirAll(path.Join(aof.rootDir, aof.dir), os.ModeDir)
	if err != nil {
		return fmt.Errorf("ERR creating directory for AOF persistence: %v", err)
	}
	incrFile := fmt.Sprintf("%v.1.incr.aof", path.Join(aof.rootDir, aof.dir, aof.fileName))
	manifestFileName := fmt.Sprintf("%v.manifest", path.Join(aof.rootDir, aof.dir, aof.fileName))
	seq := 1
	incr, err := os.OpenFile(incrFile, os.O_CREATE|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("ERR creating file for AOF persistence: %v", err)
	}
	defer incr.Close()
	_, err = os.Stat(manifestFileName)
	if os.IsNotExist(err) {
		manifest, err := os.OpenFile(manifestFileName, os.O_CREATE|os.O_RDWR, os.ModeAppend)
		if err != nil {
			log.Fatalf("ERR creating manifest for AOF persistence: %v", err)
		}
		_, err = manifest.Write(fmt.Appendf([]byte{}, "file %v.%v.incr.aof seq %v type i\r\n", aof.fileName, seq, seq))
		if err != nil {
			log.Fatalf("ERR writing to manifest for AOF persistence: %v", err)
		}
		defer manifest.Close()
		manifest.Sync()

	} else {
		incrFileName, err := aof.GetLatestIncrFile()
		if err != nil {
			log.Fatalf("ERR restoring from AOF: %v", err)
		}
		buff, err := os.ReadFile(path.Join(aof.rootDir, aof.dir, incrFileName))
		if err != nil {
			log.Fatalf("ERR restoring from AOF: %v", err)
		}
		e := NewExec(deps)
		e.Use(ExecutorMiddleware)
		p := NewParser(bufio.NewReader(bytes.NewReader(buff)))
		txs := NewTX()
		for {
			rawReq, _ := p.TryParse()
			if p.Error() != nil {
				break
			}
			tokenType := rawReq.Type

			if tokenType != ARRAY {
				// Ignore that as of now
				continue
			}
			tkns := rawReq.Literal.([]Token)
			if len(tkns) == 0 {
				continue
			}
			buffLen := uint(math.Min(float64(2), float64(len(tkns))))
			argsIndex, cmd, err := ParseCmd(tkns[:buffLen]...)
			if err != nil {
				continue
			}
			var args []Token
			if len(tkns) > argsIndex {
				args = tkns[argsIndex:]
			}

			req := NewRequest(
				context.Background(),
				DefaultAuthContext(), txs,
				NewClient(nil, nil, "default", true),
				nil,
			)
			specs, err := ParseSpec(cmd, args...)
			req.SetSpecs(specs)
			e.Exec(req)
		}
	}
	return nil
}

func (aof *aofConfig) WriteToAOF(data []byte) (n int, e error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.incrFile.Write(data)
}

func (aof *aofConfig) WriteAndFlushToAOF(data []byte) (int, error) {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	activeIncrFileName, err := aof.GetLatestIncrFile()
	if err != nil {
		return 0, err
	}
	incrFile, err := os.OpenFile(fmt.Sprintf("%v",
		path.Join(aof.rootDir, aof.dir, activeIncrFileName)),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		os.ModeAppend,
	)
	if err != nil {
		return 0, err
	}
	defer incrFile.Close()
	n, err := incrFile.Write(data)
	if err != nil {
		return 0, err
	}
	err = incrFile.Sync()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (aof *aofConfig) FlushToAOF() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.incrFile.Sync()
}

func (aof *aofConfig) Close() {
	if aof.incrFile != nil {
		aof.incrFile.Close()
	}
}

func AOFWriterMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	genericSpec := GetGenericSpec(req.Specs().String())
	if genericSpec.Write {
		rawCmd := strings.ToUpper(strings.Replace(req.Specs().String(), "_", " ", 1))
		tkns := []Token{NewToken(BULK_STRING, rawCmd)}
		tkns = append(tkns, req.Args()...)

		if e.deps.AOF.Freq() == ALAWYS {
			e.deps.AOF.WriteAndFlushToAOF(NewEncoder().Array(tkns...))
		} else {
			e.deps.AOF.WriteToAOF(NewEncoder().Array(tkns...))
		}
	}
}
