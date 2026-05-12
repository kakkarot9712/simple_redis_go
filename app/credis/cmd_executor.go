package credis

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func (s *ECHOSpecs) Execute(e *executor, req Request) Response {
	data := NewEncoder().BulkString(&s.Data)
	if data == nil {
		return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
	}
	return &response{data: data}
}

func (s *COMMANDSpecs) Execute(e *executor, req Request) Response {
	return &response{
		data:           NewEncoder().Array(),
		doNotPropagate: true,
	}
}

func (s *PINGSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if e.deps.SubManager.Count(req.ClientId()) > 0 {
		res := []Token{
			NewToken(BULK_STRING, "pong"),
			NewToken(BULK_STRING, ""),
		}
		data = NewEncoder().Array(res...)
	} else {
		data = NewEncoder().SimpleString("PONG")
	}
	if data == nil {
		return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
	}
	return &response{data: data}
}

func (s *CONFIGSpecs) Execute(e *executor, req Request) Response {
	action := s.Action
	switch action {
	case "GET", "get":
		key := s.Key
		switch key {
		case "dir":
			var dir string
			cwd, err := os.Getwd()
			if err != nil {
				return &response{
					data: NewEncoder().SimpleError("ERR " + err.Error()),
				}
			}
			if e.deps.Cfg.Dir != "" {
				dir = e.deps.Cfg.Dir
			} else {
				dir = cwd
			}
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, dir),
			)}
		case "dbfilename":
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "dbfilename"),
				NewToken(BULK_STRING, e.deps.RDB.GetRDBFileName()),
			)}
		case "appendonly":
			isAof := "no"
			if e.deps.AOF.Enabled() {
				isAof = "yes"
			}
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "appendonly"),
				NewToken(BULK_STRING, isAof),
			)}
		case "appendfsync":
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "appendfsync"),
				NewToken(BULK_STRING, string(e.deps.AOF.Freq())),
			)}
		case "appendfilename":
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "appendfilename"),
				NewToken(BULK_STRING, e.deps.AOF.FileName()),
			)}
		case "appenddirname":
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "appenddirname"),
				NewToken(BULK_STRING, e.deps.AOF.Dir()),
			)}
		default:
			return &response{data: NewEncoder().SimpleError("ERR: key unsupoorted for command")}
		}
	default:
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR: config action unsupported: %v", action))}
	}
}

func (spec *GETSpecs) Execute(e *executor, req Request) Response {
	val := e.deps.KVStore.Get(spec.Key, spec.CurrentTime)
	switch val.Type {
	case BULK_STRING, SIMPLE_STRING:
		data := val.Literal.(string)
		var enc []byte
		if data == "" {
			enc = NewEncoder().BulkString(nil)
		} else {
			enc = NewEncoder().BulkString(&data)
		}
		if enc == nil {
			return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
		}
		return &response{data: enc}
	default:
		// TODO: support other type of values
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR unsupported data as value for GET: %v", val.Literal))}
	}
}

func (spec *INCRSpecs) Execute(e *executor, req Request) Response {
	key := spec.Key
	val := e.deps.KVStore.Get(key, spec.CurrentTime)

	// Check if value is integer
	switch val.Type {
	case BULK_STRING, SIMPLE_STRING:
		var updaredNum int
		if val.Literal.(string) == "" {
			// Value does not exists, create one
			updaredNum = 1
			value := NewToken(BULK_STRING, fmt.Sprintf("%v", 1))
			e.deps.KVStore.Set(key, value, nil)
			if hasErr, data := EncodeError(e.deps.KVStore.Error(), NewEncoder()); hasErr {
				return &response{data: data}
			}
		} else {
			num, err := strconv.ParseInt(val.Literal.(string), 10, 64)
			if err != nil {
				if hasErr, data := EncodeError(&ErrNotInteger{
					data: num,
				}, NewEncoder()); hasErr {
					return &response{data: data}
				}
				return nil
			}
			updaredNum = int(num) + 1
			updatedValue := NewToken(BULK_STRING, fmt.Sprintf("%v", updaredNum))
			e.deps.KVStore.Update(key, updatedValue)
			if hasErr, data := EncodeError(e.deps.KVStore.Error(), NewEncoder()); hasErr {
				return &response{data: data}
			}
		}
		enc := NewEncoder().Integer(updaredNum)
		if enc == nil {
			return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
		}
		return &response{data: enc}
	default:
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR unsupported value for command INCR: %v", val.Literal))}
	}
}

func (spec *INFOSpecs) Execute(e *executor, req Request) Response {
	section := spec.Section
	var resp strings.Builder
	sectionInfo := e.deps.Info.Section(section)
	for key, value := range sectionInfo {
		fmt.Fprintf(&resp, "%v:%v\r\n", key, value)
	}
	data := resp.String()
	enc := NewEncoder().BulkString(&data)
	if enc == nil {
		return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
	}
	return &response{data: enc}
}

func (spec *KEYSSpecs) Execute(e *executor, req Request) Response {
	filter := spec.Filter
	if filter == "*" {
		keys := []Token{}
		for k := range e.deps.KVStore.Keys() {
			keys = append(keys, NewToken(BULK_STRING, k))
		}
		return &response{data: NewEncoder().Array(keys...)}
	} else {
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR unknown subcommand for KEYS: %v", filter))}
	}
}

func (spec *LLENSpecs) Execute(e *executor, req Request) Response {
	return &response{data: NewEncoder().Integer(e.deps.ListStore.Len(spec.Key))}
}

func (spec *LRANGESpecs) Execute(e *executor, req Request) Response {
	data := e.deps.ListStore.Get(spec.Key, spec.Start, spec.End)
	dataTokens := []Token{}
	for _, el := range data {
		dataTokens = append(dataTokens, NewToken(BULK_STRING, el))
	}
	return &response{data: NewEncoder().Array(dataTokens...)}
}

func (spec *PSYNCSpecs) Execute(e *executor, req Request) Response {
	replicaId := spec.ReplicaId
	offset := spec.Offset
	data := "FULLRESYNC "
	if replicaId == "?" {
		replicaId = e.deps.Info.Get("replication", "master_replid")
		data += replicaId
	}
	if offset == "-1" {
		offset = e.deps.Info.Get("replication", "master_repl_offset")
		data += " "
		data += offset
	}
	emptyRdb := []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xFA, 0x09, 0x72,
		0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x32,
		0x2E, 0x30, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69,
		0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65, 0xC2,
		0x6D, 0x08, 0xBC, 0x65, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D,
		0x65, 0x6D, 0xC2, 0xB0, 0xC4, 0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F,
		0x66, 0x2D, 0x62, 0x61, 0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE,
		0xC0, 0xFF, 0x5A, 0xA2}

	rsyncResp := NewEncoder().SimpleString(data)
	emptyRdbBuff := fmt.Sprintf("$%v\r\n", len(emptyRdb))
	buff := []byte{}
	buff = append(buff, rsyncResp...)
	buff = append(buff, []byte(emptyRdbBuff)...)
	buff = append(buff, emptyRdb...)
	repl := e.deps.ReplicaManager.Add(req.ClientId())
	return &response{data: buff, artifacts: repl, doNotPropagate: true}
}

func (spec *REPLCONFSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	var doNotPropagate bool
	if spec.ListeningPort != nil {
		data = NewEncoder().SimpleString("OK")
		doNotPropagate = true
	} else if spec.Capability != nil {
		data = NewEncoder().SimpleString("OK")
		doNotPropagate = true
	} else if spec.GetAck != nil {
		bytesCount := e.deps.ReplicaManager.Processed()
		data = NewEncoder().Array(
			NewToken(BULK_STRING, "REPLCONF"),
			NewToken(BULK_STRING, "ACK"),
			NewToken(BULK_STRING, fmt.Sprintf("%v", bytesCount)),
		)
	}
	if data == nil {
		return &response{data: NewEncoder().SimpleString("OK"), doNotPropagate: doNotPropagate}
	}
	return &response{data: data, doNotPropagate: doNotPropagate}
}

func (spec *WAITSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	timer := time.NewTimer(time.Duration(spec.Timeout) * time.Millisecond)
	minReplica := spec.NumReplicas
	// masterPropagatedBytes := e.deps.ReplicaManager.Processed()
	setisfiedMinReplCount := make(chan int)
	// var currentReplCount int
	terminate := make(chan bool)
	go func() {
		for {
			select {
			case <-terminate:
				return
			default:
				currentReplCount := e.deps.ReplicaManager.NumReplicas()
				if currentReplCount >= int(minReplica) {
					setisfiedMinReplCount <- currentReplCount
					return
				}
			}
		}
	}()
	select {
	case count := <-setisfiedMinReplCount:
		// Min Repl count achieved
		data = NewEncoder().Integer(count)
	case <-timer.C:
		// Timeout
		terminate <- true
		close(terminate)
		close(setisfiedMinReplCount)
		data = NewEncoder().Integer(e.deps.ReplicaManager.NumReplicas())
		// case true:
	}
	return &response{
		data: data,
	}
}

func (spec *RPUSHSpecs) Execute(e *executor, req Request) Response {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return &response{data: NewEncoder().Integer(e.deps.ListStore.Push(spec.Key, spec.Elements))}
}

func (s *EXECSpecs) Execute(e *executor, req Request) Response {
	return &response{
		data: req.TX().Exec(req.Ctx(), e, req.ClientId()),
	}
}

func (s *MULTISpecs) Execute(e *executor, req Request) Response {
	return &response{data: req.TX().Multi()}
}

func (s *DISCARDSpecs) Execute(e *executor, req Request) Response {
	data := req.TX().Discard()
	e.deps.Watcher.Cancel(req.ClientId())
	return &response{data: data}
}

func (spec *LPUSHSpecs) Execute(e *executor, req Request) Response {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return &response{data: NewEncoder().Integer(e.deps.ListStore.Prepend(spec.Key, spec.Elements))}
}

func (spec *SETSpecs) Execute(e *executor, req Request) Response {
	if spec.Px != nil {
		exp := time.Now().Add(time.Duration(*spec.Px * uint64(time.Millisecond)))
		e.deps.KVStore.Set(spec.Key, spec.Value, &exp)
	} else {
		e.deps.KVStore.Set(spec.Key, spec.Value, nil)
	}

	if e.deps.KVStore.Error() != nil {
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR: %v", e.deps.KVStore.Error()))}
	}
	return &response{data: NewEncoder().Ok()}
}

func (spec *TYPESpecs) Execute(e *executor, req Request) Response {
	key := spec.Key
	var data []byte
	if e.deps.StreamStore.IsStreamKey(key) {
		data = NewEncoder().SimpleString("stream")
	} else if e.deps.KVStore.Get(key, spec.CurrentTime).Literal.(string) != "" {
		data = NewEncoder().SimpleString("string")
	} else {
		data = NewEncoder().SimpleString("none")
	}
	if data == nil {
		return &response{data: NewEncoder().SimpleError("ERR encoding failed")}
	}
	return &response{data: data}
}

func (spec *XADDSpecs) Execute(e *executor, req Request) Response {
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
	generatedId, err := e.deps.StreamStore.CreateOrUpdateStream(spec.Key, spec.KVs, createStreamOpts...)
	if err != nil {
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR: %v", err))}
	}
	return &response{data: NewEncoder().BulkString(&generatedId)}
}

func (spec *LPOPSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if spec.AmountToRemove != nil {
		elements := []Token{}
		for range *spec.AmountToRemove {
			popped := e.deps.ListStore.Pop(spec.Key)
			if popped == nil {
				break
			}
			elements = append(elements, NewToken(BULK_STRING, *popped))
		}
		data = NewEncoder().Array(elements...)
	} else {
		popped := e.deps.ListStore.Pop(spec.Key)
		data = NewEncoder().BulkString(popped)
	}
	return &response{data: data}
}

func (spec *BLPOPSpecs) Execute(e *executor, req Request) Response {
	removedElements := []string{}
	for i := 0; i < len(spec.Keys); i++ {
		key := spec.Keys[0]
		popped := e.deps.ListStore.Pop(key)
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
	return &response{data: NewEncoder().Array(tokens...)}
}

func (s *SUBSCRIBESpecs) Execute(e *executor, req Request) Response {
	sub, count, err := e.deps.SubManager.Subscribe(s.Key, req.ClientId())
	if err != nil {
		return &response{
			data:      NewEncoder().SimpleError(fmt.Sprintf("ERR: %v", err.Error())),
			artifacts: sub,
		}
	}
	tokens := []Token{
		NewToken(BULK_STRING, "subscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, count),
	}
	return &response{data: NewEncoder().Array(tokens...), artifacts: sub}
}

func (s *PUBLISHSpecs) Execute(e *executor, req Request) Response {
	count := e.deps.SubManager.Publish(s.Key, s.Message)
	return &response{data: NewEncoder().Integer(count)}
}

func (s *UNSUBSCRIBESpecs) Execute(e *executor, req Request) Response {
	count := e.deps.SubManager.Cancel(req.ClientId(), s.Key)
	tokens := []Token{
		NewToken(BULK_STRING, "unsubscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, count),
	}
	return &response{data: NewEncoder().Array(tokens...)}
}

func (s *AUTHSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if e.deps.Auth.Authenticate(req.AuthCtx(), s.Username, s.Password) {
		data = NewEncoder().Ok()
	} else {
		data = NewEncoder().SimpleError((&ErrAuthWrongPassword{}).Error())
	}
	return &response{
		data: data,
	}
}

func (s *ACL_SETUSERSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	for _, a := range s.Rules {
		char := a[0]
		switch char {
		case '>':
			e.deps.Auth.SetPassword(s.Username, a[1:])
			// SetPassword
		default:
			// tobe implemented
		}
	}
	return &response{data: enc.Ok()}
}

func (s *ACL_WHOAMISpecs) Execute(e *executor, req Request) Response {
	currentUser := req.AuthCtx().user
	return &response{
		data: NewEncoder().BulkString(&currentUser),
	}
}

func (s *ACL_GETUSERSpecs) Execute(e *executor, req Request) Response {
	user := e.deps.Auth.AuthenticatedUser(req.AuthCtx())
	flags := []Token{}
	for _, f := range user.flags {
		flags = append(flags, NewToken(BULK_STRING, f))
	}
	passwords := []Token{}
	for _, p := range user.passwords {
		passwords = append(passwords, NewToken(BULK_STRING, p))
	}
	tokens := []Token{
		NewToken(BULK_STRING, "flags"),
		NewToken(ARRAY, flags),
		NewToken(BULK_STRING, "passwords"),
		NewToken(ARRAY, passwords),
	}
	return &response{data: NewEncoder().Array(tokens...)}
}

func (s *ZRANKSpecs) Execute(e *executor, req Request) Response {
	var rank int
	rank = e.deps.SortedSet.Rank(s.Key, s.Value)
	var data []byte
	if rank == -1 {
		data = NewEncoder().BulkString(nil)
	} else {
		data = NewEncoder().Integer(int(rank))
	}
	return &response{
		data: data,
	}
}

func (s *ZRANGESpecs) Execute(e *executor, req Request) Response {
	elems := e.deps.SortedSet.Range(s.Key, s.Start, s.End)
	tkns := []Token{}
	for _, e := range elems {
		tkns = append(tkns, NewToken(BULK_STRING, e))
	}
	return &response{
		data: NewEncoder().Array(tkns...),
	}
}

func (s *ZSCORESpecs) Execute(e *executor, req Request) Response {
	scr := e.deps.SortedSet.Get(s.Key, s.Value)
	return &response{
		data: NewEncoder().BulkString(scr),
	}
}

func (s *ZCARDSpecs) Execute(e *executor, req Request) Response {
	card := e.deps.SortedSet.Cardinality(s.Key)
	return &response{
		data: NewEncoder().Integer(card),
	}
}

func (s *ZREMSpecs) Execute(e *executor, req Request) Response {
	card := e.deps.SortedSet.Remove(s.Key, s.Value)
	return &response{
		data: NewEncoder().Integer(card),
	}
}

func (s *ZADDSpecs) Execute(e *executor, req Request) Response {
	newLen := e.deps.SortedSet.Add(s.Key, s.Value, s.Score)
	return &response{data: NewEncoder().Integer(int(newLen))}
}

func (s *WATCHSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	var data []byte
	if req.TX().IsMulti() {
		data = NewEncoder().SimpleError("ERR WATCH inside MULTI is not allowed")
	} else {
		e.deps.Watcher.Add(req.ClientId(), s.Keys...)
		data = enc.Ok()
	}
	return &response{
		data: data,
		// artifacts: notifier,
	}
}

func (s *UNWATCHSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	e.deps.Watcher.Cancel(req.ClientId())
	return &response{
		data:      enc.Ok(),
		artifacts: false,
	}
}

func (s *GEOADDSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	loc := Location{
		Lat: s.Lat,
		Lng: s.Lng,
	}
	if !ValidateCoords(loc) {
		data = NewEncoder().SimpleError(fmt.Sprintf("ERR invalid longitude,latitude pair %v,%v", s.Lng, s.Lat))
	} else {
		score := Score(loc)
		zaddSpec := ZADDSpecs{
			Key:   s.Key,
			Value: s.Member,
			Score: float64(score),
		}
		res := zaddSpec.Execute(e, req)
		data = res.Data()
	}

	return &response{
		data: data,
	}
}

func (s *GEOPOSSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	responses := []Token{}
	for _, k := range s.Locs {
		scr := e.deps.SortedSet.Get(s.Key, k)
		if scr == nil {
			arr := NewToken(ARRAY, nil)
			responses = append(responses, arr)
		} else if score, err := strconv.ParseFloat(*scr, 64); err == nil {
			loc := LatLng(uint64(score))
			arr := NewToken(ARRAY, []Token{
				NewToken(BULK_STRING, fmt.Sprintf("%v", loc.Lng)),
				NewToken(BULK_STRING, fmt.Sprintf("%v", loc.Lat)),
			})
			responses = append(responses, arr)
		} else {
			fmt.Println(err)
		}
	}

	data = NewEncoder().Array(responses...)
	return &response{
		data: data,
	}
}

func (s *GEODISTSpecs) Execute(e *executor, req Request) Response {
	scr1 := e.deps.SortedSet.Get(s.Key, s.Place1)
	scr2 := e.deps.SortedSet.Get(s.Key, s.Place2)
	var loc1, loc2 Location
	if score1, err := strconv.ParseFloat(*scr1, 64); err == nil {
		loc1 = LatLng(uint64(score1))
	}
	if score2, err := strconv.ParseFloat(*scr2, 64); err == nil {
		loc2 = LatLng(uint64(score2))
	}
	dist := fmt.Sprintf("%.4f", Dist(loc1, loc2))
	return &response{
		data: NewEncoder().BulkString(&dist),
	}
}

func (s *GEOSEARCHSpecs) Execute(e *executor, req Request) Response {
	places := e.deps.SortedSet.List(s.Place)
	placesInRadius := []Token{}
	for p, v := range places {
		loc2 := LatLng(uint64(v.score))
		dist := Dist(s.FromLatLng, loc2)
		if dist <= float64(s.Radius) {
			placesInRadius = append(placesInRadius, NewToken(BULK_STRING, p))
		}
	}
	return &response{
		data: NewEncoder().Array(placesInRadius...),
	}
}
