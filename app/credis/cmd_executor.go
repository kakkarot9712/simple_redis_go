package credis

import (
	"bufio"
	"bytes"
	"fmt"
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

func (s *PINGSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if req.Client().Srv().SubManager().Count(req.Client().Id()) > 0 {
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
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, e.rdbConfig.GetRDBDir()),
			)}
		case "dbfilename":
			return &response{data: NewEncoder().Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, e.rdbConfig.GetRDBFileName()),
			)}
		default:
			return &response{data: NewEncoder().SimpleError("ERR: key unsupoorted for command")}
		}
	default:
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR: config action unsupported: %v", action))}
	}
}

func (spec *GETSpecs) Execute(e *executor, req Request) Response {
	val := e.store.KV.Get(spec.Key, spec.CurrentTime)
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
			if hasErr, data := EncodeError(e.store.KV.Error(), NewEncoder()); hasErr {
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
			e.store.KV.Update(key, updatedValue)
			if hasErr, data := EncodeError(e.store.KV.Error(), NewEncoder()); hasErr {
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
	sectionInfo := e.serverInfo.Section(section)
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
		for k := range e.store.KV.Keys() {
			keys = append(keys, NewToken(BULK_STRING, k))
		}
		return &response{data: NewEncoder().Array(keys...)}
	} else {
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR unknown subcommand for KEYS: %v", filter))}
	}
}

func (spec *LLENSpecs) Execute(e *executor, req Request) Response {
	return &response{data: NewEncoder().Integer(e.store.List.Len(spec.Key))}
}

func (spec *LRANGESpecs) Execute(e *executor, req Request) Response {
	data := e.store.List.Get(spec.Key, spec.Start, spec.End)
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
		replicaId = e.serverInfo.Get("replication", "master_replid")
		data += replicaId
	}
	if offset == "-1" {
		offset = e.serverInfo.Get("replication", "master_repl_offset")
		data += " "
		data += offset
	}
	_ = [88]uint8{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xFA, 0x09, 0x72,
		0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x32,
		0x2E, 0x30, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69,
		0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65, 0xC2,
		0x6D, 0x08, 0xBC, 0x65, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D,
		0x65, 0x6D, 0xC2, 0xB0, 0xC4, 0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F,
		0x66, 0x2D, 0x62, 0x61, 0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE,
		0xC0, 0xFF, 0x5A, 0xA2}

	return &response{data: NewEncoder().SimpleString(data)}
	// TODO: Fix verified replica logic
	// e.verifiedReplica = true
}

func (spec *REPLCONFSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if spec.ListeningPort != nil {
		data = NewEncoder().SimpleString("OK")
	} else if spec.Capability != nil {
		data = NewEncoder().SimpleString("OK")
	} else if spec.GetAck != nil {
		arg := spec.GetAck
		isSlave := e.serverInfo.Get("replication", "role") == "slave"
		if *arg == "*" && isSlave {
			bytesCount := req.Client().ProcessedAtomic().Load()
			// REPLCONF ACK 0
			data = NewEncoder().Array(
				NewToken(BULK_STRING, "REPLCONF"),
				NewToken(BULK_STRING, "ACK"),
				NewToken(BULK_STRING, fmt.Sprintf("%v", bytesCount)),
			)
		}
	}
	if data == nil {
		return &response{data: NewEncoder().SimpleString("OK")}
	}
	return &response{data: data}
}

func (spec *RPUSHSpecs) Execute(e *executor, req Request) Response {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return &response{data: NewEncoder().Integer(e.store.List.Push(spec.Key, spec.Elements))}
}

func (s *MULTISpecs) Execute(e *executor, req Request) Response {
	return &response{data: req.Client().GetTX().Multi()}
}

func (s *DISCARDSpecs) Execute(e *executor, req Request) Response {
	return &response{data: req.Client().GetTX().Discard(req.Client())}
}

func (spec *LPUSHSpecs) Execute(e *executor, req Request) Response {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	return &response{data: NewEncoder().Integer(e.store.List.Prepend(spec.Key, spec.Elements))}
}

func (spec *SETSpecs) Execute(e *executor, req Request) Response {
	if spec.Px != nil {
		exp := time.Now().Add(time.Duration(*spec.Px * uint64(time.Millisecond)))
		e.store.KV.Set(spec.Key, spec.Value, &exp)
	} else {
		e.store.KV.Set(spec.Key, spec.Value, nil)
	}

	if e.store.KV.Error() != nil {
		return &response{data: NewEncoder().SimpleError(fmt.Sprintf("ERR: %v", e.store.KV.Error()))}
	}
	return &response{data: NewEncoder().Ok()}
}

func (spec *TYPESpecs) Execute(e *executor, req Request) Response {
	key := spec.Key
	var data []byte
	if e.store.Stream.IsStreamKey(key) {
		data = NewEncoder().SimpleString("stream")
	} else if e.store.KV.Get(key, spec.CurrentTime).Literal.(string) != "" {
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
	generatedId, err := e.store.Stream.CreateOrUpdateStream(spec.Key, spec.KVs, createStreamOpts...)
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
			popped := e.store.List.Pop(spec.Key)
			if popped == nil {
				break
			}
			elements = append(elements, NewToken(BULK_STRING, *popped))
		}
		data = NewEncoder().Array(elements...)
	} else {
		popped := e.store.List.Pop(spec.Key)
		data = NewEncoder().BulkString(popped)
	}
	return &response{data: data}
}

func (spec *BLPOPSpecs) Execute(e *executor, req Request) Response {
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
	return &response{data: NewEncoder().Array(tokens...)}
}

func (s *SUBSCRIBESpecs) Execute(e *executor, req Request) Response {
	sub, err := req.Client().Srv().SubManager().Subscribe(s.Key, req.Client().Id())
	if err != nil {
		return &response{
			data:      NewEncoder().SimpleError(fmt.Sprintf("ERR: %v", err.Error())),
			artifacts: sub,
		}
	}
	req.Client().AddSub(s.Key, sub.Cancel)
	tokens := []Token{
		NewToken(BULK_STRING, "subscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, req.Client().Srv().SubManager().Count(req.Client().Id())),
	}
	return &response{data: NewEncoder().Array(tokens...), artifacts: sub}
}

func (s *PUBLISHSpecs) Execute(e *executor, req Request) Response {
	count := req.Client().Srv().SubManager().Publish(s.Key, s.Message)
	return &response{data: NewEncoder().Integer(count)}
}

func (s *UNSUBSCRIBESpecs) Execute(e *executor, req Request) Response {
	req.Client().CancelSub(s.Key)
	tokens := []Token{
		NewToken(BULK_STRING, "unsubscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, req.Client().Srv().SubManager().Count(req.Client().Id())),
	}
	return &response{data: NewEncoder().Array(tokens...)}
}

func (s *ACL_SETUSERSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	for _, a := range s.Rules {
		char := a[0]
		switch char {
		case '>':
			req.Client().Srv().Auth(s.Username).SetPassword(a[1:])
			// SetPassword
		default:
			// tobe implemented
		}
	}
	return &response{data: enc.Ok()}
}

func (s *ACL_GETUSERSpecs) Execute(e *executor, req Request) Response {
	flags := []Token{}
	for _, f := range req.Client().Srv().Auth(s.User).Flags() {
		flags = append(flags, NewToken(BULK_STRING, f))
	}
	passwords := []Token{}
	for _, p := range req.Client().Srv().Auth(s.User).Passwords() {
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

func (s *AUTHSpecs) Execute(e *executor, req Request) Response {
	if !req.Client().Srv().Auth(s.Username).Authenticate(s.Password) {
		return &response{
			data: NewEncoder().SimpleError("ERR invalid password"),
		}
	}
	return &response{data: NewEncoder().Ok()}
}

func (s *ZRANKSpecs) Execute(e *executor, req Request) Response {
	var rank int
	rank = req.Client().SortedSet().Rank(s.Key, s.Value)
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
	elems := req.Client().SortedSet().Range(s.Key, s.Start, s.End)
	tkns := []Token{}
	for _, e := range elems {
		tkns = append(tkns, NewToken(BULK_STRING, e))
	}
	return &response{
		data: NewEncoder().Array(tkns...),
	}
}

func (s *ZSCORESpecs) Execute(e *executor, req Request) Response {
	scr := req.Client().SortedSet().Get(s.Key, s.Value)
	return &response{
		data: NewEncoder().BulkString(scr),
	}
}

func (s *ZCARDSpecs) Execute(e *executor, req Request) Response {
	card := req.Client().SortedSet().Cardinality(s.Key)
	return &response{
		data: NewEncoder().Integer(card),
	}
}

func (s *ZREMSpecs) Execute(e *executor, req Request) Response {
	card := req.Client().SortedSet().Remove(s.Key, s.Value)
	return &response{
		data: NewEncoder().Integer(card),
	}
}

func (s *ZADDSpecs) Execute(e *executor, req Request) Response {
	newLen := req.Client().SortedSet().Add(s.Key, s.Value, s.Score)
	return &response{data: NewEncoder().Integer(int(newLen))}
}

func (s *WATCHSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	var notifier *CmdNotifier
	for _, k := range s.Keys {
		notifier = req.Client().Watch(strings.ToLower(k))
	}
	return &response{
		data:      enc.Ok(),
		artifacts: notifier,
	}
}

func (s *UNWATCHSpecs) Execute(e *executor, req Request) Response {
	enc := NewEncoder()
	req.Client().TerminateWatcher()
	return &response{
		data:      enc.Ok(),
		artifacts: false,
	}
}

func (s *GEOADDSpecs) Execute(e *executor, req Request) Response {
	var data []byte
	if !ValidateCoords(s.Lng, LNG) || !ValidateCoords(s.Lat, LAT) {
		data = NewEncoder().SimpleError(fmt.Sprintf("ERR invalid longitude,latitude pair %v,%v", s.Lng, s.Lat))
	} else {
		score := Score(s.Lat, s.Lng)
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
	zscoreSpec := ZSCORESpecs{
		Key: s.Key,
	}
	responses := []Token{}
	for _, k := range s.Locs {
		zscoreSpec.Value = k
		res := zscoreSpec.Execute(e, req)
		decoded, _ := NewParser(bufio.NewReader(bytes.NewReader(res.Data()))).TryParse()
		if score, err := strconv.ParseFloat(decoded.Literal.(string), 64); err == nil {
			lat, lng := LatLng(int(score))
			arr := NewToken(ARRAY, []Token{
				NewToken(BULK_STRING, fmt.Sprintf("%v", lat)),
				NewToken(BULK_STRING, fmt.Sprintf("%v", lng)),
			})
			responses = append(responses, arr)
		} else {
			arr := NewToken(ARRAY, []Token{
				NewToken(BULK_STRING, "0"),
				NewToken(BULK_STRING, "0"),
			})
			responses = append(responses, arr)
		}
	}

	data = NewEncoder().Array(responses...)
	return &response{
		data: data,
	}
}
