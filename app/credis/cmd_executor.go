package credis

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func (s *ECHOSpecs) Execute(e *executor, req Request, res Response) error {
	data := NewEncoder().BulkString(&s.Data)
	if data == nil {
		return &ErrEncodingFailed{}
	}
	res.Set(data, nil, false)
	return nil
}

func (s *COMMANDSpecs) Execute(e *executor, req Request, res Response) error {
	res.Set(NewEncoder().Array(), nil, true)
	return nil
}

func (s *PINGSpecs) Execute(e *executor, req Request, res Response) error {
	var data []byte
	if e.deps.SubManager.Count(req.ClientId()) > 0 {
		tkns := []Token{
			NewToken(BULK_STRING, "pong"),
			NewToken(BULK_STRING, ""),
		}
		data = NewEncoder().Array(tkns...)
	} else {
		data = NewEncoder().SimpleString("PONG")
	}
	if data == nil {
		return &ErrEncodingFailed{}
	}
	res.Set(data, nil, false)
	return nil
}

func (s *CONFIGSpecs) Execute(e *executor, req Request, res Response) error {
	action := s.Action
	switch action {
	case "GET", "get":
		key := s.Key
		switch key {
		case "dir":
			var dir string
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			if e.deps.Cfg.Dir != "" {
				dir = e.deps.Cfg.Dir
			} else {
				dir = cwd
			}
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "dir"),
				NewToken(BULK_STRING, dir),
			), nil, false)
		case "dbfilename":
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "dbfilename"),
				NewToken(BULK_STRING, e.deps.RDB.GetRDBFileName()),
			), nil, false)
		case "appendonly":
			isAof := "no"
			if e.deps.AOF.Enabled() {
				isAof = "yes"
			}
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "appendonly"),
				NewToken(BULK_STRING, isAof),
			), nil, false)
		case "appendfsync":
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "appendfsync"),
				NewToken(BULK_STRING, string(e.deps.AOF.Freq())),
			), nil, false)
		case "appendfilename":
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "appendfilename"),
				NewToken(BULK_STRING, e.deps.AOF.FileName()),
			), nil, false)
		case "appenddirname":
			res.Set(NewEncoder().Array(
				NewToken(BULK_STRING, "appenddirname"),
				NewToken(BULK_STRING, e.deps.AOF.Dir()),
			), nil, false)
		default:
			res.Set(NewEncoder().SimpleError("ERR: key unsupoorted for command"), nil, false)
		}
	default:
		res.Set(NewEncoder().SimpleError(fmt.Sprintf("ERR: config action unsupported: %v", action)), nil, false)
	}
	return nil
}

func (spec *GETSpecs) Execute(e *executor, req Request, res Response) error {
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
			return &ErrEncodingFailed{}
		}
		res.Set(enc, nil, false)
	default:
		// TODO: support other type of values
		return &ErrUnsupportedDataType{
			cmd:      GET,
			dataType: val.Literal,
		}
	}
	return nil
}

func (spec *INCRSpecs) Execute(e *executor, req Request, res Response) error {
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
				res.Set(data, nil, false)
				return nil
			}
		} else {
			num, err := strconv.ParseInt(val.Literal.(string), 10, 64)
			if err != nil {
				return &ErrNotInteger{
					data: num,
				}
			}
			updaredNum = int(num) + 1
			updatedValue := NewToken(BULK_STRING, fmt.Sprintf("%v", updaredNum))
			e.deps.KVStore.Update(key, updatedValue)
			if hasErr, data := EncodeError(e.deps.KVStore.Error(), NewEncoder()); hasErr {
				res.Set(data, nil, false)
				return nil
			}
		}
		enc := NewEncoder().Integer(updaredNum)
		if enc == nil {
			return &ErrEncodingFailed{}
		}
		res.Set(enc, nil, false)
	default:
		return &ErrUnsupportedDataType{
			cmd:      INCR,
			dataType: val.Literal,
		}
	}
	return nil
}

func (spec *INFOSpecs) Execute(e *executor, req Request, res Response) error {
	section := spec.Section
	var resp strings.Builder
	sectionInfo := e.deps.Info.Section(section)
	for key, value := range sectionInfo {
		fmt.Fprintf(&resp, "%v:%v\r\n", key, value)
	}
	data := resp.String()
	enc := NewEncoder().BulkString(&data)
	if enc == nil {
		return &ErrEncodingFailed{}
	}
	res.Set(enc, nil, false)
	return nil
}

func (spec *KEYSSpecs) Execute(e *executor, req Request, res Response) error {
	filter := spec.Filter
	if filter == "*" {
		keys := []Token{}
		for k := range e.deps.KVStore.Keys() {
			keys = append(keys, NewToken(BULK_STRING, k))
		}
		res.Set(NewEncoder().Array(keys...), nil, false)
		return nil
	}
	return &ErrUnsupportedDataType{
		cmd:      KEYS,
		dataType: spec.Filter,
	}
}

func (spec *LLENSpecs) Execute(e *executor, req Request, res Response) error {
	res.Set(NewEncoder().Integer(e.deps.ListStore.Len(spec.Key)), nil, false)
	return nil
}

func (spec *LRANGESpecs) Execute(e *executor, req Request, res Response) error {
	data := e.deps.ListStore.Get(spec.Key, spec.Start, spec.End)
	dataTokens := []Token{}
	for _, el := range data {
		dataTokens = append(dataTokens, NewToken(BULK_STRING, el))
	}
	res.Set(NewEncoder().Array(dataTokens...), nil, false)
	return nil
}

func (spec *PSYNCSpecs) Execute(e *executor, req Request, res Response) error {
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
		0x65, 0x6D, 0xC2, 0xB0, 0xC4, 0x10, 0x00, 0xFA, 0x08, 0x61, 0x6F, 0x66,
		0x2D, 0x62, 0x61, 0x73, 0x65, 0xC0, 0x00, 0xFF, 0xF0, 0x6E, 0x3B, 0xFE,
		0xC0, 0xFF, 0x5A, 0xA2,
	}

	rsyncResp := NewEncoder().SimpleString(data)
	emptyRdbBuff := fmt.Sprintf("$%v\r\n", len(emptyRdb))
	buff := []byte{}
	buff = append(buff, rsyncResp...)
	buff = append(buff, []byte(emptyRdbBuff)...)
	buff = append(buff, emptyRdb...)
	e.deps.ReplicaManager.Add(req.Client())
	res.Set(buff, nil, true)
	return nil
}

func (spec *REPLCONFSpecs) Execute(e *executor, req Request, res Response) error {
	var data []byte
	if spec.ListeningPort != nil {
		data = NewEncoder().Ok()
	} else if spec.Capability != nil {
		data = NewEncoder().Ok()
	} else if spec.GetAck != nil {
		bytesCount := e.deps.ReplicaManager.Processed()
		data = NewEncoder().Array(
			NewToken(BULK_STRING, "REPLCONF"),
			NewToken(BULK_STRING, "ACK"),
			NewToken(BULK_STRING, fmt.Sprintf("%v", bytesCount)),
		)
	} else if spec.Ack != nil {
		// Keep record of acks from all replicas
		e.deps.ReplicaManager.UpdateAcks(req.ClientId(), *spec.Ack)
		res.Set(nil, req.Client(), true)
		return nil
	}
	if data == nil {
		data = NewEncoder().Ok()
	}
	res.Set(data, nil, true)
	return nil
}

func (spec *WAITSpecs) Execute(e *executor, req Request, res Response) error {
	var data []byte
	timer := time.NewTimer(time.Duration(spec.Timeout) * time.Millisecond)
	minReplica := spec.NumReplicas
	masterPropagatedBytes := e.deps.ReplicaManager.Processed()
	if masterPropagatedBytes == 0 {
		// No commands passed
		data = NewEncoder().Integer(e.deps.ReplicaManager.NumReplicas())
		res.Set(data, nil, false)
		return nil
	}
	setisfiedMinReplCount := make(chan int)
	var currentReplCount int
	terminate := make(chan bool)
	e.deps.ReplicaManager.RequestAckFromAllRepl()
	go func() {
		for {
			select {
			case <-terminate:
				return
			default:
				count := e.deps.ReplicaManager.GetAcksFromReplica(
					int(spec.NumReplicas),
					int64(masterPropagatedBytes),
				)
				currentReplCount = count
				if currentReplCount >= int(minReplica) {
					setisfiedMinReplCount <- count
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
		data = NewEncoder().Integer(currentReplCount)
		// case true:
	}
	res.Set(data, nil, false)
	return nil
}

func (spec *RPUSHSpecs) Execute(e *executor, req Request, res Response) error {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	res.Set(NewEncoder().Integer(e.deps.ListStore.Push(spec.Key, spec.Elements)), nil, false)
	return nil
}

func (s *EXECSpecs) Execute(e *executor, req Request, res Response) error {
	res.Set(req.TX().Exec(req.Ctx(), e, req.ClientId()), nil, false)
	return nil
}

func (s *MULTISpecs) Execute(e *executor, req Request, res Response) error {
	res.Set(req.TX().Multi(), nil, false)
	return nil
}

func (s *DISCARDSpecs) Execute(e *executor, req Request, res Response) error {
	data := req.TX().Discard()
	e.deps.Watcher.Cancel(req.ClientId())
	res.Set(data, nil, false)
	return nil
}

func (spec *LPUSHSpecs) Execute(e *executor, req Request, res Response) error {
	go func() {
		keyUpdatesChan <- spec.Key
	}()
	res.Set(NewEncoder().Integer(e.deps.ListStore.Prepend(spec.Key, spec.Elements)), nil, false)
	return nil
}

func (spec *SETSpecs) Execute(e *executor, req Request, res Response) error {
	if spec.Px != nil {
		exp := time.Now().Add(time.Duration(*spec.Px * uint64(time.Millisecond)))
		e.deps.KVStore.Set(spec.Key, spec.Value, &exp)
	} else {
		e.deps.KVStore.Set(spec.Key, spec.Value, nil)
	}

	if e.deps.KVStore.Error() != nil {
		return fmt.Errorf("ERR: %w", e.deps.KVStore.Error())
	}
	res.Set(NewEncoder().Ok(), nil, false)
	return nil
}

func (spec *TYPESpecs) Execute(e *executor, req Request, res Response) error {
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
		return &ErrEncodingFailed{}
	}
	res.Set(data, nil, false)
	return nil
}

func (spec *XADDSpecs) Execute(e *executor, req Request, res Response) error {
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
		return err
	}
	res.Set(NewEncoder().BulkString(&generatedId), nil, false)
	return nil
}

func (spec *XRANGESpecs) Execute(e *executor, req Request, res Response) error {
	found, streams, err := e.deps.StreamStore.GetStream(
		spec.Key,
		spec.StartId,
		true,
		spec.EndId,
	)
	if err != nil {
		return err
	}
	if found == 0 {
		res.Set(NewEncoder().NullArray(), nil, false)
		return nil
	}
	res.Set(NewEncoder().Array(streams...), nil, false)
	return nil
}

func (spec *XREADSpecs) Execute(e *executor, req Request, res Response) error {
	if spec.BlockTime != nil {
		type streamData struct {
			streams []Token
			err     error
		}
		streamChan := make(chan streamData)
		go func() {
			for {
				// TODO: improve this
				if spec.StreamIds[1] == "$" {
					streamId := e.deps.StreamStore.GetLatestStreamId()
					spec.StreamIds[1] = streamId
				}
				found, streams, err := e.deps.StreamStore.ReadSingleStream(spec.StreamIds, false)
				if err != nil {
					streamChan <- streamData{
						err: err,
					}
					break
				}
				if found != 0 {
					streamChan <- streamData{
						streams: streams,
					}
					break
				}
			}
		}()
		if *spec.BlockTime == 0 {
			data := <-streamChan
			close(streamChan)
			if data.err != nil {
				return data.err
			}
			res.Set(NewEncoder().Array(data.streams...), nil, false)
			return nil
		} else {
			timer := time.NewTicker(time.Duration(*spec.BlockTime) * time.Millisecond)
			select {
			case <-timer.C:
				// Timeout
				timer.Stop()
				close(streamChan)
				res.Set(NewEncoder().NullArray(), nil, false)
				return nil
			case data := <-streamChan:
				// Data found!
				timer.Stop()
				close(streamChan)
				if data.err != nil {
					return data.err
				}
				res.Set(NewEncoder().Array(data.streams...), nil, false)
				return nil
			}
		}
	} else {
		found, streams, err := e.deps.StreamStore.ReadSingleStream(spec.StreamIds, true)
		if err != nil {
			return err
		}
		if found == 0 {
			res.Set(NewEncoder().NullArray(), nil, false)
		} else {
			res.Set(NewEncoder().Array(streams...), nil, false)
		}
	}
	return nil
}

func (spec *LPOPSpecs) Execute(e *executor, req Request, res Response) error {
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
	res.Set(data, nil, false)
	return nil
}

func (spec *BLPOPSpecs) Execute(e *executor, req Request, res Response) error {
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
	res.Set(NewEncoder().Array(tokens...), nil, false)
	return nil
}

func (s *SUBSCRIBESpecs) Execute(e *executor, req Request, res Response) error {
	sub, count, err := e.deps.SubManager.Subscribe(s.Key, req.ClientId())
	if err != nil {
		return fmt.Errorf("ERR: %w", err)
	}
	tokens := []Token{
		NewToken(BULK_STRING, "subscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, count),
	}
	res.Set(NewEncoder().Array(tokens...), sub, false)
	return nil
}

func (s *PUBLISHSpecs) Execute(e *executor, req Request, res Response) error {
	count := e.deps.SubManager.Publish(s.Key, s.Message)
	res.Set(NewEncoder().Integer(count), nil, false)
	return nil
}

func (s *UNSUBSCRIBESpecs) Execute(e *executor, req Request, res Response) error {
	count := e.deps.SubManager.Cancel(req.ClientId(), s.Key)
	tokens := []Token{
		NewToken(BULK_STRING, "unsubscribe"),
		NewToken(BULK_STRING, s.Key),
		NewToken(INTEGER, count),
	}
	res.Set(NewEncoder().Array(tokens...), nil, false)
	return nil
}

func (s *AUTHSpecs) Execute(e *executor, req Request, res Response) error {
	var data []byte
	if e.deps.Auth.Authenticate(req.AuthCtx(), s.Username, s.Password) {
		data = NewEncoder().Ok()
	} else {
		return &ErrAuthWrongPassword{}
	}
	res.Set(data, nil, false)
	return nil
}

func (s *ACL_SETUSERSpecs) Execute(e *executor, req Request, res Response) error {
	enc := NewEncoder()
	for _, a := range s.Rules {
		char := a[0]
		switch char {
		case '>':
			e.deps.Auth.SetPassword(s.Username, a[1:])
			// SetPassword
		default:
			return &ErrUnsupportedDataType{
				cmd:      ACL_SETUSER,
				dataType: s.Rules,
			}
			// tobe implemented
		}
	}
	res.Set(enc.Ok(), nil, false)
	return nil
}

func (s *ACL_WHOAMISpecs) Execute(e *executor, req Request, res Response) error {
	currentUser := req.AuthCtx().user
	res.Set(NewEncoder().BulkString(&currentUser), nil, false)
	return nil
}

func (s *ACL_GETUSERSpecs) Execute(e *executor, req Request, res Response) error {
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
	res.Set(NewEncoder().Array(tokens...), nil, false)
	return nil
}

func (s *ZRANKSpecs) Execute(e *executor, req Request, res Response) error {
	var rank int
	rank = e.deps.SortedSet.Rank(s.Key, s.Value)
	var data []byte
	if rank == -1 {
		data = NewEncoder().BulkString(nil)
	} else {
		data = NewEncoder().Integer(int(rank))
	}
	res.Set(data, nil, false)
	return nil
}

func (s *ZRANGESpecs) Execute(e *executor, req Request, res Response) error {
	elems := e.deps.SortedSet.Range(s.Key, s.Start, s.End)
	tkns := []Token{}
	for _, e := range elems {
		tkns = append(tkns, NewToken(BULK_STRING, e))
	}
	res.Set(NewEncoder().Array(tkns...), nil, false)
	return nil
}

func (s *ZSCORESpecs) Execute(e *executor, req Request, res Response) error {
	scr := e.deps.SortedSet.Get(s.Key, s.Value)
	res.Set(NewEncoder().BulkString(scr), nil, false)
	return nil
}

func (s *ZCARDSpecs) Execute(e *executor, req Request, res Response) error {
	card := e.deps.SortedSet.Cardinality(s.Key)
	res.Set(NewEncoder().Integer(card), nil, false)
	return nil
}

func (s *ZREMSpecs) Execute(e *executor, req Request, res Response) error {
	card := e.deps.SortedSet.Remove(s.Key, s.Value)
	res.Set(NewEncoder().Integer(card), nil, false)
	return nil
}

func (s *ZADDSpecs) Execute(e *executor, req Request, res Response) error {
	newLen := e.deps.SortedSet.Add(s.Key, s.Value, s.Score)
	res.Set(NewEncoder().Integer(int(newLen)), nil, false)
	return nil
}

func (s *WATCHSpecs) Execute(e *executor, req Request, res Response) error {
	enc := NewEncoder()
	var data []byte
	if req.TX().IsMulti() {
		return &ErrWatchInsideMulti{}
	} else {
		e.deps.Watcher.Add(req.ClientId(), s.Keys...)
		data = enc.Ok()
	}
	res.Set(data, nil, false)
	return nil
}

func (s *UNWATCHSpecs) Execute(e *executor, req Request, res Response) error {
	enc := NewEncoder()
	e.deps.Watcher.Cancel(req.ClientId())
	res.Set(enc.Ok(), nil, false)
	return nil
}

func (s *GEOADDSpecs) Execute(e *executor, req Request, res Response) error {
	loc := Location{
		Lat: s.Lat,
		Lng: s.Lng,
	}
	if !ValidateCoords(loc) {
		return &ErrInvalidCoords{
			Lat: s.Lat,
			Lng: s.Lng,
		}
	}
	score := Score(loc)
	zaddSpec := ZADDSpecs{
		Key:   s.Key,
		Value: s.Member,
		Score: float64(score),
	}
	zaddSpec.Execute(e, req, res)
	return nil
}

func (s *GEOPOSSpecs) Execute(e *executor, req Request, res Response) error {
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
	res.Set(NewEncoder().Array(responses...), nil, false)
	return nil
}

func (s *GEODISTSpecs) Execute(e *executor, req Request, res Response) error {
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
	res.Set(NewEncoder().BulkString(&dist), nil, false)
	return nil
}

func (s *GEOSEARCHSpecs) Execute(e *executor, req Request, res Response) error {
	places := e.deps.SortedSet.List(s.Place)
	placesInRadius := []Token{}
	for p, v := range places {
		loc2 := LatLng(uint64(v.score))
		dist := Dist(s.FromLatLng, loc2)
		if dist <= float64(s.Radius) {
			placesInRadius = append(placesInRadius, NewToken(BULK_STRING, p))
		}
	}
	res.Set(NewEncoder().Array(placesInRadius...), nil, false)
	return nil
}
