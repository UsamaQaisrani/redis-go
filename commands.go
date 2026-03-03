package main

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

func (s *Server) Echo(arg string) []byte {
	encodedResponse := EncodeBulkString(arg)
	return encodedResponse
}

func (s *Server) Set(args []string) []byte {
	return s.maybeQueue("SET", args, func() []byte {
		data := Data{}
	key := args[0]
	val := args[1]
	if len(args) == 4 {
		timeFormat := args[2]
		exp := args[3]
		ms, err := strconv.Atoi(exp)
		if err != nil {
			return EncodeSimpleError("ERR invalid expire value")
		}
		if strings.ToUpper(timeFormat) == "PX" {
			data.ExpiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond).UnixMilli()
		}
	}
		data.Content = val
		DB.Store(key, data)
		s.Propagate("SET", args)
		return EncodeSimpleString("OK")
	})
}
func (s *Server) Get(key string) []byte {
	return s.maybeQueue("GET", []string{key}, func() []byte {
		val, ok := DB.Load(key)
	if !ok {
		return []byte("$-1\r\n")
	}

	data := val.(Data)

	if data.ExpiresAt != 0 {
		if time.Now().UnixMilli() >= data.ExpiresAt {
			DB.Delete(key)
			return []byte("$-1\r\n")
		}
	}

	var encodedResponse []byte
	switch data.Content.(type) {
	case string:
		encodedResponse = EncodeBulkString(data.Content.(string))
	default:
		return []byte("$-1\r\n")
	}
		return encodedResponse
	})
}

func (s *Server) Ping(arg string) []byte {
	encodedResonse := EncodeSimpleString(arg)
	return encodedResonse
}

func (s *Server) RPush(args []string) []byte {
	return s.maybeQueue("RPUSH", args, func() []byte {
		key := args[0]
	vals := args[1:]
	dbVal, ok := DB.Load(key)
	if !ok {
		dbVal = Data{Content: []string{}}
	}
	data := dbVal.(Data)
	list := data.Content.([]string)
	for _, val := range vals {
		list = append(list, val)
	}

	if data.Waiting != nil {
		chs := data.Waiting["RPUSH"]
		if len(chs) > 0 {
			ch := chs[0]
			data.Waiting["RPUSH"] = chs[1:]
			ch <- vals[0]
		}
	}

		data.Content = list
		DB.Store(key, data)
		s.Propagate("RPUSH", args)
		return EncodeInt(len(list))
	})
}

func (s *Server) LRange(args []string) []byte {
	return s.maybeQueue("LRANGE", args, func() []byte {
		key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return EncodeSimpleError("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		return EncodeSimpleError("ERR value is not an integer or out of range")
	}

	dbVal, ok := DB.Load(key)
	if !ok {
		encodedResponse := EncodeList([]string{})
		return encodedResponse
	}
	data := dbVal.(Data)
	list := data.Content.([]string)

	if start < 0 {
		adjustedStart := len(list) + start
		start = max(adjustedStart, 0)
	}

	if end < 0 {
		end = len(list) + end
	}

	if start >= len(list) || start > end {
		list = []string{}
		encodedResponse := EncodeList([]string{})
		return encodedResponse
	}

	if end >= len(list) {
		end = len(list) - 1
	}

		return EncodeList(list[start : end+1])
	})
}

func (s *Server) LPush(args []string) []byte {
	return s.maybeQueue("LPUSH", args, func() []byte {
		key := args[0]
	items := args[1:]
	dbVal, ok := DB.Load(key)
	if !ok {
		dbVal = Data{Content: []string{}}
	}
	data := dbVal.(Data)
	list := data.Content.([]string)
	for _, item := range items {
		list = append([]string{item}, list...)
	}
		data.Content = list
		DB.Store(key, data)
		s.Propagate("LPUSH", args)
		return EncodeInt(len(list))
	})
}

func (s *Server) LLen(args []string) []byte {
	key := args[0]
	dbVal, ok := DB.Load(key)
	if !ok {
		dbVal = Data{Content: []string{}}
	}
	data := dbVal.(Data)
	list := data.Content.([]string)
	encodedResponse := EncodeInt(len(list))
	return encodedResponse
}

func (s *Server) LPop(args []string) []byte {
	return s.maybeQueue("LPOP", args, func() []byte {
		key := args[0]
	itemsToRemove := 1
	if len(args) == 2 {
		itemsToRemove, _ = strconv.Atoi(args[1])
	}
	dbVal, ok := DB.Load(key)
	if !ok {
		return []byte("$-1\r\n")
	}
	data := dbVal.(Data)
	list := data.Content.([]string)
	if len(list) < 1 {
		return []byte("$-1\r\n")
	}

		poppedItem := list[:itemsToRemove]
		data.Content = list[itemsToRemove:]
		DB.Store(key, data)
		s.Propagate("LPOP", args)
		if len(poppedItem) > 1 {
			return EncodeList(poppedItem)
		}
		return EncodeBulkString(poppedItem[0])
	})
}

func (s *Server) BLPop(args []string) []byte {
	return s.maybeQueue("BLPOP", args, func() []byte {
		key := args[0]
	wait, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return EncodeSimpleError("ERR invalid timeout")
	}
	dbVal, ok := DB.Load(key)
	var data Data
	if !ok {
		data = Data{
			Content: make([]string, 0),
			Waiting: make(map[string][]chan string),
		}
	} else {
		data = dbVal.(Data)
		if data.Waiting == nil {
			data.Waiting = make(map[string][]chan string)
		}
	}

	list := data.Content.([]string)
	if len(list) > 0 {
		popped := list[0]
		data.Content = list[1:]
		DB.Store(key, data)
		respList := []string{key, popped}
		return EncodeList(respList)
	}

	var timeOutCh <-chan time.Time
	ch := make(chan string)
	data.Waiting["RPUSH"] = append(data.Waiting["RPUSH"], ch)
	DB.Store(key, data)

	if wait > 0 {
		timeOutCh = time.After(time.Duration(wait * float64(time.Second)))
	}

	var encodedResponse []byte
	select {
	case item := <-ch:
		s.LPop([]string{key})
		respList := []string{key, item}
		encodedResponse = EncodeList(respList)
	case <-timeOutCh:
		encodedResponse = []byte("*-1\r\n")
	}
		return encodedResponse
	})
}
func (s *Server) Type(args []string) []byte {
	key := args[0]
	dbVal, ok := DB.Load(key)
	var encodedResponse []byte
	if !ok {
		return EncodeSimpleString("none")
	}
	data := dbVal.(Data)
	dType := getDataType(data)
	encodedResponse = EncodeSimpleString(dType)
	return encodedResponse
}

func (s *Server) XADD(args []string) []byte {
	return s.maybeQueue("XADD", args, func() []byte {
	key := args[0]
	id := args[1]
	dbVal, ok := DB.Load(key)
	if !ok {
		dbVal = Data{Content: map[string][]Stream{}}
	}

		if id == "0-0" {
			return EncodeSimpleError("ERR The ID specified in XADD must be greater than 0-0")
		}

	data := dbVal.(Data)
	streams := data.Content.(map[string][]Stream)
	var chs []chan string
	if data.Waiting != nil {
		chs = data.Waiting["XADD"]
	}
	if len(chs) > 0 && (data.ExpiresAt > time.Now().UnixMilli() || data.ExpiresAt == 0) {
		ch := chs[0]
		data.Waiting["XADD"] = chs[1:]
		ch <- key
	}

		newId, err := generateStreamId(streams, key, id)
		if err != nil && newId == "" {
			return EncodeSimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

	pairs := map[string]string{}
	i := 2

	for i < len(args) {
		k := args[i]
		v := args[i+1]
		pairs[k] = v
		i += 2
	}

		streams[key] = append(streams[key], Stream{StreamID: newId, KeyValuePairs: pairs})
		data.Content = streams
		DB.Store(key, data)
		propArgs := make([]string, len(args))
		copy(propArgs, args)
		propArgs[1] = newId
		s.Propagate("XADD", propArgs)
		return EncodeBulkString(newId)
	})
}

func (s *Server) XRANGE(args []string) []Stream {
	key := args[0]

	// The command can accept IDs in the format <millisecondsTime>-<sequenceNumber>,
	// but the sequence number is optional. $ means only entries added after the command.
	var start string
	end := args[2]

	if args[1] == "$" {
		start = resolveStartIdForXREAD(key, "$")
	} else if strings.Contains(args[1], "-") {
		if args[1] == "-" {
			// Start from the beginning of the stream
			start = "0-0"
		} else {
			// Start from the id sent by the user
			start = args[1]
		}

	} else {
		start = args[1] + "-0"
	}

	dbVal, ok := DB.Load(key)
	if !ok {
		return nil
	}

	data := dbVal.(Data)
	streams, ok := data.Content.(map[string][]Stream)
	if !ok {
		return nil
	}

	stream := streams[key]

	if !strings.Contains(end, "-") {
		if end == "+" {
			end = start + strconv.Itoa(len(stream))
		} else {
			end += "-" + strconv.Itoa(len(stream))
		}
	}
	var streamSlice []Stream
	for _, entry := range stream {
		if inRangeStreamId(entry.StreamID, start, end) {
			streamSlice = append(streamSlice, entry)
		}
	}

	return streamSlice
}

func (s *Server) XREAD(args []string) []byte {
	shouldBlock := false
	var mid int
	var keys []string
	var ids []string
	var encodedResponse []byte

	if strings.ToLower(args[0]) == "block" {
		// Block the read if there are no entries
		shouldBlock = true
		currArgs := args[2:]
		mid = len(currArgs)/2 + 1
		keys = currArgs[1:mid]
		ids = currArgs[mid:]
		for i, key := range keys {
			ids[i] = resolveStartIdForXREAD(key, ids[i])
		}
	} else {
		mid = len(args)/2 + 1
		keys = args[1:mid]
		ids = args[mid:]
		for i, key := range keys {
			ids[i] = resolveStartIdForXREAD(key, ids[i])
		}
	}

	var buf bytes.Buffer
	streamsMap := map[string][]Stream{}
	buf.WriteString("*" + strconv.Itoa(len(keys)) + "\r\n")
	for i, key := range keys {
		currArgs := []string{key, ids[i], "+"}
		entries := s.XRANGE(currArgs)
		streamsMap[key] = entries
	}

	if len(streamsMap[keys[0]]) == 0 && shouldBlock {
		itemCh := make(chan string)
		var timeOutCh <-chan time.Time

		timeOut, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return EncodeSimpleError("ERR invalid timeout")
		}
		if timeOut > 0 {
			timeOutCh = time.After(time.Duration(timeOut) * time.Millisecond)
		}
		dbVal, ok := DB.Load(keys[0])
		var data Data
		if ok {
			data = dbVal.(Data)
		} else {
			data = Data{Content: map[string][]Stream{}}
		}
		if data.Waiting == nil {
			data.Waiting = make(map[string][]chan string)
		}
		data.Waiting["XADD"] = append(data.Waiting["XADD"], itemCh)
		DB.Store(keys[0], data)

		select {
		case newKey := <-itemCh:
			currArgs := []string{newKey, ids[0], "+"}
			entries := s.XRANGE(currArgs)
			streamsMap[newKey] = entries
			encodedResponse = EncodeXREADResponse(streamsMap, keys)
		case <-timeOutCh:
			encodedResponse = []byte("*-1\r\n")
		}
		return encodedResponse
	}

	encodedResponse = EncodeXREADResponse(streamsMap, keys)

	return encodedResponse
}

func (s *Server) Incr(args []string) []byte {
	return s.maybeQueue("INCR", args, func() []byte {
		key := args[0]
	dbVal, ok := DB.Load(key)
	var data Data
	if !ok {
		data = Data{Content: "0"}
	} else {
		data = dbVal.(Data)
	}
	strVal, ok := data.Content.(string)
	if !ok {
		strVal = "0"
	}
	numVal, err := strconv.Atoi(strVal)
	if err != nil {
		return EncodeSimpleError("ERR value is not an integer or out of range")
	}
		numVal++
		data.Content = strconv.Itoa(numVal)
		DB.Store(key, data)
		s.Propagate("INCR", args)
		return EncodeInt(numVal)
	})
}

func (s *Server) Multi(args []string) []byte {
	s.TxQueue = [][]string{}
	return EncodeSimpleString("OK")
}

func (s *Server) Exec() []byte {
	if s.TxQueue == nil {
		return EncodeSimpleError("ERR EXEC without MULTI")
	}
	queue := s.TxQueue
	s.TxQueue = nil
	var parts []byte
	for _, commandArgs := range queue {
		command := commandArgs[0]
		args := commandArgs[1:]
		var elem []byte
		switch strings.ToUpper(command) {
		case "SET":
			s.Set(args)
			elem = EncodeSimpleString("OK")
		case "GET":
			elem = s.Get(args[0])
		case "INCR":
			elem = s.Incr(args)
		default:
			elem = EncodeSimpleError("ERR unknown command")
		}
		parts = append(parts, elem...)
	}
	return append([]byte("*"+strconv.Itoa(len(queue))+"\r\n"), parts...)
}

func (s *Server) Discard() []byte {
	if s.TxQueue == nil {
		return EncodeSimpleError("ERR DISCARD without MULTI")
	}
	s.TxQueue = nil
	return EncodeSimpleString("OK")
}

func (s *Server) Info(args []string) []byte {
	s.SType.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	s.SType.master_repl_offset = 0
	res := "role:" + s.SType.Role + "\r\n"
	res += "master_replid" + ":" + s.SType.master_replid + "\r\n"
	res += "master_repl_offset" + ":" + strconv.FormatInt(s.SType.master_repl_offset, 10) + "\r\n"
	return EncodeBulkString(res)
}

func (s *Server) ReplConf(args []string) []byte {
	if len(args) < 2 {
		return EncodeSimpleError("ERR wrong number of arguments for 'REPLCONF' command")
	}

	return EncodeSimpleString("OK")
}

func (s *Server) PSync(args []string) []byte {
	s.SType.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	res := "FULLRESYNC " + s.SType.master_replid + " 0"
	s.Conn.Write(EncodeSimpleString(res))
	rdbResponse := EncodeRDBResponse(readRDB())
	if s.Master != nil {
		s.Master.AddReplica(s.Conn)
	}
	return rdbResponse
}

func (s *Server) Propagate(cmd string, args []string) {
	if s.Master == nil || s.SType.Role != "master" {
		return
	}
	encoded := EncodeCommand(append([]string{cmd}, args...)...)
	for _, replica := range s.Master.Replicas() {
		replica.Write(encoded)
	}
}


