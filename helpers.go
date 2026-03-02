package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func validateStreamId(streams map[string][]Stream, key, id string) (bool, error) {
	streamList := streams[key]
	currSplitParts := strings.Split(id, "-")
	currMiliSeconds, err := strconv.Atoi(currSplitParts[0])
	if err != nil {
		return false, err
	}
	currSequence, err := strconv.Atoi(currSplitParts[1])
	if err != nil {
		return false, err
	}

	if len(streamList) > 0 {
		prevStream := streamList[len(streamList)-1]
		prevSplitParts := strings.Split(prevStream.StreamID, "-")
		prevMiliSeconds, err := strconv.Atoi(prevSplitParts[0])
		if err != nil {
			return false, err
		}
		prevSequence, err := strconv.Atoi(prevSplitParts[1])
		if err != nil {
			return false, err
		}

		if currMiliSeconds > prevMiliSeconds {
			return true, nil
		} else if currMiliSeconds == prevMiliSeconds {
			return currSequence > prevSequence, nil
		} else {
			return false, nil
		}
	}
	miliSecondsNotZero := currMiliSeconds > 0 || (currMiliSeconds == 0 && currSequence >= 1)
	currSeqNotZero := currSequence >= 1

	if currMiliSeconds == 0 && currSequence == 0 {
		return false, nil
	}

	return miliSecondsNotZero && currSeqNotZero, nil
}

func generateStreamId(stream map[string][]Stream, key, id string) (string, error) {

	// If the stream is empty for a given time part, the sequence number starts at 0.
	// If there are already entries with the same time part, the new sequence number is the last sequence number plus 1.
	// The only exception is when the time part is 0. In that case, the default sequence number starts at 1.

	streamList := stream[key]
	idType := getStreamIdType(id)

	switch idType {
	case 1:
		// Explicit StreamId
		valid, err := validateStreamId(stream, key, id)
		if err != nil || !valid {
			return "", errors.New("StreamId is not valid.")
		}
		return id, nil
	case 2:
		// Partially Generate StreamId
		if len(streamList) > 0 {
			prevStream := streamList[len(streamList)-1]
			prevSplitParts := strings.Split(prevStream.StreamID, "-")
			prevMiliSeconds, err := strconv.Atoi(prevSplitParts[0])
			if err != nil {

				return "", errors.New("Unable to convert prev miliseconds to integer")
			}
			prevSequence, err := strconv.Atoi(prevSplitParts[1])
			if err != nil {

				return "", err
			}

			currSplitParts := strings.Split(id, "-")
			currMiliSeconds, err := strconv.Atoi(currSplitParts[0])

			if currMiliSeconds > prevMiliSeconds {
				newId := fmt.Sprintf("%d-%d", currMiliSeconds, 0)
				return newId, nil
			} else if currMiliSeconds == prevMiliSeconds {
				newId := fmt.Sprintf("%d-%d", currMiliSeconds, prevSequence+1)
				return newId, nil
			} else {
				return "", errors.New("Invalid StreamId")
			}

		} else {
			return "0-1", nil
		}
	case 3:
		// Auto-Generate StreamId
		currTime := time.Now().UnixMilli()
		newId := fmt.Sprintf("%d-%d", currTime, 0)
		return newId, nil

	}
	return "0-1", errors.New("Invalid StreamId format")
}

func getStreamIdType(id string) int16 {

	// 1- Explicit (1526919030473-0)
	// 2- Auto-generate only the sequence number (1526919030474-*)
	// 3- Auto-generate the time part and sequence number (*)

	if id[0] == '*' {
		return 3
	}

	splitParts := strings.Split(id, "-")
	seq := splitParts[1]

	if seq == "*" {
		return 2
	}

	return 1
}

func splitStreamId(id string) (miliseconds, sequence int) {
	splitParts := strings.Split(id, "-")
	miliseconds, err := strconv.Atoi(splitParts[0])
	if err != nil {
		fmt.Println(err)
		return 0, 0
	}
	sequence, err = strconv.Atoi(splitParts[1])
	if err != nil {
		fmt.Println(err)
		return 0, 0
	}
	return miliseconds, sequence
}

func inRangeStreamId(curr, start, end string) bool {
	currMS, currSeq := splitStreamId(curr)
	startMS, startSeq := splitStreamId(start)
	endMS, endSeq := splitStreamId(end)

	msInRange := currMS >= startMS && currMS <= endMS
	seqInRange := currSeq >= startSeq && currSeq <= endSeq
	return msInRange && seqInRange
}

func filterEntries(entries []Stream, lastID string) []Stream {
	var filtered []Stream
	for _, e := range entries {
		if e.StreamID > lastID {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func resolveStartIdForXREAD(key, id string) string {
	if id == "$" {
		dbVal, ok := DB.Load(key)
		if !ok {
			return "0-0"
		}
		data := dbVal.(Data)
		streams, ok := data.Content.(map[string][]Stream)
		if !ok {
			return "0-0"
		}
		stream := streams[key]
		if len(stream) == 0 {
			return "0-0"
		}
		lastId := stream[len(stream)-1].StreamID
		ms, seq := splitStreamId(lastId)
		return fmt.Sprintf("%d-%d", ms, seq+1)
	}
	if strings.Contains(id, "-") {
		ms, seq := splitStreamId(id)
		return fmt.Sprintf("%d-%d", ms, seq+1)
	}
	return id + "-0"
}

func getDataType(data Data) string {
	switch data.Content.(type) {
	case string:
		return "string"
	case []string:
		return "list"
	case map[string][]Stream:
		return "stream"
	}
	return "unknown"
}

func (s *Server) inMulti() bool {
	return s.TxQueue != nil
}

func (s *Server) addToMulti(command string, args []string) {
	s.TxQueue = append(s.TxQueue, append([]string{command}, args...))
}

func (s *Server) maybeQueue(cmd string, args []string, fn func() []byte) []byte {
	if s.inMulti() {
		s.addToMulti(cmd, args)
		return EncodeSimpleString("QUEUED")
	}
	return fn()
}

func readRDB() []byte {
	content := []byte("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	decodedContent, err := decodeBase64(content)
	if err != nil {
		fmt.Println("Error decoding RDB content:", err)
		return nil
	}
	return decodedContent
}

func decodeBase64(encoded []byte) ([]byte, error) {
	dec := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(dec, encoded)
	if err != nil {
		return nil, err
	}
	return dec[:n], nil
}
