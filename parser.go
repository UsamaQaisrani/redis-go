package main

import (
	"bytes"
	"errors"
	"strconv"
	"unicode"
)

func Parse(input string) (any, error) {
	decodedData, _, err := decode(input, 0)
	if err != nil {
		return nil, err
	}
	return decodedData, nil
}

func decode(input string, pos int) (any, int, error) {
	switch input[pos] {
	case '*':
		return decodeArray(input, pos)
	case '$':
		return decodeString(input, pos)
	}
	return nil, pos, errors.New("unable to decode the input")
}

func decodeString(input string, pos int) (string, int, error) {
	pos++
	start := pos
	for unicode.IsNumber(rune(input[pos])) {
		pos++
	}
	length, err := strconv.Atoi(input[start:pos])
	if err != nil {
		return input, pos, errors.New("Unable to get the string length.")
	}
	// Empty bulk string
	if length == 0 {
		pos++
		return "", pos, nil
	}

	// Skipping the \r\n
	pos += 2
	res := input[pos : pos+length]
	pos += length + 2
	return res, pos, nil
}

func decodeArray(input string, pos int) ([]any, int, error) {
	pos++
	start := pos
	for unicode.IsNumber(rune(input[pos])) {
		pos++
	}
	arrLength, err := strconv.Atoi(input[start:pos])
	if err != nil {
		return nil, pos, errors.New("Unable to get the array length.")
	}
	arr := make([]any, 0, arrLength)
	// Skipping the \r\n
	pos += 2

	for i := 0; i < arrLength; i++ {
		elem, newPos, err := decode(input, pos)
		if err != nil {
			return nil, pos, err
		}
		arr = append(arr, elem)
		pos = newPos
	}
	return arr, pos, nil
}

func EncodeBulkString(input string) []byte {
	length := strconv.Itoa(len(input))
	res := "$" + length + "\r\n" + input + "\r\n"
	return []byte(res)
}

func EncodeSimpleString(input string) []byte {
	res := "+" + input + "\r\n"
	return []byte(res)
}

func EncodeInt(input int) []byte {
	res := ":" + strconv.Itoa(input) + "\r\n"
	return []byte(res)
}

func EncodeList(input []string) []byte {
	length := strconv.Itoa(len(input))
	str := "*" + length + "\r\n"
	for _, item := range input {
		currLength := strconv.Itoa(len(item))
		currItem := "$" + currLength + "\r\n" + item + "\r\n"
		str += currItem
	}
	return []byte(str)
}

func EncodeCommand(args ...string) []byte {
	return EncodeList(args)
}

func EncodeSimpleError(input string) []byte {
	res := "-" + input + "\r\n"
	return []byte(res)
}

func EncodeStream(input []Stream) []byte {
	res := "*" + strconv.Itoa(len(input)) + "\r\n"
	for _, stream := range input {
		res += "*2\r\n"
		res += string(EncodeBulkString(stream.StreamID))
		var kvPairs []string
		for key, value := range stream.KeyValuePairs {
			kvPairs = append(kvPairs, key, value)
		}
		res += string(EncodeList(kvPairs))
	}
	return []byte(res)
}

func EncodeXREADResponse(input map[string][]Stream, keys []string) []byte {
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(len(keys)) + "\r\n")
	for _, key := range keys {
		val := input[key]
		buf.WriteString("*2\r\n")
		buf.Write(EncodeBulkString(key))
		buf.Write(EncodeStream(val))
	}

	return buf.Bytes()
}

func EncodeRDBResponse(content []byte) []byte {
	length := strconv.Itoa(len(content))
	// Redis replication uses $<length>\r\n + raw bytes (no trailing \r\n)
	res := "$" + length + "\r\n" + string(content)
	return []byte(res)
}
