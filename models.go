package main

import (
	"net"
	"sync"
)

type DictStringVal struct {
	Value     string
	Expire    string
	CreatedAt int64
}

type Server struct {
	Conn     net.Conn
	mu       sync.Mutex
	BLOCKQ   []Command
	TxQueue  [][]string
	SType    ServerType
	replicas []net.Conn
	Master   *Server
}

func (s *Server) AddReplica(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas = append(s.replicas, conn)
}

func (s *Server) Replicas() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]net.Conn{}, s.replicas...)
}

type Data struct {
	Content   any
	ExpiresAt int64
	Waiting   map[string][]chan string
}

type Command struct {
	Name     string
	WaitTime int64
}

type Stream struct {
	StreamID      string
	KeyValuePairs map[string]string
}

type ServerType struct {
	Role               string
	ip                 string
	port               int
	master_replid      string
	master_repl_offset int64
}
