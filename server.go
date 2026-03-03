package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func runServer(ip string, port int, replica bool) {
	url := fmt.Sprintf("%s:%d", ip, port)
	l, err := net.Listen("tcp", url)
	if err != nil {
		fmt.Printf("Failed to bind to port %d", port)
		os.Exit(1)
	}
	defer l.Close()
	shared := Server{SType: ServerType{ip: ip, port: port}}
	if replica {
		shared.SType.Role = "slave"
	} else {
		shared.SType.Role = "master"
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}
		s := &Server{
			Conn:   conn,
			SType:  shared.SType,
			Master: &shared,
		}
		go s.handleConn()
	}
}

func (s *Server) handleConn() {
	defer s.Conn.Close()

	for {
		command, args := s.readCommand()
		go s.handleCommands(command, args)
	}
}

func (s *Server) readCommand() (command string, args []string) {
	buf := make([]byte, 1024)
	n, err := s.Conn.Read(buf)
	if err != nil {
		return
	}

	fullInput := buf[:n]
	decodedInput, err := Parse(string(fullInput))
	if err != nil {
		fmt.Println(err)
		return "", nil
	}

	commandArgsAny, ok := decodedInput.([]any)
	if !ok {
		fmt.Println("Unable to convert decoded input to []any")
		return "", nil
	}

	commandArgs := make([]string, len(commandArgsAny))
	for i, v := range commandArgsAny {
		s, ok := v.(string)
		if !ok {
			fmt.Println("Element is not a string:", v)
			return "", nil
		}
		commandArgs[i] = s
	}
	command = commandArgs[0]
	args = commandArgs[1:]
	return command, args
}

type commandHandler func(*Server, []string) []byte

var commandHandlers = map[string]commandHandler{
	"PING":     func(s *Server, args []string) []byte { return s.Ping("PONG") },
	"ECHO":     func(s *Server, args []string) []byte { return s.Echo(args[0]) },
	"SET":      func(s *Server, args []string) []byte { return s.Set(args) },
	"GET":      func(s *Server, args []string) []byte { return s.Get(args[0]) },
	"RPUSH":    func(s *Server, args []string) []byte { return s.RPush(args) },
	"LRANGE":   func(s *Server, args []string) []byte { return s.LRange(args) },
	"LPUSH":    func(s *Server, args []string) []byte { return s.LPush(args) },
	"LLEN":     func(s *Server, args []string) []byte { return s.LLen(args) },
	"LPOP":     func(s *Server, args []string) []byte { return s.LPop(args) },
	"BLPOP":    func(s *Server, args []string) []byte { return s.BLPop(args) },
	"TYPE":     func(s *Server, args []string) []byte { return s.Type(args) },
	"XADD":     func(s *Server, args []string) []byte { return s.XADD(args) },
	"XRANGE":   func(s *Server, args []string) []byte { return EncodeStream(s.XRANGE(args)) },
	"XREAD":    func(s *Server, args []string) []byte { return s.XREAD(args) },
	"INCR":     func(s *Server, args []string) []byte { return s.Incr(args) },
	"MULTI":    func(s *Server, args []string) []byte { return s.Multi(args) },
	"EXEC":     func(s *Server, args []string) []byte { return s.Exec() },
	"DISCARD":  func(s *Server, args []string) []byte { return s.Discard() },
	"INFO":     func(s *Server, args []string) []byte { return s.Info(args) },
	"REPLCONF": func(s *Server, args []string) []byte { return s.ReplConf(args) },
	"PSYNC":    func(s *Server, args []string) []byte { return s.PSync(args) },
}

func (s *Server) handleCommands(command string, args []string) {
	handler, ok := commandHandlers[strings.ToUpper(command)]
	var response []byte
	if ok {
		response = handler(s, args)
	} else {
		response = []byte("-ERR unknown command\r\n")
	}
	s.Conn.Write(response)
}
