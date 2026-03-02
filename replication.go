package main

import (
	"fmt"
	"net"
)

type ReplicationConfig struct {
	MasterHost string
	MasterPort string
	SelfPort   string
}

func (c *ReplicationConfig) MasterAddr() string {
	return net.JoinHostPort(c.MasterHost, c.MasterPort)
}

func Handshake(cfg *ReplicationConfig) (net.Conn, error) {
	conn, err := net.Dial("tcp", cfg.MasterAddr())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}
	pingCmd := EncodeCommand("PING")
	_, err = conn.Write(pingCmd)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send PING: %w", err)
	}

	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read PONG: %w", err)
	}

	lpCmd := EncodeCommand("REPLCONF", "listening-port", cfg.SelfPort)
	_, err = conn.Write(lpCmd)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send REPLCONF: %w", err)
	}

	_, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read PSYNC response: %w", err)
	}

	psyncCmd := EncodeCommand("REPLCONF", "capa", "psync2")
	_, err = conn.Write(psyncCmd)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send PSYNC: %w", err)
	}

	_, err = conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to read PSYNC response: %w", err)
	}

	psync2Cmd := EncodeCommand("PSYNC", "?", "-1")
	_, err = conn.Write(psync2Cmd)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to send PSYNC: %w", err)
	}

	return conn, nil
}
