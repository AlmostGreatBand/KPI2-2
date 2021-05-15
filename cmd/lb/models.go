package main

import (
	"fmt"
	"sync"
)

type Server struct {
	Url         string
	Connections int32
	Available   bool
}

type ServerPool struct {
	Mutex   *sync.Mutex
	Servers []*Server
}

func (s *Server) toString() string {
	return fmt.Sprintf("Url: %s; Conn: %d; Available: %v", s.Url, s.Connections, s.Available)
}

func (sp *ServerPool) toString() string {
	res := ""
	for i, s := range sp.Servers {
		res += fmt.Sprintf("%v: %v \n", i, s.toString())
	}
	return res
}
