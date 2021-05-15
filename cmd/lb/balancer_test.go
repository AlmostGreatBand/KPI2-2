package main

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type testCase struct {
	serverPool ServerPool
	server *Server
	err error
}

func TestBalancer(t *testing.T) {
	testCases := []testCase{
		{
			serverPool: ServerPool{
				Mutex: new(sync.Mutex),
				Servers: []*Server{
					{Url: "server:8000", Connections: 2, Available: false},
					{Url: "server:8001", Connections: 6, Available: true},
					{Url: "server:8002", Connections: 5, Available: true},
				},
			},
			server:     &Server{Url: "server:8002", Connections: 5, Available: true},
			err:      	nil,
		},
		{
			serverPool: ServerPool{
				Mutex: new(sync.Mutex),
				Servers: []*Server{
					{Url: "server:8000", Connections: 2, Available: false},
					{Url: "server:8001", Connections: 6, Available: false},
					{Url: "server:8002", Connections: 5, Available: false},
				},
			},
			server:     nil,
			err:      	errors.New("no available servers"),
		},
		{
			serverPool: ServerPool{
				Mutex: new(sync.Mutex),
				Servers: []*Server{
					{Url: "server:8000", Connections: 0, Available: false},
					{Url: "server:8001", Connections: 1, Available: true},
					{Url: "server:8002", Connections: 0, Available: false},
				},
			},
			server:     &Server{Url: "server:8001", Connections: 1, Available: true},
			err:      	nil,
		},
		{
			serverPool: ServerPool{
				Mutex: new(sync.Mutex),
				Servers: []*Server{
					{Url: "server:8000", Connections: 0, Available: true},
					{Url: "server:8001", Connections: 0, Available: true},
					{Url: "server:8002", Connections: 0, Available: true},
				},
			},
			server:     &Server{Url: "server:8000", Connections: 0, Available: true},
			err:      	nil,
		},
		{
			serverPool: ServerPool{
				Mutex: new(sync.Mutex),
				Servers: []*Server{
					{Url: "server:8000", Connections: 0, Available: false},
					{Url: "server:8001", Connections: 4, Available: true},
					{Url: "server:8002", Connections: 4, Available: true},
				},
			},
			server:     &Server{Url: "server:8001", Connections: 4, Available: true},
			err:      	nil,
		},
	}

	for _, tc := range testCases {
		server, err := tc.serverPool.getMinConnectionsAvailable()
		assert.Equal(t, err, tc.err)
		assert.Equal(t, server, tc.server)
	}
}
