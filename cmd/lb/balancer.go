package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/AlmostGreatBand/KPI2-2/httptools"
	"github.com/AlmostGreatBand/KPI2-2/signal"
)

type server struct {
	Url string
	Connections int32
	Available bool
}

type serverPool struct {
	mutex *sync.Mutex
	servers []*server
}

var (
	port = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 4, "request timeout time in seconds")
	https = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout = time.Duration(*timeoutSec) * time.Second
	serverUrls = []string {
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()

	// Set up connections
	var servers []*server
	for _, url := range serverUrls {
		servers = append(servers, &server { Url: url, Available: true, Connections: 0})
	}

	serverPool := serverPool { servers: servers, mutex: new(sync.Mutex) }

	// TODO: Використовуйте дані про стан сервреа, щоб підтримувати список тих серверів, яким можна відправляти ззапит.
	for _, server := range servers {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				server.Available = health(server.Url)
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		fmt.Println(serverPool.toString())

		server, err := serverPool.getMinConnectionsAvailable()
		if err != nil {
			fmt.Print(err.Error())
			return
		}

		forward(server.Url, rw, r)
		server.Connections--
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

func (sp *serverPool) getMinConnectionsAvailable() (*server, error) {
	sp.mutex.Lock()

	var filtered []*server
	for _, server := range sp.servers {
		if server.Available {
			filtered = append(filtered, server)
		}
	}

	if filtered == nil {
		return nil, errors.New("no available servers")
	}

	min := filtered[0]

	for _, server := range filtered[1:] {
		if server.Connections < min.Connections {
			min = server
		}
	}

	min.Connections++
	sp.mutex.Unlock()

	return min, nil
}

func (s *server) toString() string {
	return fmt.Sprintf("Url: %s; Conn: %d; Available: %v", s.Url, s.Connections, s.Available)
}

func (sp *serverPool) toString() string {
	res := ""
	for i, s := range sp.servers {
		res += fmt.Sprintf("%v: %v \n", i, s.toString())
	}
	return res
}
