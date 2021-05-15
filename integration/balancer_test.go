package integration

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 10 * time.Second,
}

func TestBalancer(t *testing.T) {
	/*
		there is no proper way to test this algorithm(with the least connections)
		I've tried manually test this algorithm with 900 ms client delay, and 3 s server delay
		check this here
		https://docs.google.com/spreadsheets/d/1ZjhszAWXc9_hmWr7m9g-Z32HDHwIvSzkUklp81oPRUg/edit?usp=sharing
	 */

	serverUrls := []string {
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}

	expectedResults := []int32 { 0, 1, 2, 0, 0, 1, 2, 0, 0, 1 }

	serverNames := make(chan string, len(expectedResults))

	for range expectedResults {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))

			if resp == nil || resp.StatusCode != http.StatusOK || err != nil {
				t.Logf("error %s", err)
			}

			server := resp.Header.Get("lb-from")
			t.Logf("response from [%s]", server)
			serverNames <- server
		}()
		time.Sleep(900 * time.Millisecond)
	}

	for _, i := range expectedResults {
		server := <- serverNames
		assert.Equal(t, server, serverUrls[i])
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
	}
}
