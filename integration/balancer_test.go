package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	if err != nil {
		t.Error(err)
	}
	t.Logf("response from [%s]", resp.Header.Get("lb-from"))
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
	}
}
