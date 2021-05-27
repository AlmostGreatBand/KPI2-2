package integration

import (
	"encoding/json"
	"fmt"
	models "github.com/AlmostGreatBand/KPI2-2/cmd/common"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 10 * time.Second,
}

func TestBalancer(t *testing.T) {
	resChan := make(chan models.DbResponse)
	for i := 0; i < 10; i++  {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=agb", baseAddress))

			if resp == nil || resp.StatusCode != http.StatusOK || err != nil {
				t.Logf("error %s", err)
			}

			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Logf("Cannot read response body: %v", err)
			}

			var dbResp models.DbResponse
			err = json.Unmarshal(respBody, &dbResp)
			if err != nil {
				t.Logf("Cannot parse respBody to json: %v", err)
			}
			resChan <- dbResp
			resp.Body.Close()
		}()
	}

	for i := 0; i < 10; i++ {
		value := <- resChan
		assert.Equal(t, value.Value, time.Now().Format("2006-01-02"))
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
