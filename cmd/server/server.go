package main

import (
	"bytes"
	"encoding/json"
	"flag"
	models "github.com/AlmostGreatBand/KPI2-2/cmd/common"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/AlmostGreatBand/KPI2-2/httptools"
	"github.com/AlmostGreatBand/KPI2-2/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		keys, ok := r.URL.Query()["key"]
		if !ok || len(keys[0]) < 1 {
			log.Println("Url Param 'key' is missing")
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}
		report.Process(r)

		key := keys[0]
		resp, err := http.DefaultClient.Get("http://dbserver:8080/db/" + key)
		if err != nil {
			log.Printf("Can't get data from dbserver: %v\n", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Can't get body from response: %v\n", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		rw.Header().Set("content-type", "application/json")
		_, err = rw.Write(body)
		if err != nil {
			log.Printf("cannot write response from db to rw: %v", err)
			return
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()

	dbReq := models.DbRequest { Value: time.Now().Format("2006-01-02")}
	body, err := json.Marshal(dbReq)
	if err != nil {
		log.Printf("Can't create reqBody: %v", err)
		return
	}

	_, err = http.DefaultClient.Post(
		"http://dbserver:8080/db/agb",
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		log.Printf("Can't put data to db server: %v", err)
		return
	}

	signal.WaitForTerminationSignal()
}
