package main

import (
	"encoding/json"
	"flag"
	"github.com/AlmostGreatBand/KPI2-2/cmd/common"
	"github.com/AlmostGreatBand/KPI2-2/datastore"
	"github.com/AlmostGreatBand/KPI2-2/httptools"
	"github.com/AlmostGreatBand/KPI2-2/signal"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var port = flag.Int("port", 8080, "server port")
var dir = flag.String("dir", ".", "database storage dir")

func main() {
	h := new(http.ServeMux)
	db, err := datastore.NewDb(*dir)
	if err != nil {
		log.Printf("cannot create database instance: %v\n", err)
		return
	}

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		key := strings.TrimPrefix(r.URL.Path, "/db/")
		if r.Method == http.MethodGet {
			value, err := db.Get(key)
			if err == datastore.ErrNotFound || value == "" {
				log.Printf("cannot find record: %v\n", err)
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			if err != nil {
				log.Printf("cannot get record: %v\n", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			b, err := json.Marshal(models.DbResponse{Key: key, Value: value})
			if err != nil {
				log.Printf("cannot create json: %v\n", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = rw.Write(b)
			if err != nil {
				log.Printf("cannot write response to rw: %v", err)
				return
			}
		} else if r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			defer r.Body.Close()

			if err != nil {
				log.Printf("cannot read request body: %v", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			var req models.DbRequest
			err = json.Unmarshal(body, &req)
			if err != nil {
				log.Printf("cannot read request body: %v", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			err = db.Put(key, req.Value)
			if err != nil {
				log.Printf("cannot put value to database: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			rw.WriteHeader(http.StatusOK)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
