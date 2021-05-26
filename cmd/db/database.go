package main

import (
	"encoding/json"
	"fmt"
	"github.com/AlmostGreatBand/KPI2-2/datastore"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type DbRequest struct {
	value string
}

type DbResponse struct {
	key string
	value string
}

func main() {
	h := new(http.ServeMux)
	db, err := datastore.NewDb("database/agb")
	if err != nil {
		fmt.Errorf("cannot create database instance: %v", err)
		return
	}

	err = db.Put("agb", time.Now().Format("2006-01-02"))
	if err != nil {
		fmt.Errorf("cannot put value to database: %v", err)
		return
	}

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/db/")
		if r.Method == http.MethodGet {
			value, err := db.Get(key)
			if err == datastore.ErrNotFound || value == "" {
				fmt.Errorf("cannot find record: %v", err)
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			if err != nil {
				fmt.Errorf("cannot get record: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}

			b, err := json.Marshal(DbResponse{key: key, value: value})
			if err != nil {
				fmt.Errorf("cannot create json: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = rw.Write(b)
			if err != nil {
				fmt.Errorf("cannot write response to rw: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusOK)
		} else if r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				fmt.Errorf("cannot read request body: %v", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			var req DbRequest
			err = json.Unmarshal(body, &req)
			if err != nil {
				fmt.Errorf("cannot read request body: %v", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			err = db.Put(key, req.value)
			if err != nil {
				fmt.Errorf("cannot put value to database: %v", err)
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	})
}
