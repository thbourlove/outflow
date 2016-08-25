package main

import (
	"bytes"
	"compress/gzip"
	"log"
	"net/http"
	"strconv"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/julienschmidt/httprouter"
	"github.com/serialx/hashring"
)

func writeHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	database := r.URL.Query().Get("db")
	body := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			responseError(w, Error{http.StatusBadRequest, "failed to read body as gzip format"})
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if clStr := r.Header.Get("Content-Length"); clStr != "" {
		if length, err := strconv.Atoi(clStr); err == nil {
			bs = make([]byte, 0, length)
		}
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		responseError(w, Error{http.StatusBadRequest, "uanble to read bytes from request body"})
		return
	}

	level := r.URL.Query().Get("consistency")
	if level != "" {
		_, err := models.ParseConsistencyLevel(level)
		if err != nil {
			responseError(w, Error{http.StatusBadRequest, "failed to parse consistency level"})
			return
		}
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), r.URL.Query().Get("precision"))
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			responseJson(w, http.StatusOK, nil)
			return
		}
		responseError(w, Error{http.StatusBadRequest, "unable to parse points"})
		return
	}

	addrs := []string{"http://localhost:8086", "http://localhost:9086"}
	ring := hashring.New(addrs)
	batchMap := map[string]client.BatchPoints{}
	for _, point := range points {
		log.Println(point)
		key := point.Tags().HashKey()
		if node, ok := ring.GetNode(string(key)); ok {
			var batch client.BatchPoints
			var ok bool
			if batch, ok = batchMap[node]; !ok {
				var err error
				batch, err = client.NewBatchPoints(client.BatchPointsConfig{
					Precision:        r.URL.Query().Get("precision"),
					Database:         database,
					WriteConsistency: level,
				})
				if err != nil {
					log.Printf("failed to new batch point: %v", err)
					continue
				}
				batchMap[node] = batch
			}
			clientPoint, err := client.NewPoint(
				point.Name(),
				point.Tags(),
				point.Fields(),
				point.Time(),
			)
			if err != nil {
				log.Printf("failed to new point: %v", err)
			}
			batch.AddPoint(clientPoint)
		}
	}
	for node, batch := range batchMap {
		log.Println(node, batch)
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: node,
		})
		if err != nil {
			log.Printf("failed to new client: %v", err)
		}
		if err := c.Write(batch); err != nil {
			log.Printf("failed to write batch: %v", err)
		}
	}

	responseJson(w, http.StatusNoContent, nil)
}
