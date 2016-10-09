package httpd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/julienschmidt/httprouter"
	"github.com/thbourlove/outflow/client"
)

//Define a Handler which contains an instance of client which is defined in client package
type Handler struct {
	client *client.Client
}

//return a new handler which contains a client just passed into
func NewHandler(c *client.Client) *Handler {
	return &Handler{client: c}
}

//query parse an incoming query and, if valid, execute the query
//In this function, it check the existence of database. If such database is not presented, then this query is invalid.
func (h *Handler) query(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	qp := strings.TrimSpace(r.FormValue("q"))
	if qp == "" {
		responseError(rw, Error{http.StatusBadRequest, `missing required parameter "q"`})
		return
	}
	db := r.FormValue("db")
	if db == "" {
		responseError(rw, Error{http.StatusBadRequest, `missing required parameter "db"`})
		return
	}

	rw.Header().Add("Connection", "close")
	rw.Header().Add("Content-Type", "application/json")

	options := influxql.ExecutionOptions{
		Database: db,
		ReadOnly: r.Method == "GET",
		NodeID:   nodeID,
	}

	//Query via influxDB's client
	resp, _:= h.client.Query(options, qp)

	rw.WriteHeader(http.StatusOK)

	b, _ := json.MarshalIndent(resp, "", "   ")
	rw.Write(b)
}

//write parse an incoming write request and, if valid, write such request into database
func (h *Handler) write(rw http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	database := r.URL.Query().Get("db")
	body := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			responseError(rw, Error{http.StatusBadRequest, "failed to read body as gzip format"})
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
		responseError(rw, Error{http.StatusBadRequest, "uanble to read bytes from request body"})
		return
	}

	level := r.URL.Query().Get("consistency")
	if level != "" {
		_, err := models.ParseConsistencyLevel(level)
		if err != nil {
			responseError(rw, Error{http.StatusBadRequest, "failed to parse consistency level"})
			return
		}
	}

	precision := r.URL.Query().Get("precision")

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			responseJson(rw, http.StatusOK, nil)
			return
		}
		responseError(rw, Error{http.StatusBadRequest, "unable to parse points"})
		return
	}

	if err := h.client.Write(database, level, precision, points); err != nil {
		responseError(rw, Error{http.StatusBadRequest, err.Error()})
	}

	responseJson(rw, http.StatusNoContent, nil)
}

//ping is just used to check server is alive or not
func (h *Handler) ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusNoContent)
}
