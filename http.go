package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/julienschmidt/httprouter"
)

type HttpServer struct {
}

func NewHttpServer() (*HttpServer, error) {
	return &HttpServer{}, nil
}

func (s *HttpServer) Start() error {
	router := httprouter.New()
	router.GET("/query", queryHandler)
	addr := "0.0.0.0:10086"
	return http.ListenAndServe(addr, router)
}

func (s *HttpServer) Stop() error {
	return nil
}

type Error struct {
	code int
	Msg  string
}

var (
	ErrBadRequest     = &Error{http.StatusBadRequest, "Bad Request"}
	ErrNotFound       = &Error{http.StatusNotFound, "Not Found"}
	ErrInternalServer = &Error{http.StatusInternalServerError, "Internal Server Error"}
)

func responseError(w http.ResponseWriter, e Error) {
	responseJson(w, e.code, e)
}

func responseJson(w http.ResponseWriter, code int, v interface{}) {
	bytes, err := json.Marshal(v)
	if err != nil {
		log.Printf("marshal response %v: %v", v, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
	w.WriteHeader(code)
	if v != nil {
		if _, err := w.Write(bytes); err != nil {
			log.Printf("write response %v: %v", bytes, err)
			return
		}
	}
	return
}

func convertToEpoch(r *influxql.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}

func queryHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	qp := strings.TrimSpace(r.FormValue("q"))
	if qp == "" {
		responseError(w, Error{http.StatusBadRequest, `messing required parameter "q"`})
		return
	}
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	p := influxql.NewParser(strings.NewReader(qp))
	db := r.FormValue("db")

	query, err := p.ParseQuery()
	if err != nil {
		responseError(w, Error{http.StatusBadRequest, "error parsing query: " + err.Error()})
		return
	}

	w.Header().Add("Connection", "close")
	w.Header().Add("Content-Type", "application/json")

	options := influxql.ExecutionOptions{
		Database: db,
		ReadOnly: r.Method == "GET",
		NodeID:   nodeID,
	}

	results := executeQuery(query, options)

	resp := httpd.Response{Results: make([]*influxql.Result, 0)}

	w.WriteHeader(http.StatusOK)

	for r := range results {
		if r == nil {
			continue
		}

		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						break
					}
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					rowsMerged++
				}
			}

			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
		} else {
			resp.Results = append(resp.Results, r)
		}
	}

	b, _ := json.MarshalIndent(resp, "", "   ")
	w.Write(b)
}
