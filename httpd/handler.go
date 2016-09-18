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
	influxHttpd "github.com/influxdata/influxdb/services/httpd"
	"github.com/julienschmidt/httprouter"
	"github.com/thbourlove/outflow/client"
)

type Handler struct {
	client *client.Client
}

func NewHandler(c *client.Client) *Handler {
	return &Handler{client: c}
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

func (h *Handler) query(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	qp := strings.TrimSpace(r.FormValue("q"))
	if qp == "" {
		responseError(w, Error{http.StatusBadRequest, `messing required parameter "q"`})
		return
	}
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	p := influxql.NewParser(strings.NewReader(qp))
	db := r.FormValue("db")
	if db == "" {
		responseError(w, Error{http.StatusBadRequest, "database name required"})
		return
	}

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

	results := h.client.Query(query, options)

	resp := influxHttpd.Response{Results: make([]*influxql.Result, 0)}

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

func (h *Handler) write(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

	precision := r.URL.Query().Get("precision")

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			responseJson(w, http.StatusOK, nil)
			return
		}
		responseError(w, Error{http.StatusBadRequest, "unable to parse points"})
		return
	}

	h.client.Write(database, level, precision, points)

	responseJson(w, http.StatusNoContent, nil)
}

func (h *Handler) ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusNoContent)
}
