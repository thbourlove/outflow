package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/julienschmidt/httprouter"
)

type varVisitor struct {
	vars   []*influxql.VarRef
	fields []*influxql.Field
}

func (v *varVisitor) Visit(n influxql.Node) influxql.Visitor {
	switch n := n.(type) {
	case *influxql.VarRef:
		v.vars = append(v.vars, n)
		v.fields = append(v.fields, &influxql.Field{n, ""})
	}
	return v
}

func (v *varVisitor) Fields() influxql.Fields {
	return v.fields
}

func executeQuery(query *influxql.Query, options influxql.ExecutionOptions) <-chan *influxql.Result {
	results := make(chan *influxql.Result)
	go func() {
		defer close(results)

		clients := []client.Client{}
		addrs := []string{"http://localhost:8086", "http://localhost:9086"}

		for _, addr := range addrs {
			c, err := client.NewHTTPClient(client.HTTPConfig{
				Addr: addr,
			})
			if err != nil {
				results <- &influxql.Result{Err: err}
				return
			}
			clients = append(clients, c)
		}

		stmt, ok := query.Statements[0].(*influxql.SelectStatement)
		log.Printf("main select statement: %v", stmt)
		if !ok {
			results <- &influxql.Result{Err: fmt.Errorf("not a select stmt.")}
			return
		}

		ic, err := NewIteratorCreator(clients)
		if err != nil {
			results <- &influxql.Result{Err: fmt.Errorf("new iterator creator: %v", err)}
			return
		}
		itrs, err := influxql.Select(stmt, ic, nil)
		if err != nil {
			results <- &influxql.Result{Err: fmt.Errorf("select: %v", err)}
			return
		}

		em := influxql.NewEmitter(itrs, stmt.TimeAscending(), 0)
		em.Columns = stmt.ColumnNames()
		em.OmitTime = stmt.OmitTime
		defer em.Close()

		var emitted bool

		for {
			row, err := em.Emit()
			if err != nil {
				results <- &influxql.Result{Err: fmt.Errorf("emitter emit: %v", err)}
				return
			} else if row == nil {
				break
			}

			result := &influxql.Result{
				Series: []*models.Row{row},
			}

			results <- result

			emitted = true
		}

		if !emitted {
			results <- &influxql.Result{
				Series: make([]*models.Row, 0),
			}
		}

		return
	}()
	return results
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
