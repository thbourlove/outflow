package main

import (
	"fmt"
	"log"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
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
