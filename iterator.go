package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strconv"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
)

func NewIteratorCreator(clients []client.Client) (influxql.IteratorCreator, error) {
	ics := make([]influxql.IteratorCreator, 0)
	for _, client := range clients {
		ic := &RemoteIteratorCreator{client: client}
		ics = append(ics, ic)
	}
	return influxql.IteratorCreators(ics), nil
}

type RemoteIteratorCreator struct {
	client client.Client
}

func (ic *RemoteIteratorCreator) FieldDimensions(sources influxql.Sources) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return nil, nil, nil
}

func (ic *RemoteIteratorCreator) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return nil, nil
}

func (ic *RemoteIteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if call, ok := opt.Expr.(*influxql.Call); ok {
		refOpt := opt
		refOpt.Expr = call.Args[0].(*influxql.VarRef)
		inputs, err := ic.CreateVarRefIterator(refOpt)
		if err != nil {
			return nil, fmt.Errorf("create var ref iterator: %v", err)
		} else if len(inputs) == 0 {
			return nil, fmt.Errorf("create var ref iterator: length of result is zero")
		}

		for i, input := range inputs {
			itr, err := influxql.NewCallIterator(input, opt)
			if err != nil {
				return nil, fmt.Errorf("new call iterator: %v", err)
			}
			inputs[i] = itr
		}

		return influxql.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0)), nil
	}

	itrs, err := ic.CreateVarRefIterator(opt)
	if err != nil {
		return nil, fmt.Errorf("create var ref iterator: %v", err)
	}

	return influxql.NewSortedMergeIterator(itrs, opt), nil
}

func (ic *RemoteIteratorCreator) CreateVarRefIterator(options influxql.IteratorOptions) ([]influxql.Iterator, error) {
	if _, ok := options.Expr.(*influxql.VarRef); !ok {
		return nil, fmt.Errorf("expr is not varref")
	}
	stmt := &influxql.SelectStatement{
		Fields:    []*influxql.Field{&influxql.Field{Expr: options.Expr}},
		Sources:   options.Sources,
		Condition: influxql.CloneExpr(options.Condition),
		Limit:     options.Limit,
		Offset:    options.Offset,
		SLimit:    options.SLimit,
		SOffset:   options.SOffset,
		Fill:      options.Fill,
		FillValue: options.FillValue,
	}
	for _, d := range options.Dimensions {
		stmt.Dimensions = append(stmt.Dimensions, &influxql.Dimension{Expr: &influxql.VarRef{Val: d, Type: influxql.Tag}})
	}
	log.Printf("sub select statement: %v", stmt)

	q := client.Query{
		Command:   stmt.String(),
		Database:  "esm",
		Precision: "s",
	}
	resp, err := ic.client.Query(q)
	if err != nil {
		return nil, err
	}

	if len(resp.Results) != 1 {
		return nil, fmt.Errorf("length of results should be one.")
	}

	result := resp.Results[0]
	itrs := make([]influxql.Iterator, 0, len(result.Series))
	for _, row := range result.Series {
		points := make([]influxql.FloatPoint, 0, len(row.Values))
		for _, value := range row.Values {
			time, _ := strconv.ParseInt(string(value[0].(json.Number)), 10, 64)
			v, _ := strconv.ParseFloat(string(value[1].(json.Number)), 64)
			points = append(points, influxql.FloatPoint{
				Name:  row.Columns[1],
				Tags:  influxql.NewTags(row.Tags),
				Time:  time * 1000000000,
				Value: v,
			})
		}
		itrs = append(itrs, &FloatIterator{
			points: points,
			seek:   0,
		})
	}
	return itrs, nil
}

type FloatIterator struct {
	points []influxql.FloatPoint
	seek   int
	stats  influxql.IteratorStats
}

func (fi *FloatIterator) Stats() influxql.IteratorStats {
	return fi.stats
}

func (fi *FloatIterator) Next() (*influxql.FloatPoint, error) {
	if fi.seek >= len(fi.points) {
		return nil, nil
	}
	v := &fi.points[fi.seek]
	fi.seek += 1
	return v, nil
}

func (fi *FloatIterator) Close() error {
	return nil
}
