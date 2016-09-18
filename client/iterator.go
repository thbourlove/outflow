package client

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strconv"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
)

func NewIteratorCreator(clients []client.Client, database string) (influxql.IteratorCreator, error) {
	ics := make([]influxql.IteratorCreator, 0)
	for _, c := range clients {
		ic := &RemoteIteratorCreator{client: c, database: database}
		ics = append(ics, ic)
	}
	return influxql.IteratorCreators(ics), nil
}

type RemoteIteratorCreator struct {
	client        client.Client
	fieldsMap     map[string]map[string]influxql.DataType
	dimensionsMap map[string]map[string]struct{}
	database      string
}

func (ic *RemoteIteratorCreator) FieldDimensions(sources influxql.Sources) (map[string]influxql.DataType, map[string]struct{}, error) {
	if len(ic.fieldsMap) == 0 {
		q := client.Query{
			Command:  "show field keys",
			Database: ic.database,
		}
		resp, err := ic.client.Query(q)
		if err != nil {
			return nil, nil, fmt.Errorf("query %v: %v", q, err)
		}
		ic.fieldsMap = make(map[string]map[string]influxql.DataType, len(resp.Results[0].Series))
		for _, row := range resp.Results[0].Series {
			fields := map[string]influxql.DataType{}
			for _, value := range row.Values {
				fieldKey := value[0].(string)
				fieldType := value[1].(string)
				switch fieldType {
				case "float":
					fields[fieldKey] = influxql.Float
				case "integer":
					fields[fieldKey] = influxql.Integer
				case "string":
					fields[fieldKey] = influxql.String
				case "bool":
					fields[fieldKey] = influxql.Boolean
				}
			}
			ic.fieldsMap[row.Name] = fields
		}
	}

	if len(ic.dimensionsMap) == 0 {
		q := client.Query{
			Command:  "show tag keys",
			Database: ic.database,
		}
		resp, err := ic.client.Query(q)
		if err != nil {
			return nil, nil, fmt.Errorf("query %v: %v", q, err)
		}
		ic.dimensionsMap = make(map[string]map[string]struct{}, len(resp.Results[0].Series))
		for _, row := range resp.Results[0].Series {
			dimensions := map[string]struct{}{}
			for _, value := range row.Values {
				tagKey := value[0].(string)
				dimensions[tagKey] = struct{}{}
			}
			ic.dimensionsMap[row.Name] = dimensions
		}
	}

	fields := map[string]influxql.DataType{}
	for _, name := range sources.Names() {
		for fieldKey, fieldType := range ic.fieldsMap[name] {
			fields[fieldKey] = fieldType
		}
	}

	dimensions := map[string]struct{}{}
	for _, name := range sources.Names() {
		for tagKey, _ := range ic.dimensionsMap[name] {
			dimensions[tagKey] = struct{}{}
		}
	}

	return fields, dimensions, nil
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
	stmt := &influxql.SelectStatement{
		Sources:   options.Sources,
		Condition: influxql.CloneExpr(options.Condition),
		Limit:     options.Limit,
		Offset:    options.Offset,
		SLimit:    options.SLimit,
		SOffset:   options.SOffset,
		Fill:      options.Fill,
		FillValue: options.FillValue,
	}

	if len(options.Aux) > 0 {
		//stmt.Fields = options.
		stmt.Fields = []*influxql.Field{}
		for _, ref := range options.Aux {
			stmt.Fields = append(stmt.Fields, &influxql.Field{Expr: &ref})
		}
	} else {
		if _, ok := options.Expr.(*influxql.VarRef); !ok {
			return nil, fmt.Errorf("expr is not VarRef")
		} else {
			stmt.Fields = []*influxql.Field{&influxql.Field{Expr: options.Expr}}
		}
	}

	for _, d := range options.Dimensions {
		stmt.Dimensions = append(stmt.Dimensions, &influxql.Dimension{Expr: &influxql.VarRef{Val: d, Type: influxql.Tag}})
	}
	log.Printf("sub select statement: %v", stmt)

	q := client.Query{
		Command:   stmt.String(),
		Database:  ic.database,
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
			if len(options.Aux) > 0 {
				aux := []interface{}{}
				for i := 1; i < len(value); i++ {
					v, _ := strconv.ParseFloat(string(value[i].(json.Number)), 64)
					aux = append(aux, v)
				}
				points = append(points, influxql.FloatPoint{
					Name: row.Name,
					//Name:  row.Columns[1],
					Tags: influxql.NewTags(row.Tags),
					Time: time * 1000000000,
					Aux:  aux,
				})
			} else {
				v, _ := strconv.ParseFloat(string(value[1].(json.Number)), 64)
				points = append(points, influxql.FloatPoint{
					Name:  row.Columns[1],
					Tags:  influxql.NewTags(row.Tags),
					Time:  time * 1000000000,
					Value: v,
				})
			}
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
