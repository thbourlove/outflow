package client

import (
	"fmt"
	"log"
	"sync"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/serialx/hashring"
)

type Client struct {
	mu     sync.Mutex
	config Config

	creatorMap map[string]influxql.IteratorCreator

	upstreams   []client.Client
	upstreamMap map[string]client.Client

	ring interface {
		GetNode(string) (string, bool)
	}
}

func New(config Config) (*Client, error) {
	c := &Client{config: config}

	addrs := config.Addrs

	c.upstreamMap = make(map[string]client.Client, len(addrs))
	for _, addr := range addrs {
		influxClient, err := client.NewHTTPClient(client.HTTPConfig{
			Addr: addr,
		})
		if err != nil {
			return nil, fmt.Errorf("new http client: %v", err)
		}
		c.upstreams = append(c.upstreams, influxClient)
		c.upstreamMap[addr] = influxClient
	}

	c.ring = hashring.New(addrs)
	c.creatorMap = make(map[string]influxql.IteratorCreator)

	return c, nil
}

func (c *Client) Query(query *influxql.Query, options influxql.ExecutionOptions) <-chan *influxql.Result {
	results := make(chan *influxql.Result)
	go func() {
		defer close(results)
		var ok bool

		stmt, ok := query.Statements[0].(*influxql.SelectStatement)
		if !ok {
			results <- &influxql.Result{Err: fmt.Errorf("not a select stmt.")}
			return
		}

		c.mu.Lock()
		var ic influxql.IteratorCreator
		if ic, ok = c.creatorMap[options.Database]; !ok {
			var err error
			if ic, err = NewIteratorCreator(c.upstreams, options.Database); err != nil {
				results <- &influxql.Result{Err: fmt.Errorf("new iterator creator: %v", err)}
				c.mu.Unlock()
				return
			}
			c.creatorMap[options.Database] = ic
		}
		c.mu.Unlock()

		tmp, err := stmt.RewriteFields(ic)
		if err != nil {
			results <- &influxql.Result{Err: fmt.Errorf("statement rewrite fields: %v", err)}
			return
		}
		stmt = tmp

		log.Printf("main select statement: %v", stmt)

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

func (c *Client) Write(database string, consistency string, precision string, points []models.Point) error {
	batchMap := map[string]client.BatchPoints{}
	for _, point := range points {
		key := point.Tags().HashKey()
		if node, ok := c.ring.GetNode(string(key)); ok {
			var batch client.BatchPoints
			var ok bool
			if batch, ok = batchMap[node]; !ok {
				var err error
				batch, err = client.NewBatchPoints(client.BatchPointsConfig{
					Precision:        precision,
					Database:         database,
					WriteConsistency: consistency,
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
				continue
			}
			batch.AddPoint(clientPoint)
		}
	}
	for node, batch := range batchMap {
		upstream, ok := c.upstreamMap[node]
		if !ok {
			return fmt.Errorf("client does not exists")
		}
		if err := upstream.Write(batch); err != nil {
			return fmt.Errorf("failed to write batch: %v", err)
		}
	}
	return nil
}
