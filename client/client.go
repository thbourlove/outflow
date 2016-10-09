package client

import (
	"fmt"
	"sync"
	"strings"

	client "github.com/influxdata/influxdb/client/v2"
	log "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/serialx/hashring"
)
//Client provides implementation to interface Client which is defined in client
type Client struct {
	config Config     //represents configuration file

	upstreams   []client.Client //represents an array of client that can write/query the database
	upstreamMap map[string]client.Client //represents a mapping relationship between database name and actual client

	ring interface {
		GetNode(string) (string, bool)
	}
}

//New will create a new instance for client according the the configuration file
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

	return c, nil
}

//TODO: need revise Query function in order to support all query functionality. For now, it only support select statement
//TODO: need think about how to redirect the incoming request to correct influxDB's node url
func (c *Client) Query(options influxql.ExecutionOptions, command string) (*client.Response, error) {
	if !strings.Contains(command, "FROM") {
		return nil, fmt.Errorf("Illegal statement %s; measurement name required", command)
	}

	strs := strings.Split(command, " ")
	index := -1
	for i := 0; i < len(strs); i++ {
		index = i
		if ok := strings.Compare(strs[i], "FROM"); ok == 0 {
			break
		}
	}

	measurement := strs[index + 1]
	node, ok:= c.ring.GetNode(options.Database+measurement)
	if !ok {
		return nil, fmt.Errorf("Such server node %s has not been added yet, please try it laster", node)
	}
	//retrieve influxDB's client from map
	nodeC := c.upstreamMap[node]
	query := client.Query{Database:options.Database, Command:command}

	//check such sever is alive or not. If alive, then continue to perform query.
	_, _, err := nodeC.Ping(0)
	if err != nil {
		return nil, fmt.Errorf("Server %s is dead.", node)
	}
	resp, err := nodeC.Query(query)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// makeBatchPointsMap will create a map which define a relationship between node's url and batch.
// For each batch points as value, it key node's url is the destination.
func (c *Client) makeBatchPointsMap(database string, consistency string, precision string, points []models.Point) map[string]client.BatchPoints {
	//how many points will write to influxDB together. This is not thread-safe.
	//BatchPoints is NOT thread-safe, you must create a separate
	batchMap := map[string]client.BatchPoints{}
	for _, point := range points {
		//make a key by concatenating database and measurement
		key := database + point.Name()
		if node, ok := c.ring.GetNode(key); ok {
			log.Debugln(key, "writes into node: ", node)
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
					log.Errorln("failed to new batch point: %v", err)
					continue
				}
				batchMap[node] = batch
			}
			clientPoint, err := client.NewPoint(
				point.Name(),
				//Tags represents a sorted list of tags
				//But in client.NewPoint's second argument is map[string][string]
				//If we want to use tags from points, we actually need convert tags to map
				//This is because the prototype in client and influxql are different
				point.Tags().Map(),
				point.Fields(),
				point.Time(),
			)
			if err != nil {
				log.Errorln("failed to new point: %v", err)
				continue
			}
			batch.AddPoint(clientPoint)
		}
	}
	return batchMap
}

//Write will create a batchMap and write batch points to its key node's url according to such map
func (c *Client) Write(database string, consistency string, precision string, points []models.Point) error {
	batchMap := c.makeBatchPointsMap(database, consistency, precision, points)
	for node, batch := range batchMap {
		upstream, ok := c.upstreamMap[node]
		if !ok {
			return fmt.Errorf("client %s does not exists", upstream)
		}
		if err := upstream.Write(batch); err != nil {
			return fmt.Errorf("failed to write batch: %v", err)
		}
	}
	return nil
}
