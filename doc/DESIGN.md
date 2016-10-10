# What is problem we want to solve 

## background
influxDB is great time series database but its limitation is its data only store in one node.
Storage in one node often has performance issue. This project want to add a extra plugin which 
allows influxDB can write/query data into a distributed framwork. As a user, all commands you can 
execute in influxDB will be support in this project. On the other hand, this plugin will turn 
current single influxDB into a cluster one. 

## Issues
### Choice of Distributed Algorithm
Solving a distributed system problem often realtes with `sync`, `replication`, `crashed recovery` 
and `consistency` issue. For trackling down all these issues, we need make some trade-offs given
our current bussiness's need. After some research, we want to implement this plugin via [Raft](https://raft.github.io/).
#### Raft Distributed Consensus Algorithm
please check raft.md in `doc` directory. In addition, this [visualization](http://thesecretlivesofdata.com/raft/) can help you understand how raft work in general.

## Some problems specifal for influxDB
1. influxDB support sql-like query language. By default, influxDB support function query such as `min`, `max` and so on. What we want is that this plugin can support all these different kind of query not just only support `SELECT`.
2. If it is possible, we also want enable our plugin with the `cli` query features just like native `cli` in influxDB.
