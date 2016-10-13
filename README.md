# Outflow
Proxy for influxdb

## How to install and build
`go install` will instsll all necessary dependency. In addition, a executable binary file whose name is same 
as the project name will be created at `$GOPATH/bin`

## About
It is a simple proxy for influxDB. All it does is just forward incoming request to correct destination. Its successfully
running need assume all node are alive. Any of it can not be crashed. This is pretty strict but it works for now.

## How to create multiple influxDB instance
When you check the source code, you will find a bash scrip file called `start_mutliple_influxDB.sh`. You can execute it to create multiple
influxDB instance. By default, you will be able to create two influxDB server. You can add more for development purpose. Adding a new influxDB will
requires you add configuration file manually. All configuration files resides at `config` directory. For now, adding a new configuration file will 
increase its ID number related with naming system. It is easy to figure out when you actually look at the configuration files. 

## How to write data into influxDB
`sh test_write.sh`

When you execute above command, such database may not be present in influxDB. In such case, you need execute
create database first.

`sh test_createDB.sh`

Actually, creating any new database via this proxy is broken. This is part of our TODO tasks. We want to make this proxy 
compatible with original influxDB's query language.

## How to read data from influxDB
`sh test_read.sh`

## Protocol for distributing incoming request to correct destination
A simple fact is that influxDB requires each query providing with `database` and `measurement`. Based on this, we can
take the concatenation of database and measurement as key.

## TODO list
For now, this program only support very basic query and write request. Ideally, it 
should be able to allow user enable replication in case of any node crashed. 
Additionally, the software should have some kind of disaster recovery processes 
to prevent any delegation of overall performance.

1. Complete Query support
2. Repliaton 
3. Disaster recovery process
4. test code
