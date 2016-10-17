#!/usr/bin/env bash
curl -i -XPOST 'http://localhost:8086/query' --data-urlencode "q=CREATE DATABASE foo"
curl -i -XPOST 'http://localhost:8086/query' --data-urlencode "q=CREATE DATABASE bar"

curl -i -XPOST 'http://192.168.67.19:8086/query?q=CREATE+DATABASE+foo&db=_internal'
curl -i -XPOST 'http://192.168.67.19:8086/query?q=CREATE+DATABASE+bar&db=_internal'
