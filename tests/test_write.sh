#!/usr/bin/env bash
curl -i -XPOST 'http://localhost:10086/write?db=foo' --data-binary 'cpu_load_short,host=server01,region=ch-west value=0.64'
curl -i -XPOST 'http://localhost:10086/write?db=foo' --data-binary 'cpu_load_short,host=server02,region=ch-west value=0.74'
curl -i -XPOST 'http://localhost:10086/write?db=foo' --data-binary 'cpu_load_short,host=server02,region=ch-west value=0.84'

curl -i -XPOST 'http://localhost:10086/write?db=bar' --data-binary 'cpu_load_short,host=server01,region=ch-west value=0.94'
curl -i -XPOST 'http://localhost:10086/write?db=bar' --data-binary 'cpu_load_short,host=server02,region=ch-west value=1.04'
curl -i -XPOST 'http://localhost:10086/write?db=bar' --data-binary 'cpu_load_short,host=server02,region=ch-west value=2.04'

curl -i -XPOST 'http://localhost:10086/write?db=baz' --data-binary 'temperature,host=server01,region=ch-east value=0.94'
curl -i -XPOST 'http://localhost:10086/write?db=baz' --data-binary 'temperature,host=server01,region=ch-east value=1.04'
curl -i -XPOST 'http://localhost:10086/write?db=baz' --data-binary 'temperature,host=server01,region=ch-east value=2.04'

