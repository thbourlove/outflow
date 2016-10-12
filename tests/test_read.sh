#!/usr/bin/env bash
curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=foo" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='ch-west'"
curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=foo" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='ch-west'"

curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=baz" --data-urlencode "q=SELECT \"value\" FROM \"temperature\" WHERE \"region\"='ch-east'"
curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=baz" --data-urlencode "q=SELECT \"value\" FROM \"temperature\" WHERE \"region\"='ch-east'"

curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=baz" --data-urlencode "q=SELECT max(\"value\") FROM \"temperature\" WHERE \"region\"='ch-east'"


