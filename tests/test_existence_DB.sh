curl -GET 'http://localhost:10086/query?pretty=true' --data-urlencode "db=foo" --data-urlencode "q=SELECT \"value\" FROM \"cpu_load_short\" WHERE \"region\"='ch-west'"
