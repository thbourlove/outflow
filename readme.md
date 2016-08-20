# Outflow

Proxy for influxdb

## Example

`SELECT count(value) FROM "cpu.clock" WHERE time > 1462860100000000000 AND time < 1462860110000000000 GROUP BY id, time(5s)`

`curl 'http://localhost:10086/query?db=esm&epoch=ns&q=SELECT+count%28value%29+FROM+%22cpu.clock%22+WHERE+time+%3E+1462860100000000000+AND+time+%3C+1462860110000000000+group+by+id%2C+time%285s%29'`
