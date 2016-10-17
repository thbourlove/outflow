# This script will kill all alive influxDB
for KILLPID in `ps ax | grep "influx" | awk ' {print $1;}'`; do
    echo find a alive influxDB instance whose PID is $KILLPID
    kill -9 $KILLPID
done
