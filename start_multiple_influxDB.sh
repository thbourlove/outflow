

# This script will launch multiple influxDB instance which listen to different port number. In this way, it makes testing much easier.
CONFIG_DIR=config
if [ ! -d $CONFIG_DIR ]; then
    echo $CONFIG_DIR directory is not existend. Exiting!!!
    exit
fi

#TODO For now, adding new configuration is done by human. Future work should make it automatically read
#contents in config directory.
influxd -config $CONFIG_DIR/influx_1.conf&
influxd -config $CONFIG_DIR/influx_2.conf&
