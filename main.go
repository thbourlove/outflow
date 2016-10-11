package main

import (
	"flag"
	"fmt"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"

	"github.com/thbourlove/outflow/client"
	"github.com/thbourlove/outflow/httpd"
)

var (
	configPath string
	logfile    string
)

func init() {
	flag.StringVar(&configPath, "config", "outflow.toml", "outflow config file path")
	flag.StringVar(&logfile, "logfile", "", "log file")
	flag.Parse()
}

func initLog() {
	if logfile != "" {
		rotateOutput := &lumberjack.Logger{
			Filename:   logfile,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		}
		log.SetOutput(rotateOutput)
	}
}

func main() {
	initLog()
	config, err := ParseConfig(configPath)
	if err != nil {
		log.Fatalf("pares config: %v", err)
	}

	c, err := client.New(config.Upstreams)
	if err != nil {
		log.Fatalf("new upstreams: %v", err)
	}

	server, err := httpd.NewHttpServer(c, config.Httpd)
	if err != nil {
		log.Fatalf("new http server: %v", err)
	}

	fmt.Println("start web server", config.Httpd)
	log.Fatal(server.Start())
}
