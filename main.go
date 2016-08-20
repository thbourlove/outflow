package main

import "log"

func main() {
	server, err := NewHttpServer()
	if err != nil {
		log.Fatal("new http server: %v", err)
	}
	log.Fatal(server.Start())
}
