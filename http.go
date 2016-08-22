package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type HttpServer struct {
}

func NewHttpServer() (*HttpServer, error) {
	return &HttpServer{}, nil
}

func (s *HttpServer) Start() error {
	router := httprouter.New()
	router.POST("/query", queryHandler)
	router.GET("/query", queryHandler)
	addr := "0.0.0.0:10086"
	return http.ListenAndServe(addr, router)
}

func (s *HttpServer) Stop() error {
	return nil
}

type Error struct {
	code int
	Msg  string
}

var (
	ErrBadRequest     = &Error{http.StatusBadRequest, "Bad Request"}
	ErrNotFound       = &Error{http.StatusNotFound, "Not Found"}
	ErrInternalServer = &Error{http.StatusInternalServerError, "Internal Server Error"}
)

func responseError(w http.ResponseWriter, e Error) {
	responseJson(w, e.code, e)
}

func responseJson(w http.ResponseWriter, code int, v interface{}) {
	bytes, err := json.Marshal(v)
	if err != nil {
		log.Printf("marshal response %v: %v", v, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))
	w.WriteHeader(code)
	if v != nil {
		if _, err := w.Write(bytes); err != nil {
			log.Printf("write response %v: %v", bytes, err)
			return
		}
	}
	return
}
