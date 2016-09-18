package httpd

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/influxdata/influxdb/influxql"
	"github.com/julienschmidt/httprouter"
	"github.com/thbourlove/outflow/client"
)

type HttpServer struct {
	config Config
	router *httprouter.Router
}

func NewHttpServer(c *client.Client, config Config) (*HttpServer, error) {
	handler := NewHandler(c)

	router := httprouter.New()
	router.POST("/query", handler.query)
	router.GET("/query", handler.query)
	router.POST("/ping", handler.ping)
	router.GET("/ping", handler.ping)
	router.POST("/write", handler.write)
	router.GET("/write", handler.write)

	return &HttpServer{router: router, config: config}, nil
}

func (s *HttpServer) Start() error {
	return http.ListenAndServe(s.config.Addr, s.router)
}

func (s *HttpServer) Stop() error {
	return nil
}

type Error struct {
	code int
	Err  string
}

func (r Error) MarshalJSON() ([]byte, error) {
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
	}

	o.Results = append(o.Results, &influxql.Result{})
	if r.Err != "" {
		o.Err = r.Err
	}

	return json.Marshal(&o)
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
