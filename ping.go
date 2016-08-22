package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func pingHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusNoContent)
}
