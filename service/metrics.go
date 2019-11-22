package service

import (
	"encoding/json"
	"log"
	"net/http"

	metrics "github.com/go-dataframe/utils/metrics"
)

func returnJSON(w http.ResponseWriter, count int) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if err := json.NewEncoder(w).Encode(count); err != nil {
		log.Println("Could not encode count:", count)
		panic(err)
	}
}

func TotalCalls(w http.ResponseWriter, r *http.Request) {
	returnJSON(w, metrics.GetTotalCalls())
}

func TotalGetCalls(w http.ResponseWriter, r *http.Request) {
	returnJSON(w, metrics.GetTotalGetCalls())
}

func TotalPostCalls(w http.ResponseWriter, r *http.Request) {
	returnJSON(w, metrics.GetTotalPostCalls())
}
