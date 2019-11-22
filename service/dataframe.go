package service

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/go-dataframe/domain"
	"github.com/gorilla/mux"
)

type JSONError struct {
	Error    error `json:"-"`
	ErrorMsg string
}

func writerEncoder(w http.ResponseWriter, data interface{}) {
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Fatal(err)
	}
}

func verifyErrorEncode(w http.ResponseWriter, err error, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writerEncoder(w, JSONError{Error: err, ErrorMsg: err.Error()})
	} else {
		w.WriteHeader(http.StatusOK)
		writerEncoder(w, data)
	}
}

func bodyReader(w http.ResponseWriter, r *http.Request) []byte {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Fatal(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if err := r.Body.Close(); err != nil {
		log.Fatal(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	return body
}

func CreateDataFrame(w http.ResponseWriter, r *http.Request) {
	body := bodyReader(w, r)

	var dfHeader domain.DataFrameHeader
	if err := json.Unmarshal(body, &dfHeader); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	domain.GetCreateDataFrame(dfHeader.Pivots, dfHeader.Vals)

	w.WriteHeader(http.StatusCreated)
}

func AddRecord(w http.ResponseWriter, r *http.Request) {
	body := bodyReader(w, r)

	var dfHeader domain.DataFrameRows
	if err := json.Unmarshal(body, &dfHeader); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	if err := domain.AddRecord(dfHeader.Pivots, dfHeader.Vals); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(JSONError{Error: err, ErrorMsg: err.Error()}); err != nil {
			log.Fatal(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	w.WriteHeader(http.StatusCreated)
}

func GetAllRows(w http.ResponseWriter, r *http.Request) {
	rows, err := domain.GetAllRows()
	verifyErrorEncode(w, err, rows)
}

func GetRow(w http.ResponseWriter, r *http.Request) {
	// Get param from url
	params := mux.Vars(r)
	// Convert the value to Int
	rowIdx, _ := strconv.Atoi(params["row"])

	row, err := domain.GetRow(rowIdx)
	verifyErrorEncode(w, err, row)
}

func CleanDataFrame(w http.ResponseWriter, r *http.Request) {
	domain.CleanDataframe()
	w.WriteHeader(http.StatusOK)
}

func writerAgg(w http.ResponseWriter, body []byte, fn func(agg domain.Agg) (domain.AggResult, error)) {
	var agg domain.Agg

	if err := json.Unmarshal(body, &agg); err != nil {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(422)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	rows, err := fn(agg)
	verifyErrorEncode(w, err, rows)
}

func GetAggSum(w http.ResponseWriter, r *http.Request) {
	body := bodyReader(w, r)
	writerAgg(w, body, domain.GetAggSum)
}

func GetAggCount(w http.ResponseWriter, r *http.Request) {
	body := bodyReader(w, r)
	writerAgg(w, body, domain.GetAggCount)
}
