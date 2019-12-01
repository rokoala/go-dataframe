package metrics

import (
	"net/http"
	"strings"
	"sync"
)

type Metrics struct {
	totalCalls     int // total requests - any kind of request
	totalGetCalls  int // total get requests
	totalPostCalls int // total post requests
}

var metrics *Metrics

var mutex = &sync.Mutex{}

func NewMetrics(totalCalls, totalGetCalls, totalPostCalls int) *Metrics {
	metrics = &Metrics{totalCalls, totalGetCalls, totalPostCalls}
	return metrics
}

func GetMetrics() *Metrics {
	return metrics
}

func GetTotalCalls() int {
	return metrics.totalCalls
}

func GetTotalGetCalls() int {
	return metrics.totalGetCalls
}

func GetTotalPostCalls() int {
	return metrics.totalPostCalls
}

func (metrics *Metrics) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// Do not add as count if metric api is called
		if !strings.Contains(r.URL.String(), "/metrics/") {
			switch r.Method {
			case "GET":
				mutex.Lock()
				metrics.totalGetCalls++
				mutex.Unlock()
			case "POST":
				mutex.Lock()
				metrics.totalPostCalls++
				mutex.Unlock()
			}
			mutex.Lock()
			metrics.totalCalls++
			mutex.Unlock()
		}
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)
	})
}
