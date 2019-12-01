package api

import (
	"net/http"

	"github.com/go-dataframe/service"
)

// Defines a single route, e.g. a human readable name, HTTP method and the
// pattern the function that will execute when the route is called.
type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

// Defines the type Routes which is just an array (slice) of Route structs.
type Routes []Route

// Initialize our routes
var routes = Routes{
	Route{
		"CreateDataframe", // Name
		"POST",            // HTTP method
		"/frame",          // Route pattern
		service.CreateDataFrame,
	},
	Route{
		"AddRow",
		"POST",
		"/frame/row",
		service.AddRecord,
	},
	Route{
		"GetAllRows",
		"GET",
		"/frame/row/all",
		service.GetAllRows,
	},
	Route{
		"GetRow",
		"GET",
		"/frame/row/{row}",
		service.GetRow,
	},
	Route{
		"CleanDataframe",
		"PUT",
		"/frame/clean",
		service.CleanDataFrame,
	},
	Route{
		"GetAggSum",
		"GET",
		"/frame/agg/sum",
		service.GetAggSum,
	},
	Route{
		"GetAggCount",
		"GET",
		"/frame/agg/count",
		service.GetAggCount,
	},
	Route{
		"GetAgg",
		"GET",
		"/frame/agg",
		service.GetAgg,
	},
	Route{
		"GetMetricsTotal",
		"GET",
		"/metrics/total",
		service.TotalCalls,
	},
	Route{
		"GetMetricsTotal",
		"GET",
		"/metrics/total/get",
		service.TotalGetCalls,
	},
	Route{
		"GetMetricsTotal",
		"GET",
		"/metrics/total/post",
		service.TotalPostCalls,
	},
}
