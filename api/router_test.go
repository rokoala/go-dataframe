package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

type Test struct {
	requestType    string
	url            string
	body           string
	expectedStatus int
	expectedBody   string
}

func testRequestStatus(t *testing.T, router *mux.Router, requestType string, url string, body string, expectedStatus int) {
	assert := assert.New(t)
	res := httptest.NewRecorder()
	req, _ := http.NewRequest(requestType, url, bytes.NewBufferString(body))
	router.ServeHTTP(res, req)
	assert.Equal(expectedStatus, res.Code)
}

func testRequestStatusAndBody(t *testing.T, router *mux.Router, requestType string, url string, body string, expectedStatus int, expectedBody string) {
	assert := assert.New(t)
	res := httptest.NewRecorder()
	req, _ := http.NewRequest(requestType, url, bytes.NewBufferString(body))
	router.ServeHTTP(res, req)
	assert.Equal(expectedStatus, res.Code)
	assert.JSONEq(expectedBody, res.Body.String())
}

func TestRESTCreateDataframe(t *testing.T) {
	router := NewRouter()
	// Verifica a criação de um Dataframe
	testRequestStatus(t, router, "POST", "/frame", `{"pivots":["A","B","C"],"vals":["V1","V3"]}`, 201)
}

func TestRESTAddRecordDataframe(t *testing.T) {
	router := NewRouter()

	// Verifica a criação de um Dataframe
	testRequestStatus(t, router, "POST", "/frame", `{"pivots":["A","B","C"],"vals":["V1","V3"]}`, 201)

	// Adiciona um record
	dfRowJSON := `{"pivots":["a","d","c"],"vals":[2,2]}`
	testRequestStatus(t, router, "POST", "/frame/row", dfRowJSON, 201)

	tests := []Test{
		// Retorna todos o records
		{requestType: "GET", url: "/frame/row/all", body: "", expectedStatus: 200, expectedBody: "[" + dfRowJSON + "]"},
		// Retorna o primeiro record
		{requestType: "GET", url: "/frame/row/0", body: "", expectedStatus: 200, expectedBody: dfRowJSON},
		// Verifica erro ao adicionar record inválido
		{requestType: "POST", url: "/frame/row", body: `{"pivots":["a","d","c"],"vals":[2,2,2]}`, expectedStatus: 400, expectedBody: `{"ErrorMsg":"vals length 3 exceed the Datafram val current size 2"}`},
		// Verifica erro ao obter indice inválido
		{requestType: "GET", url: "/frame/row/20", body: "", expectedStatus: 400, expectedBody: `{"ErrorMsg":"Could not find index 20"}`},
	}

	// Realiza os testes
	for _, test := range tests {
		testRequestStatusAndBody(t, router, test.requestType, test.url, test.body, test.expectedStatus, test.expectedBody)
	}
}

func TestRESTCleanDataframe(t *testing.T) {
	router := NewRouter()

	// Verifica a criação de um Dataframe
	testRequestStatus(t, router, "POST", "/frame", `{"pivots":["A","B","C"],"vals":["V1","V3"]}`, 201)

	// Adiciona um record
	dfRowJSON := `{"pivots":["a","d","c"],"vals":[2,2]}`
	testRequestStatus(t, router, "POST", "/frame/row", dfRowJSON, 201)

	// Remove dataframe
	testRequestStatus(t, router, "PUT", "/frame/clean", "", 200)

	// Verifica que não existe mais dataframe e não é possível realizar as operações
	tests := []Test{
		{requestType: "POST", url: "/frame/row", body: dfRowJSON, expectedStatus: 400, expectedBody: `{"ErrorMsg":"No instance of dataframe created"}`},
		{requestType: "GET", url: "/frame/row/all", body: "", expectedStatus: 400, expectedBody: `{"ErrorMsg":"No instance of dataframe created"}`},
		{requestType: "GET", url: "/frame/row/1", body: "", expectedStatus: 400, expectedBody: `{"ErrorMsg":"No instance of dataframe created"}`},
		{requestType: "GET", url: "/frame/agg/sum", body: `{"pivots":["A","B"],"aggColumn":1}`, expectedStatus: 400, expectedBody: `{"ErrorMsg":"No instance of dataframe created"}`},
		{requestType: "GET", url: "/frame/agg/count", body: `{"pivots":["A","B"],"aggColumn":1}`, expectedStatus: 400, expectedBody: `{"ErrorMsg":"No instance of dataframe created"}`},
	}

	for _, test := range tests {
		testRequestStatusAndBody(t, router, test.requestType, test.url, test.body, test.expectedStatus, test.expectedBody)
	}
}

func TestRESTAggDataframe(t *testing.T) {
	router := NewRouter()

	// Verifica a criação de um Dataframe
	testRequestStatus(t, router, "POST", "/frame", `{"pivots":["A","B","C"],"vals":["V1","V3"]}`, 201)

	// Adiciona um record
	dfRowJSON := `{"pivots":["a","d","c"],"vals":[2,2]}`
	testRequestStatus(t, router, "POST", "/frame/row", dfRowJSON, 201)

	bodyJSON := `{
		"pivots":["A","B"],
		"aggColumn":1
	}`

	// Verifica agg sum
	testRequestStatusAndBody(t, router, "GET", "/frame/agg/sum", bodyJSON, 200, `[{"pivots":["a","d"],"value":2}]`)

	// Adiciona um novo record
	testRequestStatus(t, router, "POST", "/frame/row", dfRowJSON, 201)

	// Verifica agg count
	testRequestStatusAndBody(t, router, "GET", "/frame/agg/count", bodyJSON, 200, `[{"pivots":["a","d"],"value":2}]`)
}

func TestRESTMetrics(t *testing.T) {
	router := NewRouter()

	// Verifica a criação de um Dataframe
	testRequestStatus(t, router, "POST", "/frame", `{"pivots":["A","B","C"],"vals":["V1","V3"]}`, 201)

	// Chamadas para a api metrics não deverão ser contaiblizadas
	testRequestStatusAndBody(t, router, "GET", "/metrics/total", "", 200, "1")
	testRequestStatusAndBody(t, router, "GET", "/metrics/total/get", "", 200, "0")
	testRequestStatusAndBody(t, router, "GET", "/metrics/total/post", "", 200, "1")
	testRequestStatus(t, router, "GET", "/frame/row/0", "", 200)
	// Verifica total de GET apos nova chamada GET
	testRequestStatusAndBody(t, router, "GET", "/metrics/total/get", "", 200, "1")
	// Verifica novo total apos nova chamada GET
	testRequestStatusAndBody(t, router, "GET", "/metrics/total", "", 200, "2")

}
