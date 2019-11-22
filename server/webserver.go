package server

import (
	"log"
	"net/http"

	"github.com/go-dataframe/api"
)

func StartWebServer(port string) {
	log.Println("Starting HTTP service at " + port)

	router := api.NewRouter()

	err := http.ListenAndServe(":"+port, router)
	if err != nil {
		log.Println("An error occured starting HTTP listener at port " + port)
		log.Println("Error is: " + err.Error())
	}

}
