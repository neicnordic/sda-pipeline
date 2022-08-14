package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"sda-pipeline/internal/config"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TestSuite struct {
	suite.Suite
}

func TestApiTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func TestSetup(t *testing.T) {
	viper.Set("log.level", "debug")

	viper.Set("broker.host", "test")
	viper.Set("broker.port", 123)
	viper.Set("broker.user", "test")
	viper.Set("broker.password", "test")
	viper.Set("broker.queue", "test")
	viper.Set("broker.routingkey", "test")

	viper.Set("db.host", "test")
	viper.Set("db.port", 123)
	viper.Set("db.user", "test")
	viper.Set("db.password", "test")
	viper.Set("db.database", "test")

	conf := config.Config{}
	conf.API.Host = "localhost"
	conf.API.Port = 8080
	server := setup(&conf)

	assert.Equal(t, "localhost:8080", server.Addr)

}

func TestReadinessResponse(t *testing.T) {
	r := mux.NewRouter()
	r.HandleFunc("/ready", readinessResponse)
	ts := httptest.NewServer(r)
	defer ts.Close()

	res, err := http.Get(ts.URL + "/ready")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	defer res.Body.Close()
}
