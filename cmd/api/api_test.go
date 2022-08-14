package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	"github.com/DATA-DOG/go-sqlmock"
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

	Conf = &config.Config{}
	u, err := url.Parse(ts.URL)
	assert.NoError(t, err)
	Conf.Broker.Host = u.Hostname()
	assert.Equal(t, "127.0.0.1", Conf.Broker.Host)
	Conf.Broker.Port, _ = strconv.Atoi(u.Port())

	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	assert.NotNil(t, db)
	Conf.API.DB = &database.SQLdb{DB: db, ConnInfo: ""}

	res, err := http.Get(ts.URL + "/ready")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	defer res.Body.Close()
}

func TestTCPDialCheck(t *testing.T) {
	assert.Error(t, checkMQ("localhost:12345", 1*time.Second))

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello")
	}))

	u, err := url.Parse(ts.URL)
	assert.NoError(t, err)
	assert.NoError(t, checkMQ(u.Host, 1*time.Second))
}

func TestDatabasePingCheck(t *testing.T) {
	database := database.SQLdb{}
	assert.Error(t, checkDB(&database, 1*time.Second), "nil DB should fail")

	database.DB, _, err = sqlmock.New()
	assert.NoError(t, err)
	assert.NoError(t, checkDB(&database, 1*time.Second), "ping should succeed")
}
