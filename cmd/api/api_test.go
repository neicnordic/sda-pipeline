package main

import (
	"log"
	"testing"
	"time"

	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestDatabasePingCheck(t *testing.T) {
	database := database.SQLdb{}
	assert.Error(t, checkDB(&database, 1*time.Second), "nil DB should fail")

	database.DB, _, err = sqlmock.New()
	assert.NoError(t, err)
	assert.NoError(t, checkDB(&database, 1*time.Second), "ping should succeed")
}

func TestGetToken(t *testing.T) {
	authHeader := "Bearer sometoken"
	_, err := getToken(authHeader)
	assert.NoError(t, err)

	authHeader = "Bearer "
	_, err = getToken(authHeader)
	log.Print(err)
	assert.EqualError(t, err, "token string is missing from authorization header")

	authHeader = "Beare"
	_, err = getToken(authHeader)
	assert.EqualError(t, err, "authorization scheme must be bearer")

	authHeader = ""
	_, err = getToken(authHeader)
	assert.EqualError(t, err, "access token must be provided")
}
