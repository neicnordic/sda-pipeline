package config

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TestSuite struct {
	suite.Suite
}

func (suite *TestSuite) SetupTest() {
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
}

func (suite *TestSuite) TearDownTest() {
	viper.Reset()
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (suite *TestSuite) TestNonExistingApplication() {
	expectedError := errors.New("application 'test' doesn't exist")
	config, err := New("test")
	assert.Nil(suite.T(), config)
	if assert.Error(suite.T(), err) {
		assert.Equal(suite.T(), expectedError, err)
	}
}

func (suite *TestSuite) TestMissingRequiredConfVar() {
	for _, requiredConfVar := range requiredConfVars {
		requiredConfVarValue := viper.Get(requiredConfVar)
		viper.Set(requiredConfVar, nil)
		expectedError := fmt.Errorf("%s not set", requiredConfVar)
		config, err := New("test")
		assert.Nil(suite.T(), config)
		if assert.Error(suite.T(), err) {
			assert.Equal(suite.T(), expectedError, err)
		}
		viper.Set(requiredConfVar, requiredConfVarValue)
	}
}

func (suite *TestSuite) TestMapperConfiguration() {
	config, err := New("mapper")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config.Broker)
	assert.Equal(suite.T(), "test", config.Broker.Host)
	assert.Equal(suite.T(), 123, config.Broker.Port)
	assert.Equal(suite.T(), "test", config.Broker.User)
	assert.Equal(suite.T(), "test", config.Broker.Password)
	assert.Equal(suite.T(), "test", config.Broker.Queue)
	assert.Equal(suite.T(), "test", config.Broker.RoutingKey)
	assert.NotNil(suite.T(), config.Postgres)
	assert.Equal(suite.T(), "test", config.Postgres.Host)
	assert.Equal(suite.T(), 123, config.Postgres.Port)
	assert.Equal(suite.T(), "test", config.Postgres.User)
	assert.Equal(suite.T(), "test", config.Postgres.Password)
	assert.Equal(suite.T(), "test", config.Postgres.Database)
}

func (suite *TestSuite) TestFinalizeConfiguration() {
	config, err := New("finalize")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config.Broker)
	assert.Equal(suite.T(), "test", config.Broker.Host)
	assert.Equal(suite.T(), 123, config.Broker.Port)
	assert.Equal(suite.T(), "test", config.Broker.User)
	assert.Equal(suite.T(), "test", config.Broker.Password)
	assert.Equal(suite.T(), "test", config.Broker.Queue)
	assert.Equal(suite.T(), "test", config.Broker.RoutingKey)
	assert.NotNil(suite.T(), config.Postgres)
	assert.Equal(suite.T(), "test", config.Postgres.Host)
	assert.Equal(suite.T(), 123, config.Postgres.Port)
	assert.Equal(suite.T(), "test", config.Postgres.User)
	assert.Equal(suite.T(), "test", config.Postgres.Password)
	assert.Equal(suite.T(), "test", config.Postgres.Database)
}

func (suite *TestSuite) TestVerifyConfiguration() {
	viper.Set("archive.location", "test")
	viper.Set("c4gh.filepath", "test")
	viper.Set("c4gh.passphrase", "test")
	config, err := New("verify")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config.Broker)
	assert.Equal(suite.T(), "test", config.Broker.Host)
	assert.Equal(suite.T(), 123, config.Broker.Port)
	assert.Equal(suite.T(), "test", config.Broker.User)
	assert.Equal(suite.T(), "test", config.Broker.Password)
	assert.Equal(suite.T(), "test", config.Broker.Queue)
	assert.Equal(suite.T(), "test", config.Broker.RoutingKey)
	assert.NotNil(suite.T(), config.Postgres)
	assert.Equal(suite.T(), "test", config.Postgres.Host)
	assert.Equal(suite.T(), 123, config.Postgres.Port)
	assert.Equal(suite.T(), "test", config.Postgres.User)
	assert.Equal(suite.T(), "test", config.Postgres.Password)
	assert.Equal(suite.T(), "test", config.Postgres.Database)
	assert.NotNil(suite.T(), config.Archive)
	assert.NotNil(suite.T(), config.Archive.Posix)
	assert.Equal(suite.T(), "test", config.Archive.Posix.Location)
	assert.NotNil(suite.T(), config.Crypt4gh)
	assert.Equal(suite.T(), "test", config.Crypt4gh.KeyPath)
	assert.Equal(suite.T(), "test", config.Crypt4gh.Passphrase)
}

func (suite *TestSuite) TestIngestConfiguration() {
	viper.Set("inbox.location", "test")
	viper.Set("archive.location", "test")
	viper.Set("c4gh.filepath", "test")
	viper.Set("c4gh.passphrase", "test")
	config, err := New("ingest")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config.Broker)
	assert.Equal(suite.T(), "test", config.Broker.Host)
	assert.Equal(suite.T(), 123, config.Broker.Port)
	assert.Equal(suite.T(), "test", config.Broker.User)
	assert.Equal(suite.T(), "test", config.Broker.Password)
	assert.Equal(suite.T(), "test", config.Broker.Queue)
	assert.Equal(suite.T(), "test", config.Broker.RoutingKey)
	assert.NotNil(suite.T(), config.Postgres)
	assert.Equal(suite.T(), "test", config.Postgres.Host)
	assert.Equal(suite.T(), 123, config.Postgres.Port)
	assert.Equal(suite.T(), "test", config.Postgres.User)
	assert.Equal(suite.T(), "test", config.Postgres.Password)
	assert.Equal(suite.T(), "test", config.Postgres.Database)
	assert.NotNil(suite.T(), config.Inbox)
	assert.NotNil(suite.T(), config.Inbox.Posix)
	assert.Equal(suite.T(), "test", config.Inbox.Posix.Location)
	assert.NotNil(suite.T(), config.Archive)
	assert.NotNil(suite.T(), config.Archive.Posix)
	assert.Equal(suite.T(), "test", config.Archive.Posix.Location)
	assert.NotNil(suite.T(), config.Crypt4gh)
	assert.Equal(suite.T(), "test", config.Crypt4gh.KeyPath)
	assert.Equal(suite.T(), "test", config.Crypt4gh.Passphrase)
}

func (suite *TestSuite) TestDefaultLogLevel() {
	viper.Set("log.level", "test")
	config, err := New("test")
	assert.Nil(suite.T(), config)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), log.TraceLevel, log.GetLevel())
}
