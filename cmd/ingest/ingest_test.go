package main

import (
	"io"
	"os"
	"testing"

	"sda-pipeline/internal/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TestSuite struct {
	suite.Suite
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func (suite *TestSuite) SetupTest() {
	viper.Set("log.level", "debug")
	viper.Set("archive.location", "../../dev_utils")
	viper.Set("c4gh.filepath", "../../dev_utils/c4gh.sec.pem")
	viper.Set("c4gh.passphrase", "oaagCP1YgAZeEyl2eJAkHv9lkcWXWFgm")

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

func (suite *TestSuite) TestTryDecrypt_keyError() {

	viper.Set("c4gh.filepath", "/tmp/foo")
	config, err := config.New("verify")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)

	buf := make([]byte, 1024)

	byte, err := tryDecrypt(config.Crypt4gh, buf)
	assert.Nil(suite.T(), byte)
	assert.Error(suite.T(), err)
}

func (suite *TestSuite) TestTryDecrypt_passError() {

	viper.Set("c4gh.passphrase", "asdf")
	config, err := config.New("verify")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)

	buf := make([]byte, 1024)

	byte, err := tryDecrypt(config.Crypt4gh, buf)
	assert.Nil(suite.T(), byte)
	assert.Error(suite.T(), err)
}

func (suite *TestSuite) TestTryDecrypt_wrongFile() {
	config, err := config.New("verify")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)

	buf := make([]byte, 1024)
	file, err := os.Open("../../dev_utils/README.md")
	assert.NoError(suite.T(), err)

	_, err = io.ReadFull(file, buf)
	assert.NoError(suite.T(), err)

	b, err := tryDecrypt(config.Crypt4gh, buf)
	assert.Nil(suite.T(), b)
	assert.Error(suite.T(), err)
}

func (suite *TestSuite) TestTryDecrypt() {
	config, err := config.New("verify")
	assert.NotNil(suite.T(), config)
	assert.NoError(suite.T(), err)

	buf := make([]byte, 65*1024)

	file, err := os.Open("../../dev_utils/dummy_data.c4gh")
	assert.NoError(suite.T(), err)

	_, err = io.ReadFull(file, buf)
	assert.NoError(suite.T(), err)

	b, err := tryDecrypt(config.Crypt4gh, buf)
	assert.NotNil(suite.T(), b)
	assert.NoError(suite.T(), err)
}
