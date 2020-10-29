package main

import (
	"encoding/json"
	"testing"

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
}

func (suite *TestSuite) TestValidateJSON_Mapping() {
	msg := message{
		Type:        "mapping",
		DatasetID:   "EGAD12345678901",
		AccessionIDs: []string{"EGAF12345678901", "EGAF12345678902",},
	}
	message, _ := json.Marshal(&msg)

	res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
}
