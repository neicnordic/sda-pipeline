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

func (suite *TestSuite) TestValidateJSON_Accession() {
	msg := finalize{
		Type:        "accession",
		User:        "foo",
		Filepath:    "dummy_data.c4gh",
		AccessionID: "EGAF12345678901",
		DecryptedChecksums: []checksums{
			{"sha256", "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}
	message, _ := json.Marshal(&msg)

	res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
}

func (suite *TestSuite) TestValidateJSON_Completed() {
	msg := completed{
		User:        "foo",
		Filepath:    "dummy_data.c4gh",
		AccessionID: "EGAF12345678901",
		DecryptedChecksums: []checksums{
			{"sha256", "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{"md5", "e3c19316a6f1cf8ef82a079a902e98d0"},
		},
	}
	message, _ := json.Marshal(&msg)

	res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
}
