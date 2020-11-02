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

func (suite *TestSuite) TestValidateJSON_Ingest() {
	msg := message{
		User:        "foo",
		FileID:      123,
		ArchivePath: "09612f01-c1d0-4d84-9ada-bdd3324e580e",
		EncryptedChecksums: []checksums{
			{"sha256", "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
		ReVerify: false,
	}
	message, _ := json.Marshal(&msg)

	res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
}

func (suite *TestSuite) TestValidateJSON_AccessionRequest() {
	msg := verified{
		User:     "foo",
		FilePath: "dummy_data.c4gh",
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
