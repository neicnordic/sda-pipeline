package main

import (
	"encoding/json"
	"fmt"
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

type accession struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

type Checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type ingest struct {
	Type     string `json:"type"`
	User     string `json:"user"`
	FilePath string `json:"filepath"`
}

type mapping struct {
	Type        string   `json:"type"`
	DatasetID   string   `json:"dataset_id"`
	AcessionIDs []string `json:"accession_ids"`
}

type missing struct {
	User     string `json:"user"`
	FilePath string `json:"filepath"`
}

func (suite *TestSuite) TestValidateJSON_Accession() {
	msg := accession{
		Type:        "accession",
		User:        "foo",
		FilePath:    "/tmp/foo",
		AccessionID: "EGAF12345678901",
		DecryptedChecksums: []Checksums{
			{"md5", "7Ac236b1a8dce2dac89e7cf45d2b48BD"},
		},
	}
	message, _ := json.Marshal(&msg)

	msgType, res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
	assert.Equal(suite.T(), "accession", msgType)

}

func (suite *TestSuite) TestValidateJSON_Cancel() {
	msg := ingest{
		Type:     "cancel",
		User:     "foo",
		FilePath: "/tmp/foo",
	}
	message, _ := json.Marshal(&msg)

	msgType, res, err := validateJSON("file://../../schemas/federated/", message)
	if fmt.Sprintf("%v", msgType) == "" {
		fmt.Println("empty")
	}
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
	assert.Empty(suite.T(), msgType)

}

func (suite *TestSuite) TestValidateJSON_Ingest() {
	msg := ingest{
		Type:     "ingest",
		User:     "foo",
		FilePath: "/tmp/foo",
	}
	message, _ := json.Marshal(&msg)

	msgType, res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
	assert.Equal(suite.T(), "ingest", msgType)

}

func (suite *TestSuite) TestValidateJSON_Mapping() {
	msg := mapping{
		Type:      "mapping",
		DatasetID: "EGAD12345678900",
		AcessionIDs: []string{
			"EGAF12345678901",
		},
	}
	message, _ := json.Marshal(&msg)

	msgType, res, err := validateJSON("file://../../schemas/federated/", message)
	assert.Nil(suite.T(), err)
	assert.True(suite.T(), res.Valid())
	assert.Equal(suite.T(), "mapping", msgType)

}

func (suite *TestSuite) TestValidateJSON_Notype() {
	msg := missing{
		User:     "foo",
		FilePath: "/tmp/foo",
	}
	message, _ := json.Marshal(&msg)

	msgType, res, err := validateJSON("file://../../schemas/federated", message)
	assert.NotNil(suite.T(), err)
	assert.Nil(suite.T(), res)
	assert.Empty(suite.T(), msgType)

}
