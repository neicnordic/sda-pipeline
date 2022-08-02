package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TestSuite struct {
	suite.Suite
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}

func TestValidateJSONInfoError(t *testing.T) {
	org := IngestionAccession{
		Type:        "accession",
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "EGAF00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{Type: "md5", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
	}

	orgMsg, _ := json.Marshal(org)

	infoError := InfoError{
		Error:           "Failed to open file to ingest",
		Reason:          "This is an error",
		OriginalMessage: &orgMsg,
	}
	msg, _ := json.Marshal(infoError)
	res, err := ValidateJSON("file://../../schemas/federated/info-error.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionAccession{
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "ABCD00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/info-error.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())

}

func TestValidateJSONIngestionAccession(t *testing.T) {
	okMsg := IngestionAccession{
		Type:        "accession",
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "EGAF00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{Type: "md5", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/ingestion-accession.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionAccession{
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "ABCD00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/ingestion-accession.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())

}

func TestValidateJSONIngestionAccessionRequest(t *testing.T) {
	okMsg := IngestionAccessionRequest{
		User:     "JohnDoe",
		FilePath: "path/to file",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{Type: "md5", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/ingestion-accession-request.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionAccessionRequest{
		User:     "JohnDoe",
		FilePath: "EGAF00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "", Value: ""},
		},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/ingestion-accession-request.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())

}

func TestValidateJSONIngestionCompletion(t *testing.T) {
	okMsg := IngestionCompletion{
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "EGAF00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{Type: "md5", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/ingestion-completion.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionCompletion{
		User:        "JohnDoe",
		FilePath:    "path/to file",
		AccessionID: "EGAF00123456789",
		DecryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/ingestion-completion.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())

}

func TestValidateJSONIngestionTrigger(t *testing.T) {
	okMsg := IngestionTrigger{
		Type:     "ingest",
		User:     "JohnDoe",
		FilePath: "path/to/file",
		EncryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/ingestion-trigger.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionTrigger{
		User:     "JohnDoe",
		FilePath: "path/to file",
		EncryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
		},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/ingestion-trigger.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())

}

func TestValidateJSONIngestionVerification(t *testing.T) {
	okMsg := IngestionVerification{
		User:        "JohnDoe",
		FilePath:    "path/to/file",
		FileID:      123456789,
		ArchivePath: "filename",
		EncryptedChecksums: []Checksums{
			{Type: "sha256", Value: "da886a89637d125ef9f15f6d676357f3a9e5e10306929f0bad246375af89c2e2"},
			{Type: "md5", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
		ReVerify: false,
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/ingestion-verification.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := IngestionVerification{
		User:        "JohnDoe",
		FilePath:    "path/to/file",
		FileID:      123456789,
		ArchivePath: "filename",
		EncryptedChecksums: []Checksums{
			{Type: "sha256", Value: "68b329da9893e34099c7d8ad5cb9c940"},
		},
		ReVerify: false,
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/ingestion-verification.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())
}

func TestValidateJSONDatasetMapping(t *testing.T) {
	okMsg := DatasetMapping{
		Type:         "mapping",
		DatasetID:    "EGAD12345678901",
		AccessionIDs: []string{"EGAF12345678901", "EGAF12345678902", "EGAF12345678903"},
	}

	msg, _ := json.Marshal(okMsg)
	res, err := ValidateJSON("file://../../schemas/federated/dataset-mapping.json", msg)
	assert.Nil(t, err)
	assert.True(t, res.Valid())

	badMsg := DatasetMapping{
		Type:         "mapping",
		DatasetID:    "BAD12345678901",
		AccessionIDs: []string{"EGAF12345678901", "EGAF12345678902", "EGAF12345678903"},
	}

	msg, _ = json.Marshal(badMsg)
	res, err = ValidateJSON("file://../../schemas/federated/dataset-mapping.json", msg)
	assert.Nil(t, err)
	assert.False(t, res.Valid())
}

func TestValidateJSONWrongSchema(t *testing.T) {
	okMsg := DatasetMapping{
		Type:         "mapping",
		DatasetID:    "EGAD12345678901",
		AccessionIDs: []string{"EGAF12345678901", "EGAF12345678902", "EGAF12345678903"},
	}

	msg, _ := json.Marshal(okMsg)
	_, err := ValidateJSON("file://../../schemas/federated/inbox-remove.json", msg)
	assert.Equal(t, "Unknown reference schema", err.Error())
}

func TestValidateJSONMissingSchema(t *testing.T) {
	okMsg := DatasetMapping{
		Type:         "mapping",
		DatasetID:    "EGAD12345678901",
		AccessionIDs: []string{"EGAF12345678901", "EGAF12345678902", "EGAF12345678903"},
	}

	msg, _ := json.Marshal(okMsg)
	_, err := ValidateJSON("file://../../schemas/federated/inbox.json", msg)
	assert.Equal(t, "open ../../schemas/federated/inbox.json: no such file or directory", err.Error())
}
