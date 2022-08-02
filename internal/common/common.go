package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/xeipuuv/gojsonschema"
)

func ValidateJSON(reference string, body []byte) (*gojsonschema.Result, error) {

	schema := gojsonschema.NewReferenceLoader(reference)
	res, err := gojsonschema.Validate(schema, gojsonschema.NewBytesLoader(body))
	if err != nil {
		return nil, err
	}

	dest := getStructName(reference)
	if dest == "" {
		return nil, fmt.Errorf("Unknown reference schema")
	}

	d := json.NewDecoder(bytes.NewBuffer(body))
	d.DisallowUnknownFields()
	de := d.Decode(dest)
	if err != nil {
		return nil, de
	}

	return res, err
}

func getStructName(path string) interface{} {
	switch strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)) {
	case "dataset-mapping":
		return new(DatasetMapping)
	case "info-error":
		return new(InfoError)
	case "ingestion-accession":
		return new(IngestionAccession)
	case "ingestion-accession-request":
		return new(IngestionAccessionRequest)
	case "ingestion-completion":
		return new(IngestionCompletion)
	case "ingestion-trigger":
		return new(IngestionTrigger)
	case "ingestion-verification":
		return new(IngestionVerification)
	default:
		return ""
	}
}

type Checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type InfoError struct {
	Error           string      `json:"error"`
	Reason          string      `json:"reason"`
	OriginalMessage interface{} `json:"original-message"`
}

type DatasetMapping struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

type IngestionAccession struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

type IngestionAccessionRequest struct {
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

type IngestionCompletion struct {
	User               string      `json:"user,omitempty"`
	FilePath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

type IngestionTrigger struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	EncryptedChecksums []Checksums `json:"encrypted_checksums"`
}

type IngestionVerification struct {
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	FileID             int64       `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []Checksums `json:"encrypted_checksums"`
	ReVerify           bool        `json:"re_verify"`
}
