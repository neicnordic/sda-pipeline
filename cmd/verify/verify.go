// The verify service reads and decrypts ingested files from the archive
// storage and sends accession requests.
package main

import (
	"bytes"
	"crypto/md5" // #nosec
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/streaming"
	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type message struct {
	FilePath           string      `json:"filepath"`
	User               string      `json:"user"`
	FileID             int         `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []checksums `json:"encrypted_checksums"`
	ReVerify           bool       `json:"re_verify"`
}

// Verified is struct holding the full message data
type verified struct {
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	conf, err := config.NewConfig("verify")
	if err != nil {
		log.Fatal(err)
	}
	mq, err := broker.NewMQ(conf.Broker)
	if err != nil {
		log.Fatal(err)
	}
	db, err := database.NewDB(conf.Database)
	if err != nil {
		log.Fatal(err)
	}

	backend, err := storage.NewBackend(conf.Archive)
	if err != nil {
		log.Fatal(err)
	}

	key, err := config.GetC4GHKey()
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	go func() {
		for {
			connError := mq.ConnectionWatcher()
			log.Error(connError)
			os.Exit(1)
		}
	}()

	forever := make(chan bool)

	log.Info("starting verify service")

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("received a message: %s", delivered.Body)
			res, err := validateJSON(conf.SchemasPath, delivered.Body)
			if err != nil {
				log.Errorf("josn error: %v", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			}
			if !res.Valid() {
				log.Errorf("result.error: %v", res.Errors())
				log.Error("Validation failed")
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			}

			var message message

			// we unmarshal the message in the validation step so this is safe to do
			_ = json.Unmarshal(delivered.Body, &message)


			header, err := db.GetHeader(message.FileID)
			if err != nil {
				log.Error(err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", err)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, delivered.Body); e != nil {
					log.Error("faild to publish message, reason: ", e)
				}
				continue
			}

			var file database.FileInfo

			file.Size, err = backend.GetFileSize(message.ArchivePath)

			if err != nil {
				log.Errorf("Failed to get file size for %s, reason: %v", message.ArchivePath, err)
				continue
			}

			archiveFileHash := sha256.New()

			f, err := backend.NewFileReader(message.ArchivePath)
			if err != nil {
				log.Errorf("Failed to open file: %s, reason: %v", message.ArchivePath, err)
				// Send the errorus message to an error queue so it can be analyzed.
				fileError := broker.FileError{
					User:     message.User,
					FilePath: message.FilePath,
					Reason:   err.Error(),
				}
				body, _ := json.Marshal(fileError)
				if e := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, body); e != nil {
					log.Error("faild to publish message, reason: ", e)
				}
				// Restart on new message
				continue
			}

			hr := bytes.NewReader(header)
			// Feed everything read from the archive file to archiveFileHash
			mr := io.MultiReader(hr, io.TeeReader(f, archiveFileHash))

			c4ghr, err := streaming.NewCrypt4GHReader(mr, *key, nil)
			if err != nil {
				log.Error(err)
				continue
			}

			md5hash := md5.New() // #nosec
			sha256hash := sha256.New()

			stream := io.TeeReader(c4ghr, md5hash)

			if file.DecryptedSize, err = io.Copy(sha256hash, stream); err != nil {
				log.Error(err)
				continue
			}

			file.Checksum = archiveFileHash
			file.DecryptedChecksum = sha256hash

			//nolint:nestif
			if !message.ReVerify {

				c := verified{
					User:     message.User,
					FilePath: message.FilePath,
					DecryptedChecksums: []checksums{
						{"sha256", fmt.Sprintf("%x", sha256hash.Sum(nil))},
						{"md5", fmt.Sprintf("%x", md5hash.Sum(nil))},
					},
				}

				verified, _ := json.Marshal(&c)
				res, err = validateJSON(conf.SchemasPath, verified)

				if err != nil {
				log.Errorf("josn error: %v", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			}
			if !res.Valid() {
				log.Errorf("result.error: %v", res.Errors())
				log.Error("Validation failed")
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			}

				// Mark file as "COMPLETED"
				if e := db.MarkCompleted(file, message.FileID); e != nil {
					log.Errorf("MarkCompleted failed: %v", e)
					continue
					// this should really be hadled by the DB retry mechanism
				}

				log.Debug("Mark completed")
				// Send message to verified queue

				if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, verified); err != nil {
					// TODO fix resend mechanism
					log.Errorln("We need to fix this resend stuff ...")
				}
				if err := delivered.Ack(false); err != nil {
					log.Errorf("failed to ack message for reason: %v", err)
				}

			}
		}
	}()

	<-forever
}

// Validate the JSON in a received message
func validateJSON(schemasPath string, body []byte) (*gojsonschema.Result, error) {
	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return nil, err
	}

	var schema gojsonschema.JSONLoader

	_, ok := message["file_id"]
	if ok {
		schema = gojsonschema.NewReferenceLoader(schemasPath + "ingestion-verification.json")
	} else {
		schema = gojsonschema.NewReferenceLoader(schemasPath + "ingestion-accession-request.json")
	}
	res, err := gojsonschema.Validate(schema, gojsonschema.NewBytesLoader(body))
	return res, err
}
