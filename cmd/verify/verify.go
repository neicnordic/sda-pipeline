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

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/streaming"
	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	Filepath           string      `json:"filepath"`
	User               string      `json:"user"`
	FileID             int         `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []Checksums `json:"encrypted_checksums"`
	ReVerify           *bool       `json:"re_verify"`
}

// Verified is struct holding the full message data
type Verified struct {
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type Checksums struct {
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

	ingestVerification := gojsonschema.NewReferenceLoader(conf.SchemasPath + "ingestion-verification.json")

	forever := make(chan bool)

	log.Info("starting verify service")

	go func() {
		messages, err := broker.GetMessages(mq, conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("received a message: %s", delivered.Body)
			res, err := gojsonschema.Validate(ingestVerification, gojsonschema.NewBytesLoader(delivered.Body))
			if err != nil {
				log.Error(err)
				// publish MQ error
				continue
			}
			if !res.Valid() {
				log.Error(res.Errors())
				// publish MQ error
				continue
			}

			var message Message
			if err := json.Unmarshal(delivered.Body, &message); err != nil {
				log.Errorf("Unmarshaling json message failed, reason: %s", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := broker.SendMessage(mq, delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, delivered.Body); e != nil {
					log.Error("faild to publish message, reason: ", e)
				}
				// Restart on new message
				continue
			}

			header, err := db.GetHeader(message.FileID)
			if err != nil {
				log.Error(err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", err)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := broker.SendMessage(mq, delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, delivered.Body); e != nil {
					log.Error("faild to publish message, reason: ", e)
				}
				continue
			}

			f, err := backend.NewFileReader(message.ArchivePath)
			if err != nil {
				log.Errorf("Failed to open file: %s, reason: %v", message.ArchivePath, err)
				continue
			}

			hr := bytes.NewReader(header)
			mr := io.MultiReader(hr, f)

			c4ghr, err := streaming.NewCrypt4GHReader(mr, *key, nil)
			if err != nil {
				log.Error(err)
				continue
			}

			md5hash := md5.New() // #nosec
			sha256hash := sha256.New()

			stream := io.TeeReader(c4ghr, md5hash)

			if _, err := io.Copy(sha256hash, stream); err != nil {
				log.Error(err)
				continue
			}

			//nolint:nestif
			if message.ReVerify == nil || !*message.ReVerify {
				log.Debug("Mark completed")
				// Mark file as "COMPLETED"
				if e := db.MarkCompleted(fmt.Sprintf("%x", sha256hash.Sum(nil)), message.FileID); e != nil {
					// this should really be hadled by the DB retry mechanism
				} else {
					// Send message to verified
					c := Verified{
						User:     message.User,
						Filepath: message.ArchivePath,
						DecryptedChecksums: []Checksums{
							{"sha256", fmt.Sprintf("%x", sha256hash.Sum(nil))},
							{"md5", fmt.Sprintf("%x", md5hash.Sum(nil))},
						},
					}

					verifyMsg := gojsonschema.NewReferenceLoader(conf.SchemasPath + "ingestion-accession-request.json")
					res, err := gojsonschema.Validate(verifyMsg, gojsonschema.NewGoLoader(c))
					if err != nil {
						fmt.Println("error:", err)
						log.Error(err)
						// publish MQ error
						continue
					}
					if !res.Valid() {
						fmt.Println("result:", res.Errors())
						log.Error(res.Errors())
						// publish MQ error
						continue
					}

					verified, _ := json.Marshal(&c)

					if err := broker.SendMessage(mq, delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, verified); err != nil {
						// TODO fix resend mechanism
						log.Errorln("We need to fix this resend stuff ...")
					}
					if err := delivered.Ack(false); err != nil {
						log.Errorf("failed to ack message for reason: %v", err)
					}
				}
			}
		}
	}()

	<-forever
}
