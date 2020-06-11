package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/postgres"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/keys"
	"github.com/elixir-oslo/crypt4gh/streaming"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	Filepath     string `json:"filepath"`
	User         string `json:"user"`
	FileID       int    `json:"file_id"`
	ArchivePath  string `json:"archive_path"`
	FileChecksum string `json:"file_checksum"`
	ReVerify     *bool  `json:"re_verify"`
}

// Completed is struct holding the full message data
type Completed struct {
	User               string      `json:"user"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checkksum type and value
type Checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	config := NewConfig()
	mq := broker.New(config.Broker)
	db, err := postgres.NewDB(config.Postgres)
	if err != nil {
		log.Println("err:", err)
	}

	// Storage logici for S3 or Posix
	var backend storage.Backend
	if config.ArchiveType == "posix" {
		backend = storage.NewPosixBackend(config.PosixArchive)
	} else {
		backend = storage.NewS3Backend(config.S3Archive, TransportConfigS3(config))
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting verify service")

	go func() {
		for delivered := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", delivered.Body)
			var message Message
			// TODO verify json structure
			err := json.Unmarshal(delivered.Body, &message)
			if err != nil {
				log.Errorf("Not a json message: %s", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				if e := delivered.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the errorus message to an error queue so it can be analyzed.
				if e := broker.SendMessage(mq, delivered.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, delivered.Body); e != nil {
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
				if e := broker.SendMessage(mq, delivered.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, delivered.Body); e != nil {
					log.Error("faild to publish message, reason: ", e)
				}
				continue
			}

			// do file verfication
			keyFile, err := os.Open(config.Crypt4gh.KeyPath)
			if err != nil {
				log.Error(err)
			}
			keyFile.Close()

			key, err := keys.ReadPrivateKey(keyFile, []byte(config.Crypt4gh.Passphrase))
			if err != nil {
				log.Error(err)
			}

			f, err := backend.ReadFile(message.ArchivePath)
			if err != nil {
				log.Errorf("Failed to open file: %s, reason: %v", message.ArchivePath, err)
				continue
			}

			var buf bytes.Buffer
			buf.Write(header)
			rw := io.ReadWriter(&buf)
			if _, e := io.Copy(rw, f); e != nil {
				log.Error(e)
			}

			c4ghr, err := streaming.NewCrypt4GHReader(rw, key, nil)
			if err != nil {
				log.Error(err)
			}
			hash := sha256.New()
			if _, err := io.Copy(hash, c4ghr); err != nil {
				log.Error(err)
			}

			if !*message.ReVerify {
			if message.ReVerify == nil {
				// Mark file as "COMPLETED"
				if e := db.MarkCompleted(fmt.Sprintf("%x", hash.Sum(nil)), message.FileID); e != nil {
					// this should really be hadled by the DB retry mechanism
				} else {
					// Send message to completed
					c := Completed{
						User: message.User,
						DecryptedChecksums: []Checksums{
							{"SHA256", fmt.Sprintf("%x", hash.Sum(nil))},
						},
					}
					completed, err := json.Marshal(&c)
					if err != nil {
						log.Error(err)
						// This should really not fail.
					}
					if err := broker.SendMessage(mq, delivered.CorrelationId, config.Broker.Exchange, config.Broker.RoutingKey, completed); err != nil {
						// TODO fix resend mechainsm
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
