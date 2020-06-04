package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

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

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting verify service")

	go func() {
		for d := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			var m Message
			e := json.Unmarshal(bytes.Replace(d.Body, []byte(`'`), []byte(`"`), -1), &m)
			if e != nil {
				log.Errorf("Not a json message: %s", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				d.Nack(false, false)
				// Send the errorus message to an error queue so it can be analyzed.
				if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			} else {
				e := reflect.ValueOf(&m).Elem()
				for i := 0; i < e.NumField(); i++ {
					if e.Field(i).Interface() == "" || e.Field(i).Interface() == 0 {
						log.Errorf("%s is missing", e.Type().Field(i).Name)
						err = fmt.Errorf("%s is missing", e.Type().Field(i).Name)
					}
				}
				if err != nil {
					// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
					d.Nack(false, false)
					// Send the errorus message to an error queue so it can be analyzed.
					if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
						log.Error("faild to publish message, reason: ", err)
					}
					// Restart on new message
					continue
				}
			}

			header, e := postgres.GetHeader(db, m.FileID)
			if e != nil {
				log.Error(e)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				d.Nack(false, false)
				// Send the errorus message to an error queue so it can be analyzed.
				if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				continue
			}

			// do file verfication
			k, e := os.Open(config.Crypt4gh.KeyPath)
			if e != nil {
				log.Error(e)
			}
			defer k.Close()

			key, e := keys.ReadPrivateKey(k, []byte(config.Crypt4gh.Passphrase))
			if e != nil {
				log.Error(e)
			}
			f := storage.FileReader(config.Archive.Type, filepath.Join(filepath.Clean(config.Archive.Location), m.ArchivePath))

			var buf bytes.Buffer
			buf.Write(header)
			var rw io.ReadWriter
			rw = &buf
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
			key = [32]byte{}

			// Mark file as "COMPLETED"
			if e := postgres.MarkCompleted(db, fmt.Sprintf("%x", hash.Sum(nil)), m.FileID); e != nil {
				// this should really be hadled by the DB retry mechanism
			} else {
				// Send message to completed
				c := Completed{
					User: m.User,
					DecryptedChecksums: []Checksums{
						{"SHA256", fmt.Sprintf("%x", hash.Sum(nil))},
					},
				}
				completed, err := json.Marshal(&c)
				if err != nil {
					// do something
				}
				broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingKey, completed)
				d.Ack(false)
			}
		}
	}()

	<-forever
}
