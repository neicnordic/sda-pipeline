// The finalize command accepts messages with accessionIDs for
// ingested files and registers them in the database.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type Checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// Completed is struct holding the full message data
type Completed struct {
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []Checksums `json:"decrypted_checksums"`
}

func main() {
	conf, err := config.NewConfig("finalize")
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

	log.Info("starting finalize service")
	var message Message

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("received a message: %s", delivered.Body)
			res, err := validateJSON(conf.SchemasPath, delivered.Body)
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

			// we unmarshal the message in the validation step so this is safe to do
			_ = json.Unmarshal(delivered.Body, &message)

			// Extract the sha256 from the message and use it for the database
			var checksumSha256 string
			for _, checksum := range message.DecryptedChecksums {
				if checksum.Type == "sha256" {
					checksumSha256 = checksum.Value
				}
			}
			log.Debug("Mark ready")
			if err := db.MarkReady(message.AccessionID, message.User, message.Filepath, checksumSha256); err != nil {
				log.Errorf("MarkReady failed, reason: %v", err)
				continue
				// this should be handled by the SQL retry mechanism
			}
			c := Completed{
				User:               message.User,
				Filepath:           message.Filepath,
				AccessionID:        message.AccessionID,
				DecryptedChecksums: message.DecryptedChecksums,
			}

			completeMsg := gojsonschema.NewReferenceLoader(conf.SchemasPath + "ingestion-completion.json")
			res, err = gojsonschema.Validate(completeMsg, gojsonschema.NewGoLoader(c))
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

			completed, _ := json.Marshal(&c)
			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, completed); err != nil {
				// TODO fix resend mechanism
				log.Errorln("We need to fix this resend stuff ...")
			}

			if err := delivered.Ack(false); err != nil {
				log.Errorf("failed to ack message for reason: %v", err)
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

	_, ok := message["type"]
	if ok {
		schema = gojsonschema.NewReferenceLoader(schemasPath + "ingestion-accession.json")
	} else {
		schema = gojsonschema.NewReferenceLoader(schemasPath + "ingestion-completion.json")
	}
	res, err := gojsonschema.Validate(schema, gojsonschema.NewBytesLoader(body))
	return res, err
}
