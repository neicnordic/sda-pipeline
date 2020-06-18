package main

import (
	"encoding/json"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/postgres"

	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	Type               string `json:"type"`
	User               string `json:"user"`
	Filepath           string `json:"filepath"`
	AccessionID        string `json:"accession_id"`
	DecryptedChecksums []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"decrypted_checksums"`
}

func main() {
	config := config.New("finalize")
	mq := broker.New(config.Broker)
	db, err := postgres.NewDB(config.Postgres)
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	ingestAccession := gojsonschema.NewReferenceLoader("file://schemas/ingestion-accession.json")

	forever := make(chan bool)

	log.Info("starting finalize service")
	var message Message

	go func() {
		for d := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			res, err := gojsonschema.Validate(ingestAccession, gojsonschema.NewBytesLoader(d.Body))
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

			if err := json.Unmarshal(d.Body, &message); err != nil {
				log.Errorf("Unmarshaling json message failed, reason: %s", err)
				// publish MQ error
				continue
			}

			if err == nil {
				err := db.MarkReady(message.AccessionID, message.User, message.Filepath, message.DecryptedChecksums[0].Value)
				if err != nil {
					log.Errorf("MarkReady failed, reason: %v", err)
					// this should be handled by the SQL retry mechanism
				}
			}

			if err := d.Ack(false); err != nil {
				log.Errorf("failed to ack message for reason: %v", err)
			}
		}
	}()

	<-forever
}
