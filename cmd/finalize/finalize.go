// The finalize command accepts messages with accessionIDs for
// ingested files and registers them in the database.
package main

import (
	"encoding/json"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	log "github.com/sirupsen/logrus"
)

// finalize struct that holds the json message data
type finalize struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// Completed is struct holding the full message data
type completed struct {
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
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
		connError := mq.ConnectionWatcher()
		log.Error(connError)
		os.Exit(1)
	}()

	forever := make(chan bool)

	log.Info("starting finalize service")
	var message finalize

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("Received a message (corr-id: %s, message: ***%s***)",
				delivered.CorrelationId,
				delivered.Body)

			err := mq.ValidateJSON(&delivered,
				"ingestion-accession",
				delivered.Body,
				&message)

			if err != nil {
				log.Errorf("Validation of incoming message failed "+
					"(corr-id: %s, error: %v",
					delivered.CorrelationId,
					err)
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

			c := completed{
				User:               message.User,
				Filepath:           message.Filepath,
				AccessionID:        message.AccessionID,
				DecryptedChecksums: message.DecryptedChecksums,
			}

			completeMsg, _ := json.Marshal(&c)

			err = mq.ValidateJSON(&delivered,
				"ingestion-completion",
				completeMsg,
				new(completed))

			if err != nil {
				log.Errorf("Validation of outgoing message failed "+
					"(corr-id: %s, error: %v",
					delivered.CorrelationId,
					err)

				continue
			}

			if err := db.MarkReady(message.AccessionID, message.User, message.Filepath, checksumSha256); err != nil {
				log.Errorf("MarkReady failed, reason: %v", err)
				// nack the message but requeue until we fixed the SQL retry.
				if e := delivered.Nack(false, true); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				continue
				// this should be handled by the SQL retry mechanism
			}

			log.Debug("Mark ready")

			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, completeMsg); err != nil {
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
