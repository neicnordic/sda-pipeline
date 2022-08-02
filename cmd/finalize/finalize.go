// The finalize command accepts messages with accessionIDs for
// ingested files and registers them in the database.
package main

import (
	"encoding/json"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/common"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	log "github.com/sirupsen/logrus"
)

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

	log.Info("Starting finalize service")
	var message common.IngestionAccession

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("Received a message (corr-id: %s, message: %s)",
				delivered.CorrelationId,
				delivered.Body)

			err := mq.ValidateJSON(&delivered,
				"ingestion-accession",
				delivered.Body,
				&message)

			if err != nil {
				log.Errorf("Validation of incoming message failed "+
					"(corr-id: %s, error: %v)",
					delivered.CorrelationId,
					err)

				continue
			}

			// we unmarshal the message in the validation step so this is safe to do
			_ = json.Unmarshal(delivered.Body, &message)

			log.Infof("Received work (corr-id: %s, "+
				"filepath: %s, "+
				"user: %s, "+
				"accessionid: %s, "+
				"decryptedChecksums: %v)",
				delivered.CorrelationId,
				message.FilePath,
				message.User,
				message.AccessionID,
				message.DecryptedChecksums)

			// Extract the sha256 from the message and use it for the database
			var checksumSha256 string
			for _, checksum := range message.DecryptedChecksums {
				if checksum.Type == "sha256" {
					checksumSha256 = checksum.Value
				}
			}

			c := common.IngestionCompletion{
				User:               message.User,
				FilePath:           message.FilePath,
				AccessionID:        message.AccessionID,
				DecryptedChecksums: message.DecryptedChecksums,
			}

			completeMsg, _ := json.Marshal(&c)

			err = mq.ValidateJSON(&delivered,
				"ingestion-completion",
				completeMsg,
				new(common.IngestionCompletion))

			if err != nil {
				log.Errorf("Validation of outgoing message failed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.FilePath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				continue
			}

			if err := db.MarkReady(message.AccessionID, message.User, message.FilePath, checksumSha256); err != nil {
				log.Errorf("MarkReady failed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.FilePath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				// Nack message so the server gets notified that something is wrong. Do not requeue.
				if e := delivered.Nack(false, false); e != nil {
					log.Errorf("Failed to NAck because of MarkReady failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.FilePath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				// Send the message to an error queue so it can be analyzed.
				infoErrorMessage := common.InfoError{
					Error:           "MarkReady failed",
					Reason:          err.Error(),
					OriginalMessage: message,
				}
				body, _ := json.Marshal(infoErrorMessage)
				if e := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingError, conf.Broker.Durable, body); e != nil {
					log.Errorf("Failed to publish MarkReady error message "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.FilePath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}

				continue
			}

			log.Infof("Set accession id for file "+
				"(corr-id: %s, "+
				"filepath: %s, "+
				"user: %s, "+
				"accessionid: %s, "+
				"decryptedChecksums: %v)",
				delivered.CorrelationId,
				message.FilePath,
				message.User,
				message.AccessionID,
				message.DecryptedChecksums)

			log.Debug("Mark ready")

			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, completeMsg); err != nil {
				// TODO fix resend mechanism
				log.Errorf("Failed to send message for completed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.FilePath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				// Restart loop, do not ack
				continue
			}

			if err := delivered.Ack(false); err != nil {

				log.Errorf("Failed to ack message after work completed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.FilePath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

			}
		}
	}()

	<-forever
}
