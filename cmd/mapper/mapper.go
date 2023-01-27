// The mapper service register mapping of accessionIDs
// (IDs for files) to datasetIDs.
package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	log "github.com/sirupsen/logrus"
)

type message struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

func main() {
	forever := make(chan bool, 1)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	defer func() {
		if r := recover(); r != nil {
			log.Infoln("Recovered")
		}
	}()

	go func() {
		<-sigc
		forever <- false
	}()

	conf, err := config.NewConfig("mapper")
	if err != nil {
		log.Error(err)
		sigc <- syscall.SIGINT
		panic(err)
	}
	mq, err := broker.NewMQ(conf.Broker)
	if err != nil {
		log.Error(err)
		sigc <- syscall.SIGINT
		panic(err)
	}
	defer mq.Channel.Close()
	defer mq.Connection.Close()

	db, err := database.NewDB(conf.Database)
	if err != nil {
		log.Error(err)
		sigc <- syscall.SIGINT
		panic(err)
	}
	defer db.Close()

	go func() {
		connError := mq.ConnectionWatcher()
		if connError != nil {
			log.Errorf("Broker connError: %v", connError)
			sigc <- syscall.SIGTERM
			// panic(connError)
		}
	}()

	log.Info("Starting mapper service")
	var mappings message

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Errorf("Failed to get message from mq (error: %v)", err)
			sigc <- syscall.SIGINT
			panic(err)
		}
		for delivered := range messages {
			log.Debugf("received a message: %s", delivered.Body)
			err := mq.ValidateJSON(&delivered, "dataset-mapping", delivered.Body, &mappings)
			if err != nil {
				log.Errorf("Failed to validate message for work "+
					"(corr-id: %s, "+
					"message: %s, "+
					"error: %v)",
					delivered.CorrelationId,
					delivered.Body,
					err)

				continue
			}

			if err := json.Unmarshal(delivered.Body, &mappings); err != nil {
				log.Errorf("Failed to unmarshal message for work "+
					"(corr-id: %s, "+
					"message: %s, "+
					"error: %v)",
					delivered.CorrelationId,
					delivered.Body,
					err)

				continue
			}

			if err := db.MapFilesToDataset(mappings.DatasetID, mappings.AccessionIDs); err != nil {
				log.Errorf("MapFilesToDataset failed  "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionids: %v, "+
					"error: %v)",
					delivered.CorrelationId,
					mappings.DatasetID,
					mappings.AccessionIDs,
					err)

				// Nack message so the server gets notified that something is wrong and requeue the message
				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to nack message on mapping files to dataset) "+
						"(corr-id: %s, "+
						"datasetid: %s, "+
						"accessionid: %s, "+
						"reason: %v)",
						delivered.CorrelationId,
						mappings.DatasetID,
						mappings.AccessionIDs,
						e)
				}

				continue
			}

			for _, aID := range mappings.AccessionIDs {
				log.Infof("Mapped file to dataset "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionid: %s)",
					delivered.CorrelationId,
					mappings.DatasetID,
					aID)
			}

			if err := delivered.Ack(false); err != nil {
				log.Errorf("Failed to ack message for work "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionids: %v, "+
					"error: %v)",
					delivered.CorrelationId,
					mappings.DatasetID,
					mappings.AccessionIDs,
					err)
			}
		}
	}()

	<-forever
	log.Infoln("exiting")
}
