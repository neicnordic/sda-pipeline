// The mapper service register mapping of accessionIDs
// (IDs for files) to datasetIDs.
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
	conf, err := config.NewConfig("mapper")
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

	log.Info("Starting mapper service")
	var mappings common.DatasetMapping

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatalf("Failed to get message from mq (error: %v)", err)
		}
		for d := range messages {
			log.Debugf("received a message: %s", d.Body)
			err := mq.ValidateJSON(&d, "dataset-mapping", d.Body, &mappings)
			if err != nil {
				log.Errorf("Failed to validate message for work "+
					"(corr-id: %s, "+
					"message: %s, "+
					"error: %v)",
					d.CorrelationId,
					d.Body,
					err)

				continue
			}

			if err := json.Unmarshal(d.Body, &mappings); err != nil {
				log.Errorf("Failed to unmarshal message for work "+
					"(corr-id: %s, "+
					"message: %s, "+
					"error: %v)",
					d.CorrelationId,
					d.Body,
					err)

				continue
			}

			if err := db.MapFilesToDataset(mappings.DatasetID, mappings.AccessionIDs); err != nil {
				log.Errorf("MapFilesToDataset failed  "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionids: %v, "+
					"error: %v)",
					d.CorrelationId,
					mappings.DatasetID,
					mappings.AccessionIDs,
					err)
			}

			for _, aID := range mappings.AccessionIDs {
				log.Infof("Mapped file to dataset "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionid: %s)",
					d.CorrelationId,
					mappings.DatasetID,
					aID)
			}

			if err := d.Ack(false); err != nil {
				log.Errorf("Failed to ack message for work "+
					"(corr-id: %s, "+
					"datasetid: %s, "+
					"accessionids: %v, "+
					"error: %v)",
					d.CorrelationId,
					mappings.DatasetID,
					mappings.AccessionIDs,
					err)

			}
		}
	}()

	<-forever
}
