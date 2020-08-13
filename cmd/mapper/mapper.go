package main

import (
	"encoding/json"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/postgres"

	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

type message struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

func main() {
	conf, err := config.New("ingest")
	if err != nil {
		log.Fatal(err)
	}
	mq := broker.NewMQ(conf.Broker)
	db, err := postgres.NewDB(conf.Postgres)
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	datasetMapping := gojsonschema.NewReferenceLoader("file://schemas/dataset-mapping.json")

	forever := make(chan bool)

	log.Info("starting mapper service")
	var mappings message

	go func() {
		for d := range broker.GetMessages(mq, conf.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			res, err := gojsonschema.Validate(datasetMapping, gojsonschema.NewBytesLoader(d.Body))
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

			if err := json.Unmarshal(d.Body, &mappings); err != nil {
				log.Errorf("Unmarshaling json message failed, reason: %s", err)
				// publish MQ error
				continue
			}

			if err := db.MapFilesToDataset(mappings.DatasetID, mappings.AccessionIDs); err != nil {
				log.Errorf("MapfileToDataset failed, reason: %v", err)
				// this should be handled by the SQL retry mechanism
			}

			if err := d.Ack(false); err != nil {
				log.Errorf("failed to ack message for reason: %v", err)
			}
		}
	}()

	<-forever
}
