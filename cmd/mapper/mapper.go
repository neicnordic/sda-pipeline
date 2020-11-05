// The mapper service register mapping of accessionIDs
// (stable ids for files) to datasetIDs.
package main

import (
	"encoding/json"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"

	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

type message struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

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

	log.Info("starting mapper service")
	var mappings message

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for d := range messages {
			log.Debugf("received a message: %s", d.Body)
			res, err := validateJSON(conf.SchemasPath, d.Body)
			if err != nil {
				log.Errorf("josn error: %v", err)
				// Nack message so the server gets notified that something is wrong but don't requeue the message
				if e := d.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&d, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			}
			if !res.Valid() {
				log.Errorf("result.error: %v", res.Errors())
				log.Error("Validation failed")
				// Nack message so the server gets notified that something is wrong but don't requeue the message
				if e := d.Nack(false, false); e != nil {
					log.Errorln("failed to Nack message, reason: ", e)
				}
				// Send the message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&d, err.Error(), conf.Broker); e != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
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

// Validate the JSON in a received message
func validateJSON(schemasPath string, body []byte) (*gojsonschema.Result, error) {
	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return nil, err
	}

	schema := gojsonschema.NewReferenceLoader(schemasPath + "dataset-mapping.json")
	res, err := gojsonschema.Validate(schema, gojsonschema.NewBytesLoader(body))
	return res, err
}
