// The intercept service relays message between the queue
// provided from the federated service and local queues.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"

	log "github.com/sirupsen/logrus"
	"github.com/xeipuuv/gojsonschema"
)

	const (
		msgAccession string = "accession"
		msgCancel    string = "cancel"
		msgIngest    string = "ingest"
		msgMapping   string = "mapping"
	)

func main() {
	conf, err := config.NewConfig("intercept")
	if err != nil {
		log.Fatal(err)
	}
	mq, err := broker.NewMQ(conf.Broker)
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()


	go func() {
		for {
			connError := mq.ConnectionWatcher()
			log.Error(connError)
			os.Exit(1)
		}
	}()

	forever := make(chan bool)

	log.Info("starting intercept service")

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("received a message: %s", delivered.Body)
			msgType, res, err := validateJSON(delivered.Body)
			if res == nil {
				continue
			}
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

			var routingKey string

			switch msgType {
				case msgAccession:
					routingKey = "accessionIDs"
				case msgCancel:
					routingKey = ""
					continue
				case msgIngest:
					routingKey = "ingest"
				case msgMapping:
					routingKey = "mappings"
				default:
					log.Debug("Unknown type")
					routingKey = ""
					continue
			}

			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, routingKey, conf.Broker.Durable, delivered.Body); err != nil {
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
func validateJSON(body []byte) (string, *gojsonschema.Result, error) {
	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return "", nil, err
	}

	msgType, ok := message["type"]
	if !ok {
		return "", nil, errors.New("Malformed message, type is missing")
	}

	var schema gojsonschema.JSONLoader
	var res *gojsonschema.Result

	switch msgType {
		case msgAccession:
			schema = gojsonschema.NewReferenceLoader("file://../../schemas/federated/ingestion-accession.json")
		case msgCancel:
			schema = gojsonschema.NewReferenceLoader("file://../../schemas/federated/ingestion-trigger.json")
			msgType = ""
		case msgIngest:
			schema = gojsonschema.NewReferenceLoader("file://../../schemas/federated/ingestion-trigger.json")
		case msgMapping:
			schema = gojsonschema.NewReferenceLoader("file://../../schemas/federated/dataset-mapping.json")
		default:
			schema = gojsonschema.NewStringLoader(`{"required": ["type"],
												"properties": {
														"type": {
															"type": "string",
															"title": "The message type",
															"description": "The message type",
															"enum": ["accession", "cancel", "ingest", "mapping"]
														}
													}
												}`)
			msgType = ""
	}

	res, err = gojsonschema.Validate(schema, gojsonschema.NewBytesLoader(body))
	return fmt.Sprintf("%v", msgType), res, err
}
