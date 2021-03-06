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
		connError := mq.ConnectionWatcher()
		log.Error(connError)
		os.Exit(1)
	}()

	forever := make(chan bool)

	log.Info("Starting intercept service")

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("Received a message: %s", delivered.Body)

			msgType, err := typeFromMessage(delivered.Body)
			if err != nil {
				log.Errorf("Failed to get type for message "+
					"(corr-id: %s, error: %v, message: %s)",
					delivered.CorrelationId,
					err,
					delivered.Body)
				continue
			}

			schema, err := schemaNameFromType(msgType)

			if err != nil {

				log.Errorf("Don't know schema for message type "+
					"(corr-id: %s, msgType: %s, error: %v, message: %s)",
					delivered.CorrelationId,
					msgType,
					err,
					delivered.Body)
				continue
			}

			err = mq.ValidateJSON(&delivered, schema, delivered.Body, nil)

			if err != nil {
				log.Errorf("Validation failed for message "+
					"(corr-id: %s, error: %v, schema: %s, message: %s)",
					delivered.CorrelationId,
					err,
					schema,
					delivered.Body)

				continue
			}

			routing := map[string]string{
				msgAccession: "accessionIDs",
				msgIngest:    "ingest",
				msgMapping:   "mappings",
			}

			routingKey := routing[msgType]

			if routingKey == "" {
				continue
			}

			log.Infof("Routing message "+
				"(corr-id: %s, routingkey: %s)",
				delivered.CorrelationId,
				routingKey)

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

// schemaNameFromType returns the schema to use for messages of
// type msgType
func schemaNameFromType(msgType string) (string, error) {
	m := map[string]string{
		msgAccession: "ingestion-accession",
		msgCancel:    "ingestion-trigger",
		msgIngest:    "ingestion-trigger",
		msgMapping:   "dataset-mapping",
	}

	if m[msgType] != "" {
		return m[msgType], nil
	}

	return "", fmt.Errorf("Don't know what schema to use for %s", msgType)
}

// typeFromMessage returns the type value given a JSON structure for the message
// supplied in body
func typeFromMessage(body []byte) (string, error) {
	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return "", err
	}

	msgTypeFetch, ok := message["type"]
	if !ok {
		return "", errors.New("Malformed message, type is missing")
	}

	msgType, ok := msgTypeFetch.(string)
	if !ok {
		return "", errors.New("Could not cast type attribute to string")
	}

	return msgType, nil
}
