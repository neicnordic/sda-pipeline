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
	"sda-pipeline/internal/database"

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
				/// Nack message so the server gets notified that something is wrong. Do not requeue the message.
				if e := delivered.Nack(false, false); e != nil {
					log.Errorf("Failed to Nack message (get type for message) "+
						"(corr-id: %s, reason: %v)",
						delivered.CorrelationId,
						e)
				}
				// Send the message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, delivered.Body, mq.Conf, err.Error(), "Failed to get type for message"); e != nil {
					log.Errorf("Failed to publish message (get type for message), to error queue "+
						"(corr-id: %s, reason: %v)",
						delivered.CorrelationId, e)
				}
				// Restart on new message
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
				/// Nack message so the server gets notified that something is wrong. Do not requeue the message.
				if e := delivered.Nack(false, false); e != nil {
					log.Errorf("Failed to Nack message (unknown schema) "+
						"(corr-id: %s, msgType: %s, error: %v, message: %s)",
						delivered.CorrelationId, msgType, err, delivered.Body)
				}
				// Send the message to an error queue so it can be analyzed.
				if e := mq.SendJSONError(&delivered, delivered.Body, mq.Conf, err.Error(), "Don't know schema for message type"); e != nil {
					log.Errorf("Failed to publish message (unknown schema), to error queue "+
						"(corr-id: %s, reason: %v)",
						delivered.CorrelationId, e)
				}
				// Restart on new message
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
				msgCancel:    "ingest",
				msgIngest:    "ingest",
				msgMapping:   "mappings",
			}

			switch msgType {
			case msgCancel:
				message := make(map[string]interface{})
				_ = json.Unmarshal(delivered.Body, &message)
				log.Debugln("mark file as DISABLED")
				if err := db.DisableFile(message["filepath"].(string), message["user"].(string)); err != nil {
					log.Errorf("MarkDisabled failed: %v", err)
					if err := delivered.Nack(false, true); err != nil {
						log.Errorf("Failed to nack message: %v", err)
					}

					continue
				}

				if err := delivered.Ack(false); err != nil {
					log.Errorf("Failed to ack message: %v", err)
				}

				continue

			case msgIngest:
				message := make(map[string]interface{})
				_ = json.Unmarshal(delivered.Body, &message)

				ID, err := db.GetFileID(message["filepath"].(string), message["user"].(string))
				if err != nil {
					switch err.Error() {
					case "sql: no rows in result set":
						log.Debugln("inserting file in DB")
						ID, err = db.InsertFile(message["filepath"].(string), message["user"].(string))
						if err != nil {
							log.Errorf("InsertFile failed: %v", err)

							if err := delivered.Nack(false, true); err != nil {
								log.Errorf("Failed to nack message: %v", err)
							}

							continue
						}
					default:
						log.Errorf("Failed to get file id: %v", err)
						if err := delivered.Nack(false, true); err != nil {
							log.Errorf("Failed to nack message: %v", err)
						}

						continue
					}
				}

				status, err := db.GetStatus(ID)
				log.Debugf("status: %v", status)
				if err != nil {
					log.Errorf("Failed to check file status: %v", err)

					if err := delivered.Nack(false, true); err != nil {
						log.Errorf("Failed to nack message: %v", err)
					}

					continue
				}
				if status == "DISABLED" {
					if err := db.ResetFileStatus(ID); err != nil {
						log.Errorf("Failed to reset file status: %v", err)

						if err := delivered.Nack(false, true); err != nil {
							log.Errorf("Failed to nack message: %v", err)
						}

						continue
					}
				}

			case "":
				continue
			}

			log.Infof("Routing message "+
				"(corr-id: %s, routingkey: %s)",
				delivered.CorrelationId,
				routing[msgType])

			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, routing[msgType], conf.Broker.Durable, delivered.Body); err != nil {
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
