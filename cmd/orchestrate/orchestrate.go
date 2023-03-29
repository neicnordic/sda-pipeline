// The orchestrate service plays the role of processing messages
// in stand-alone operations.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	guuid "github.com/google/uuid"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"

	log "github.com/sirupsen/logrus"
)

const (
	queueVerify    string = "verified"
	queueInbox     string = "inbox"
	queueComplete  string = "completed"
	queueBackup    string = "backup"
	queueMapping   string = "mappings"
	queueIngest    string = "ingest"
	queueAccession string = "accessionIDs"
)

type upload struct {
	Operation          string      `json:"operation"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	Filesize           int         `json:"filesize"`
	LastModified       string      `json:"file_last_modified,omitempty"`
	EncryptedChecksums []checksums `json:"encrypted_checksums,omitempty"`
}

type request struct {
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

type trigger struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	EncryptedChecksums []checksums `json:"encrypted_checksums"`
}

// finalize struct that holds the json message data
type finalize struct {
	Type               string      `json:"type"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

type mapping struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

// Checksums is struct for the checksum type and value
type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	conf, err := config.NewConfig("orchestrate")
	if err != nil {
		log.Fatal(err)
	}
	mq, err := broker.NewMQ(conf.Broker)
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()

	queues := []string{queueInbox, queueVerify, queueComplete}

	go func() {
		connError := mq.ConnectionWatcher()
		log.Error(connError)
		os.Exit(1)
	}()

	routing := map[string]string{
		queueVerify:   queueAccession,
		queueInbox:    queueIngest,
		queueComplete: queueMapping,
	}

	forever := make(chan bool)

	log.Info("Starting orchestrate service")

	for _, queue := range queues {
		routingKey := routing[queue]
		go processQueue(mq, queue, routingKey, conf.Broker.Durable)
	}
	<-forever
}

func processQueue(mq *broker.AMQPBroker, queue string, routingKey string, durable bool) {
	log.Infof("Monitoring queue: %s", queue)

	messages, err := mq.GetMessages(queue)
	if err != nil {
		log.Fatal(err)
	}
	for delivered := range messages {
		log.Debugf("Received a message: %s", delivered.Body)

		schema, err := schemaNameFromQueue(queue, delivered.Body)

		if err != nil {
			log.Errorf("Don't know schema for message type "+
				"(corr-id: %s, queue: %s, error: %v, message: %s)",
				delivered.CorrelationId,
				queue,
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

		var publishMsg []byte
		var publishType interface{}

		routingSchema, err := schemaNameFromQueue(routingKey, nil)

		if err != nil {
			log.Errorf("Don't know schema for routing key "+
				"(corr-id: %s, routing-key: %s, error: %v)",
				delivered.CorrelationId,
				routingKey,
				err,
			)

			continue
		}

		switch routingKey {
		case queueAccession:
			publishMsg, publishType = finalizeMessage(delivered.Body)
		case queueIngest:
			publishMsg, publishType = ingestMessage(delivered.Body)
		case queueMapping:
			publishMsg, publishType = mappingMessage(delivered.Body)
		}

		err = mq.ValidateJSON(&delivered,
			routingSchema,
			publishMsg,
			publishType)

		if err != nil {
			log.Errorf("Validation of outgoing message failed "+
				"(corr-id: %s, "+
				"error: %v)",
				delivered.CorrelationId,
				err)

			continue
		}

		log.Infof("Routing message "+
			"(corr-id: %s, routingkey: %s, message: %s)",
			delivered.CorrelationId,
			routingKey,
			publishMsg)

		if err := mq.SendMessage(delivered.CorrelationId, mq.Conf.Exchange, routingKey, durable, publishMsg); err != nil {
			// TODO fix resend mechanism
			log.Errorln("We need to fix this resend stuff ...")
		}
		if err := delivered.Ack(false); err != nil {
			log.Errorf("failed to ack message for reason: %v", err)
		}
	}
}

// schemaNameFromQueue returns the schema to use for messages
// determined by the queue
func schemaNameFromQueue(queue string, body []byte) (string, error) {
	if queue == queueInbox {
		return schemaFromInboxOperation(body)
	}
	m := map[string]string{
		queueVerify:    "ingestion-accession-request",
		queueComplete:  "ingestion-completion",
		queueIngest:    "ingestion-trigger",
		queueMapping:   "dataset-mapping",
		queueAccession: "ingestion-accession",
		queueBackup:    "ingestion-completion",
	}

	if m[queue] != "" {
		return m[queue], nil
	}

	return "", fmt.Errorf("Don't know what schema to use for %s", queue)
}

// schemaFromInboxOperation returns the operation done in inbox
// supplied in body of the message
func schemaFromInboxOperation(body []byte) (string, error) {
	message := make(map[string]interface{})
	err := json.Unmarshal(body, &message)
	if err != nil {
		return "", err
	}

	inboxOperationFetch, ok := message["operation"]
	if !ok {
		return "", errors.New("Malformed message, inbox operation is missing")
	}

	inboxOps, ok := inboxOperationFetch.(string)
	if !ok {
		return "", errors.New("Could not cast operation attribute to string")
	}

	switch inboxOps {
	case "upload":
		return "inbox-upload", nil
	case "rename":
		return "inbox-rename", nil
	case "remove":
		return "inbox-remove", nil
	default:
		return "", errors.New("Could not recognize inbox operation")
	}

}

func ingestMessage(body []byte) ([]byte, interface{}) {
	var message upload
	err := json.Unmarshal(body, &message)
	if err != nil {
		return nil, nil
	}

	msg := trigger{
		Type:               "ingest",
		User:               message.User,
		Filepath:           message.Filepath,
		EncryptedChecksums: message.EncryptedChecksums,
	}

	publish, _ := json.Marshal(&msg)

	return publish, new(trigger)
}

func finalizeMessage(body []byte) ([]byte, interface{}) {
	var message request
	err := json.Unmarshal(body, &message)
	if err != nil {
		return nil, nil
	}
	accessionID := fmt.Sprintf("urn:%s:%s", message.User, genUUID())

	msg := finalize{
		Type:               "accession",
		User:               message.User,
		Filepath:           message.Filepath,
		DecryptedChecksums: message.DecryptedChecksums,
		AccessionID:        accessionID,
	}

	publish, _ := json.Marshal(&msg)

	return publish, new(finalize)
}

func mappingMessage(body []byte) ([]byte, interface{}) {
	var message finalize
	if err := json.Unmarshal(body, &message); err != nil {
		return nil, nil
	}
	datasetID := fmt.Sprintf("urn:%s:%s", message.User, genUUID())

	msg := mapping{
		Type:         "mapping",
		DatasetID:    datasetID,
		AccessionIDs: []string{message.AccessionID},
	}

	publish, _ := json.Marshal(&msg)

	return publish, new(mapping)
}

func genUUID() string {
	id := guuid.New().String()
	log.Infof("Generated String UUID: %s", id)

	return id
}
