package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/postgres"

	log "github.com/sirupsen/logrus"
)

const completed = "UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = $1, archive_file_checksum_type = 'SHA256'  WHERE file_id = $3;"

// Message struct that holds the json message data
type Message struct {
	Filepath     string `json:"filepath"`
	User         string `json:"user"`
	FileID       int    `json:"file_id"`
	ArchivePath  string `json:"archive_path"`
	FileChecksum string `json:"file_checksum"`
}

func main() {
	config := NewConfig()
	mq := broker.New(config.Broker)
	db, err := postgres.NewDB(config.Postgres)
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting verify service")
	// var header []byte

	go func() {
		for d := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			var m Message
			e := json.Unmarshal(bytes.Replace(d.Body, []byte(`'`), []byte(`"`), -1), &m)
			if e != nil {
				log.Errorf("Not a json message: %s", err)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				d.Nack(false, false)
				// Send the errorus message to an error queue so it can be analyzed.
				if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				// Restart on new message
				continue
			} else {
				e := reflect.ValueOf(&m).Elem()
				for i := 0; i < e.NumField(); i++ {
					if e.Field(i).Interface() == "" || e.Field(i).Interface() == 0 {
						log.Errorf("%s is missing", e.Type().Field(i).Name)
						err = fmt.Errorf("%s is missing", e.Type().Field(i).Name)
					}
				}
				if err != nil {
					// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
					d.Nack(false, false)
					// Send the errorus message to an error queue so it can be analyzed.
					if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
						log.Error("faild to publish message, reason: ", err)
					}
					// Restart on new message
					continue
				}
			}

			header, e := postgres.GetHeader(db, m.FileID)
			if e != nil {
				log.Error(e)
				// Nack errorus message so the server gets notified that something is wrong but don't requeue the message
				d.Nack(false, false)
				// Send the errorus message to an error queue so it can be analyzed.
				if err := broker.SendMessage(mq, d.CorrelationId, config.Broker.Exchange, config.Broker.RoutingError, d.Body); err != nil {
					log.Error("faild to publish message, reason: ", err)
				}
				continue
			}
				if err != nil {
					log.Error(err)
					d.Nack(false, true)
				}
				fmt.Println(header)
			}

			d.Ack(false)
		}
	}()

	<-forever
}
