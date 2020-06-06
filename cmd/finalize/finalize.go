package main

import (
	"encoding/json"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/postgres"

	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	User      string `json:"user"`
	StableID  string `json:"stable_id"`
	FilePath  string `json:"file_path"`
	Checksums []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"decrypted_checksums"`
}

func main() {
	config := NewConfig()
	mq := broker.New(config.Broker)
	db, err := postgres.NewDB(config.Postgres)
	if err != nil {
		log.Println("err:", err)
	}

	defer db.Close()
	defer mq.Channel.Close()
	defer mq.Connection.Close()

	forever := make(chan bool)

	log.Info("starting finalize service")
	var message Message

	go func() {
		for d := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			// TODO verify json structure
			err := json.Unmarshal(d.Body, &message)
			if err != nil {
				log.Errorf("Not a json message: %s", err)
			}

			if err == nil {
				err := db.MarkReady(message.StableID, message.User, message.FilePath, message.Checksums[0].Value)
				if err != nil {
					// this should be handled by the SQL retry mechanism
				}
			}

			d.Ack(false)
		}
	}()

	<-forever
}
