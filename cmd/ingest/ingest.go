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

const addfile = "SELECT local_ega.insert_files filename = $1, user_id = $2;"

// Message struct that holds the json message data
type Message struct {
	Filepath  string `json:"filepath"`
	Checksums []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"encrypted_checksums"`
	User string `json:"user"`
}

func main() {
	config := NewConfig()
	mq := broker.New(config.Broker)
	dbs, err := postgres.NewDB(config.Postgres)
	db := dbs.Db
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting ingest service")
	var message Message

	go func() {
		for d := range broker.GetMessages(mq, config.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			err := json.Unmarshal(bytes.Replace(d.Body, []byte(`'`), []byte(`"`), -1), &message)
			if err != nil {
				log.Errorf("Not a json message: %s", err)
			}

			e := reflect.ValueOf(&message).Elem()
			for i := 0; i < e.NumField(); i++ {
				if e.Field(i).Interface() == "" || e.Field(i).Interface() == 0 {
					log.Errorf("%s is missing", e.Type().Field(i).Name)
					err = fmt.Errorf("%s is missing", e.Type().Field(i).Name)
				}
			}

			d.Ack(false)
		}
	}()

	<-forever
}
