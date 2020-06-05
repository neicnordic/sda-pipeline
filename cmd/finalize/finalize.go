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

const query = "UPDATE local_ega.files SET status = 'READY', stable_id = $1 WHERE elixir_id = $2 and inbox_path = $3 and inbox_file_checksum = $4 and status != 'DISABLED';"

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
	dbs, err := postgres.NewDB(config.Postgres)
	db := dbs.Db
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting finalize service")
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

			if err == nil {
				res, err := db.Exec(query, message.StableID, message.User, message.FilePath, message.Checksums[0].Value)
				if err != nil {
					log.Printf("something went wrong with the DB qurey: %s", err)
				}
				if ra, _ := res.RowsAffected(); ra == 0 {
					log.Println("something went wrong with the query zero rows where changed")
				}
			}

			d.Ack(false)
		}
	}()

	<-forever
}
