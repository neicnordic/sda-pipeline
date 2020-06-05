package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/postgres"

	log "github.com/sirupsen/logrus"
)

const completed = "UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = $1, archive_file_checksum_type = 'SHA256'  WHERE file_id = $3;"
const getHeader = "SELECT header from local_ega.files WHERE id = $1"

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
	dbs, err := postgres.NewDB(config.Postgres)
	db := dbs.Db
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting verify service")
	var message Message
	// var header []byte

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
				row := db.QueryRow(getHeader, message.FileID)
				var hexString string
				err = row.Scan(&hexString)
				if err != nil {
					log.Fatal(err)
				}
				header, err := hex.DecodeString(hexString)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(header)
			}

			d.Ack(false)
		}
	}()

	<-forever
}
