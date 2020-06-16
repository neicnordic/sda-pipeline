package main

import (
	"encoding/json"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/postgres"

	log "github.com/sirupsen/logrus"
)

type message struct {
	Type         string   `json:"type"`
	DatasetID    string   `json:"dataset_id"`
	AccessionIDs []string `json:"accession_ids"`
}

func main() {
	conf := config.New("ingest")
	mq := broker.New(conf.Broker)
	db, err := postgres.NewDB(conf.Postgres)
	if err != nil {
		log.Println("err:", err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting mapper service")
	var mappings message

	go func() {
		for d := range broker.GetMessages(mq, conf.Broker.Queue) {
			log.Debugf("received a message: %s", d.Body)
			// TODO verify json structure
			err = json.Unmarshal(d.Body, &mappings)
			if err != nil {
				log.Errorf("Not a json message: %s", err)
			}
			for _, accessionID := range mappings.AccessionIDs {
				fileID, err := db.GetFileIDByAccessionID(accessionID)
				if err != nil {
					log.Errorf("failed to get FileID from AccecssionID: %v", accessionID)
					// this should be handled by the SQL retry mechanism
				}

				err = db.MapfileToDataset(int(fileID), mappings.DatasetID)
				if err != nil {
					log.Errorf("MapfileToDataset failed, reason: %v", err)
					// this should be handled by the SQL retry mechanism
				}
			}

			if err := d.Ack(false); err != nil {
				log.Errorf("failed to ack message for reason: %v", err)
			}

		}
	}()

	<-forever
}
