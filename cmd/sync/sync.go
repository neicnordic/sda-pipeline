// The sync service accepts messages for files that have been uploaded
// and creates a copy to the destination s3 backend
package main

import (
	"io"

	"sda-pipeline/internal/config"
	"sda-pipeline/internal/storage"

	log "github.com/sirupsen/logrus"
)

type trigger struct {
	Type     string `json:"type"`
	User     string `json:"user"`
	Filepath string `json:"filepath"`
}

func main() {
	conf, err := config.NewConfig("ingest")
	if err != nil {
		log.Fatal(err)
	}

	archive, err := storage.NewBackend(conf.Archive)
	if err != nil {
		log.Fatal(err)

	}

	inbox, err := storage.NewBackend(conf.Inbox)
	if err != nil {
		log.Fatal(err)

	}

	forever := make(chan bool)

	var message trigger
	message.Filepath = "Web.pdf"

	file, err := inbox.NewFileReader(message.Filepath)
	if err != nil {
		log.Errorf("Failed to open file: %s, reason: %v", message.Filepath, err)
		//continue
		return
	}

	dest, err := archive.NewFileWriter(message.Filepath)
	if err != nil {
		log.Errorf("Failed to create file, reason: %v", err)
		return
	}

	_, err = io.Copy(dest, file)
	if err != nil {
		log.Fatal(err)
	}

	file.Close()
	dest.Close()
	log.Debugln("Sync completed")

	<-forever
}
