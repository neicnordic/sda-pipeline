package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/postgres"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/keys"
	"github.com/elixir-oslo/crypt4gh/model/headers"
	"github.com/elixir-oslo/crypt4gh/streaming"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// Message struct that holds the json message data
type Message struct {
	FilePath string `json:"filepath"`
	User     string `json:"user"`
}

// Archived is a struct holding the full message data
type Archived struct {
	User               string      `json:"user"`
	FileID             int64       `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []Checksums `json:"encrypted_checksums"`
}

// Checksums is struct for the checkksum type and value
type Checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	conf := config.New("ingest")
	mq := broker.New(conf.Broker)
	db, err := postgres.NewDB(conf.Postgres)
	if err != nil {
		log.Println("err:", err)
	}
	var archive, inbox storage.Backend

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	forever := make(chan bool)

	log.Info("starting ingest service")
	var message Message

	go func() {
		for delivered := range broker.GetMessages(mq, conf.Broker.Queue) {
			if conf.ArchiveType == "s3" {
				archive = storage.NewS3Backend(conf.ArchiveS3)
			} else {
				archive = storage.NewPosixBackend(conf.ArchivePosix)
			}

			if conf.InboxType == "s3" {
				inbox = storage.NewS3Backend(conf.InboxS3)

			} else {
				inbox = storage.NewPosixBackend(conf.InboxPosix)
			}

			log.Debugf("Received a message: %s", delivered.Body)
			if err := json.Unmarshal(delivered.Body, &message); err != nil {
				log.Errorf("Not a json message: %s", err)
			}

			fileID, err := db.InsertFile(message.FilePath, message.User)
			if err != nil {
				log.Errorf("InsertFile failed, reason: %v", err)
				// This should really be hadled by the DB retry mechanism
			}

			file, err := inbox.ReadFile(message.FilePath)
			if err != nil {
				log.Errorf("Failed to open file: %s, reason: %v", message.FilePath, err)
				continue
			}

			fileSize, err := inbox.GetFileSize(message.FilePath)
			if err != nil {
				log.Errorf("Failed to get file size of: %s, reason: %v", message.FilePath, err)
				continue
			}

			// 4MiB readbuffer, this must be large enough that we get the entire header and the first 64KiB datablock
			// Should be made configurable once we have S3 support
			var bufSize int
			if bufSize = 4 * 1024 * 1024; conf.InboxS3.Chunksize > 4*1024*1024 {
				bufSize = conf.InboxS3.Chunksize
			}
			readBuffer := make([]byte, bufSize)
			hash := sha256.New()
			var bytesRead int64
			var byteBuf bytes.Buffer

			// Create a random uuid as file name
			archivedFile := uuid.New().String()
			dest, err := archive.WriteFile(archivedFile)
			if err != nil {
				log.Errorf("Failed to create file: %s, reason: %v", archivedFile, err)
				continue
			}

			for bytesRead < fileSize {
				i, _ := file.Read(readBuffer)
				if i == 0 {
					return
				}
				// truncate the readbuffer if the file is smaller than the buffer size
				if i < len(readBuffer) {
					readBuffer = readBuffer[:i]
				}

				bytesRead = bytesRead + int64(i)

				h := bytes.NewReader(readBuffer)
				if _, err = io.Copy(hash, h); err != nil {
					log.Error(err)
				}

				//nolint:nestif
				if bytesRead <= int64(len(readBuffer)) {
					header, err := tryDecrypt(conf.Crypt4gh, readBuffer)
					if err != nil {
						log.Errorln(err)
						continue
					}
					log.Debugln("store header")
					if err := db.StoreHeader(header, fileID); err != nil {
						log.Error("StoreHeader failed")
						// This should really be hadled by the DB retry mechanism
					}

					if _, err = byteBuf.Write(readBuffer); err != nil {
						log.Errorf("Failed to write buffer, reason: %v", err)
						continue
					}

					// Strip header from buffer
					h := make([]byte, len(header))
					if _, err = byteBuf.Read(h); err != nil {
						log.Errorf("Failed to read buffer, reason: %v", err)
						continue
					}

				} else {
					if i < len(readBuffer) {
						readBuffer = readBuffer[:i]
					}
					if _, err = byteBuf.Write(readBuffer); err != nil {
						log.Errorf("failed to write to buffer, reason: %v", err)
						continue
					}
				}

				// Write data to file
				if _, err = byteBuf.WriteTo(dest); err != nil {
					log.Errorf("Failed to write buffer to file, reason: %v", err)
					continue
				}
			}

			log.Debugln("Mark as archived")
			fileInfo := postgres.FileInfo{}
			fileInfo.Checksum = fmt.Sprintf("%x", hash.Sum(nil))
			if err := db.SetArchived(fileInfo, fileID); err != nil {
				log.Error("SetArchived failed")
				// This should really be hadled by the DB retry mechanism
			}

			// Send message to archived
			msg := Archived{
				User:        message.User,
				FileID:      fileID,
				ArchivePath: archivedFile,
				EncryptedChecksums: []Checksums{
					{"SHA256", fmt.Sprintf("%x", hash.Sum(nil))},
				},
			}
			brokerMsg, err := json.Marshal(&msg)
			if err != nil {
				log.Error(err)
				// This should really not fail.
			}
			if err := broker.SendMessage(mq, delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, brokerMsg); err != nil {
				// TODO fix resend mechainsm
				log.Errorln("We need to fix this resend stuff ...")
			}
			if err := delivered.Ack(false); err != nil {
				log.Errorf("failed to ack message for reason: %v", err)
			}
		}
	}()

	<-forever
}

func tryDecrypt(c config.Crypt4gh, buf []byte) ([]byte, error) {
	keyFile, err := os.Open(c.KeyPath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	key, err := keys.ReadPrivateKey(keyFile, []byte(c.Passphrase))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugln("Try decrypting the first data block")
	a := bytes.NewReader(buf)
	b, err := streaming.NewCrypt4GHReader(a, key, nil)
	if err != nil {
		log.Error(err)
		return nil, err

	}
	_, err = b.ReadByte()
	if err != nil {
		log.Error(err)
		return nil, err
	}
	keyFile.Close()

	f := bytes.NewReader(buf)
	header, err := headers.ReadHeader(f)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return header, nil
}
