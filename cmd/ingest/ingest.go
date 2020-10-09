// The ingest service accepts messages for files uploaded to the inbox,
// registers the files in the database with their headers, and stores them
// header-stripped in the archive storage.
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
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/keys"
	"github.com/elixir-oslo/crypt4gh/model/headers"
	"github.com/elixir-oslo/crypt4gh/streaming"
	"github.com/google/uuid"
	"github.com/xeipuuv/gojsonschema"

	log "github.com/sirupsen/logrus"
)

type trigger struct {
	Type     string `json:"type"`
	User     string `json:"user"`
	Filepath string `json:"filepath"`
}

// archived holds what should go in an message to inform about
// archival of files
type archived struct {
	User               string      `json:"user"`
	FilePath           string      `json:"filepath"`
	FileID             int64       `json:"file_id"`
	ArchivePath        string      `json:"archive_path"`
	EncryptedChecksums []checksums `json:"encrypted_checksums"`
	ReVerify           bool        `json:"re_verify"`
}

type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	conf, err := config.NewConfig("ingest")
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

	key, err := readKey(conf.Crypt4gh)
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

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	ingestTrigger := gojsonschema.NewReferenceLoader("file://schemas/ingestion-trigger.json")

	forever := make(chan bool)

	log.Info("starting ingest service")
	var message trigger

	go func() {
		messages, err := broker.GetMessages(mq, conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			res, err := gojsonschema.Validate(ingestTrigger, gojsonschema.NewBytesLoader(delivered.Body))
			if err != nil {
				log.Error(err)
				// publish MQ error
				continue
			}
			if !res.Valid() {
				log.Error(res.Errors())
				// publish MQ error
				continue
			}

			log.Debugf("Received a message: %s", delivered.Body)
			if err := json.Unmarshal(delivered.Body, &message); err != nil {
				log.Errorf("Unmarshaling json message failed, reason: %s", err)
				// publish MQ error
				continue
			}

			fileID, err := db.InsertFile(message.Filepath, message.User)
			if err != nil {
				log.Errorf("InsertFile failed, reason: %v", err)
				// This should really be handled by the DB retry mechanism
			}

			file, err := inbox.NewFileReader(message.Filepath)
			if err != nil {
				log.Errorf("Failed to open file: %s, reason: %v", message.Filepath, err)
				continue
			}

			fileSize, err := inbox.GetFileSize(message.Filepath)
			if err != nil {
				log.Errorf("Failed to get file size of: %s, reason: %v", message.Filepath, err)
				continue
			}

			// 4MiB readbuffer, this must be large enough that we get the entire header and the first 64KiB datablock
			// Should be made configurable once we have S3 support
			var bufSize int
			if bufSize = 4 * 1024 * 1024; conf.Inbox.S3.Chunksize > 4*1024*1024 {
				bufSize = conf.Inbox.S3.Chunksize
			}
			readBuffer := make([]byte, bufSize)
			hash := sha256.New()
			var bytesRead int64
			var byteBuf bytes.Buffer

			// Create a random uuid as file name
			archivedFile := uuid.New().String()
			dest, err := archive.NewFileWriter(archivedFile)
			if err != nil {
				log.Errorf("Failed to create file: %s, reason: %v", archivedFile, err)
				continue
			}

			for bytesRead < fileSize {
				i, _ := io.ReadFull(file, readBuffer)
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
					header, err := tryDecrypt(key, readBuffer)
					if err != nil {
						log.Errorln(err)
						continue
					}
					log.Debugln("store header")
					if err := db.StoreHeader(header, fileID); err != nil {
						log.Error("StoreHeader failed")
						// This should really be handled by the DB retry mechanism
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

			file.Close()
			dest.Close()
			log.Debugln("Mark as archived")
			fileInfo := database.FileInfo{}
			fileInfo.Path = archivedFile

			fileInfo.Size, err = archive.GetFileSize(archivedFile)

			if err != nil {
				log.Error("Couldn't get file size from archive")
				continue
			}

			fileInfo.Checksum = fmt.Sprintf("%x", hash.Sum(nil))
			if err := db.SetArchived(fileInfo, fileID); err != nil {
				log.Error("SetArchived failed")
				// This should really be handled by the DB retry mechanism
			}

			// Send message to archived
			msg := archived{
				User:        message.User,
				FilePath:    message.Filepath,
				FileID:      fileID,
				ArchivePath: archivedFile,
				EncryptedChecksums: []checksums{
					{"sha256", fmt.Sprintf("%x", hash.Sum(nil))},
				},
			}

			ingestedMsg := gojsonschema.NewReferenceLoader("file://schemas/ingestion-verification.json")
			res, err = gojsonschema.Validate(ingestedMsg, gojsonschema.NewGoLoader(msg))
			if err != nil {
				fmt.Println("error:", err)
				log.Error(err)
				// publish MQ error
				continue
			}
			if !res.Valid() {
				fmt.Println("result:", res.Errors())
				log.Error(res.Errors())
				// publish MQ error
				continue
			}

			brokerMsg, _ := json.Marshal(&msg)

			if err := broker.SendMessage(mq, delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, brokerMsg); err != nil {
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

// readKey reads and decrypts the c4gh key so it's ready for use
func readKey(conf config.Crypt4gh) (*[32]byte, error) {
	// Make sure the key path and passphrase is valid
	keyFile, err := os.Open(conf.KeyPath)
	if err != nil {
		return nil, err
	}

	key, err := keys.ReadPrivateKey(keyFile, []byte(conf.Passphrase))
	if err != nil {
		return nil, err
	}

	keyFile.Close()
	return &key, nil
}

// tryDecrypt tries to decrypt the start of buf.
func tryDecrypt(key *[32]byte, buf []byte) ([]byte, error) {

	log.Debugln("Try decrypting the first data block")
	a := bytes.NewReader(buf)
	b, err := streaming.NewCrypt4GHReader(a, *key, nil)
	if err != nil {
		log.Error(err)
		return nil, err

	}
	_, err = b.ReadByte()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	f := bytes.NewReader(buf)
	header, err := headers.ReadHeader(f)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return header, nil
}
