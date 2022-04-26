// The backup command accepts messages with accessionIDs for
// ingested files and copies them to the second storage.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/config"
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/storage"

	"github.com/elixir-oslo/crypt4gh/model/headers"
	log "github.com/sirupsen/logrus"
)

// Backup struct that holds the json message data
type backup struct {
	Type               string      `json:"type,omitempty"`
	User               string      `json:"user"`
	Filepath           string      `json:"filepath"`
	AccessionID        string      `json:"accession_id"`
	DecryptedChecksums []checksums `json:"decrypted_checksums"`
}

// Checksums is struct for the checksum type and value
type checksums struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func main() {
	conf, err := config.NewConfig("backup")
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
	backup, err := storage.NewBackend(conf.Backup)
	if err != nil {
		log.Fatal(err)
	}
	archive, err := storage.NewBackend(conf.Archive)
	if err != nil {
		log.Fatal(err)
	}

	key, err := config.GetC4GHKey()
	if err != nil {
		log.Fatal(err)
	}

	publicKey, err := config.GetC4GHPublicKey()
	if err != nil {
		log.Fatal(err)
	}

	defer mq.Channel.Close()
	defer mq.Connection.Close()
	defer db.Close()

	go func() {
		connError := mq.ConnectionWatcher()
		log.Error(connError)
		os.Exit(1)
	}()

	forever := make(chan bool)

	log.Info("Starting backup service")
	var message backup
	jsonSchema := "ingestion-completion"

	if conf.Broker.Queue == "accessionIDs" {
		jsonSchema = "ingestion-accession"
	}

	go func() {
		messages, err := mq.GetMessages(conf.Broker.Queue)
		if err != nil {
			log.Fatal(err)
		}
		for delivered := range messages {
			log.Debugf("Received a message (corr-id: %s, message: %s)",
				delivered.CorrelationId,
				delivered.Body)

			err := mq.ValidateJSON(&delivered,
				jsonSchema,
				delivered.Body,
				&message)

			if err != nil {
				log.Errorf("Validation of incoming message failed "+
					"(corr-id: %s, error: %v)",
					delivered.CorrelationId,
					err)
				continue
			}

			// we unmarshal the message in the validation step so this is safe to do
			_ = json.Unmarshal(delivered.Body, &message)

			log.Infof("Received work (corr-id: %s, "+
				"filepath: %s, "+
				"user: %s, "+
				"accessionid: %s, "+
				"decryptedChecksums: %v)",
				delivered.CorrelationId,
				message.Filepath,
				message.User,
				message.AccessionID,
				message.DecryptedChecksums)

			// Extract the sha256 from the message and use it for the database
			var checksumSha256 string
			for _, checksum := range message.DecryptedChecksums {
				if checksum.Type == "sha256" {
					checksumSha256 = checksum.Value
				}
			}

			var filePath string
			var fileSize int
			if filePath, fileSize, err = db.GetArchived(message.User, message.Filepath, checksumSha256); err != nil {
				log.Errorf("GetArchived failed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				// nack the message but requeue until we fixed the SQL retry.
				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of GetArchived failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				continue
			}

			log.Debug("Backup initiated")

			// Get size on disk, will also give some time for the file to
			// appear if it has not already

			diskFileSize, err := archive.GetFileSize(filePath)

			if err != nil {
				log.Errorf("Failed to get size info for archived file %s "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					filePath,
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of GetFileSize failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}

				continue
			}

			if diskFileSize != int64(fileSize) {
				log.Errorf("File size in archive does not match database for archive file %s "+
					"- archive size is %d, database has %d "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					filePath,
					diskFileSize,
					fileSize,
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of file size differences failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				continue

			}

			file, err := archive.NewFileReader(filePath)
			if err != nil {
				log.Errorf("Failed to open archived file %s "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					filePath,
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				//FIXME: should it retry?
				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of NewFileReader failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				continue
			}

			dest, err := backup.NewFileWriter(filePath)
			if err != nil {
				log.Errorf("Failed to open backup file %s for writing "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					filePath,
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				//FIXME: should it retry?
				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of NewFileWriter failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				continue
			}

			// Check if the header is needed
			//nolint:nestif
			if config.CopyHeader() {
				// Get the header from db
				header, err := db.GetHeaderForStableId(message.AccessionID)
				if err != nil {
					log.Errorf("GetHeaderForStableId failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						err)
				}

				// Decrypt header
				log.Debug("Decrypt header")
				DecrHeader, err := FormatHexHeader(header, *key)
				if err != nil {
					log.Errorf("Failed to decrypt the header %s "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						filePath,
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						err)

					if e := delivered.Nack(false, true); e != nil {
						log.Errorf("Failed to NAck because of decrypt header failed "+
							"(corr-id: %s, "+
							"filepath: %s, "+
							"user: %s, "+
							"accessionid: %s, "+
							"decryptedChecksums: %v, error: %v)",
							delivered.CorrelationId,
							message.Filepath,
							message.User,
							message.AccessionID,
							message.DecryptedChecksums,
							e)
					}
				}

				// Reencrypt header
				log.Debug("Reencrypt header")
				newHeader, err := reencryptHeader(*key, *publicKey, *DecrHeader)
				if err != nil {
					log.Errorf("Failed to reencrypt the header %s "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						filePath,
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						err)

					if e := delivered.Nack(false, true); e != nil {
						log.Errorf("Failed to NAck because of reencrypt header failed "+
							"(corr-id: %s, "+
							"filepath: %s, "+
							"user: %s, "+
							"accessionid: %s, "+
							"decryptedChecksums: %v, error: %v)",
							delivered.CorrelationId,
							message.Filepath,
							message.User,
							message.AccessionID,
							message.DecryptedChecksums,
							e)
					}
				}

				// write header to destination file
				_, err = dest.Write(newHeader)
				if err != nil {
					log.Errorf("Failed to write the header to destination %s "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						filePath,
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						err)
				}
			}

			// Copy the file and check is sizes match
			copiedSize, err := io.Copy(dest, file)
			if err != nil || copiedSize != int64(fileSize) {
				log.Errorf("Failed to copy file "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				//FIXME: should it retry?
				if e := delivered.Nack(false, true); e != nil {
					log.Errorf("Failed to NAck because of Copy failed "+
						"(corr-id: %s, "+
						"filepath: %s, "+
						"user: %s, "+
						"accessionid: %s, "+
						"decryptedChecksums: %v, error: %v)",
						delivered.CorrelationId,
						message.Filepath,
						message.User,
						message.AccessionID,
						message.DecryptedChecksums,
						e)
				}
				continue
			}

			file.Close()
			dest.Close()

			log.Infof("Backuped file %s (%d bytes) from archive to backup "+
				"(corr-id: %s, "+
				"filepath: %s, "+
				"user: %s, "+
				"accessionid: %s, "+
				"decryptedChecksums: %v)",
				filePath,
				fileSize,
				delivered.CorrelationId,
				message.Filepath,
				message.User,
				message.AccessionID,
				message.DecryptedChecksums)

			if err := mq.SendMessage(delivered.CorrelationId, conf.Broker.Exchange, conf.Broker.RoutingKey, conf.Broker.Durable, delivered.Body); err != nil {
				// TODO fix resend mechanism
				log.Errorf("Failed to send message for completed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

				// Restart loop, do not ack
				continue
			}

			if err := delivered.Ack(false); err != nil {

				log.Errorf("Failed to ack message after work completed "+
					"(corr-id: %s, "+
					"filepath: %s, "+
					"user: %s, "+
					"accessionid: %s, "+
					"decryptedChecksums: %v, error: %v)",
					delivered.CorrelationId,
					message.Filepath,
					message.User,
					message.AccessionID,
					message.DecryptedChecksums,
					err)

			}
		}
	}()

	<-forever
}

// FormatHexHeader decrypts a hex formatted file header using the proivided secret key,
// and returns the data as a Header struct
func FormatHexHeader(hexData string, secKey [32]byte) (*headers.Header, error) {

	// Trim whitespace that might otherwise confuse the hex parse
	headerHexStr := strings.TrimSpace(hexData)

	// Decode the hex
	binaryHeader, err := hex.DecodeString(headerHexStr)
	if err != nil {
		return nil, err
	}

	// This will return the same data, but will check validity
	headerReader := bytes.NewReader(binaryHeader)
	_, err = headers.ReadHeader(headerReader)
	if err != nil {
		return nil, err
	}

	// Rewind header reader
	_, err = headerReader.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	// Create the header struct
	header, err := headers.NewHeader(headerReader, secKey)
	if err != nil {
		return nil, err
	}

	return header, nil
}

// Modified struct of the Crypt4GHWriter struct which can be found here:
// https://github.com/elixir-oslo/crypt4gh/blob/master/streaming/out.go
type headerWriter struct {
	header                               headers.Header
	dataEncryptionParametersHeaderPacket headers.DataEncryptionParametersHeaderPacket
}

// reencryptHeader reencrypts the given header by decrypting it with the givern secKey, and
// reencrypting it for the pubKey.
func reencryptHeader(secKey, pubKey [32]byte, header headers.Header) ([]byte, error) {

	buffer := bytes.Buffer{}

	// Get data encryption key from previous header
	encryptionPackets, err := header.GetDataEncryptionParameterHeaderPackets()
	if err != nil {
		return nil, err
	}
	encryptionPacket := headers.DataEncryptionParametersHeaderPacket{}
	for _, encPack := range *encryptionPackets {
		encryptionPacket = encPack
	}

	fileHeader := make([]headers.HeaderPacket, 0)

	headerWriter := headerWriter{}
	headerWriter.dataEncryptionParametersHeaderPacket = encryptionPacket

	// Add the private and public keys to the header
	fileHeader = append(fileHeader, headers.HeaderPacket{
		WriterPrivateKey:       secKey,
		ReaderPublicKey:        pubKey,
		HeaderEncryptionMethod: headers.X25519ChaCha20IETFPoly1305,
		EncryptedHeaderPacket:  encryptionPacket,
	})

	// Create the main header block
	headerWriter.header = headers.Header{
		MagicNumber:       header.MagicNumber,
		Version:           header.Version,
		HeaderPacketCount: uint32(len(fileHeader)),
		HeaderPackets:     fileHeader,
	}

	// Convert to binary
	binaryHeader, err := headerWriter.header.MarshalBinary()
	if err != nil {
		return nil, err
	}

	_, err = buffer.Write(binaryHeader)
	if err != nil {
		return nil, err
	}
	buffer.Grow(headers.UnencryptedDataSegmentSize)

	return buffer.Bytes(), nil
}
