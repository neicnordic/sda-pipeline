package postgres

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	// Needed implicitly to enable Postgres driver
	_ "github.com/lib/pq"
)

// Database defines methods to be implemented by SQLdb
type Database interface {
	GetHeader(fileID int) ([]byte, error)
	MarkCompleted(checksum string, fileID int) error
	MarkReady(accessionID, user, filepath, checksum string) error
	Close()
}

// SQLdb htructs that acts as a reciever for the db update methods
type SQLdb struct {
	Db *sql.DB
}

// Pgconf stores information about the db backend
type Pgconf struct {
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	Cacert     string
	SslMode    string
	ClientCert string
	ClientKey  string
}

// FileInfo is used by ingest
type FileInfo struct {
	Checksum string
	Size     int
	Path     string
}

// For testing

var sqlOpen = sql.Open

// NewDB creates a new DB connection
func NewDB(c Pgconf) (*SQLdb, error) {
	var err error

	connInfo := buildConnInfo(c)

	log.Debugf("Connecting to DB with <%s>", connInfo)
	db, err := sqlOpen("postgres", connInfo)
	if err != nil {
		log.Errorf("PostgresErrMsg 1: %s", err)
		panic(err)
	}

	if err = db.Ping(); err != nil {
		log.Errorf("Couldn't ping postgres database (%s)", err)
		panic(err)
	}

	return &SQLdb{Db: db}, err
}

func buildConnInfo(c Pgconf) string {

	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SslMode)

	if c.SslMode == "disable" {
		return connInfo
	}

	if c.Cacert != "" {
		connInfo += fmt.Sprintf(" sslrootcert=%s", c.Cacert)
	}

	if c.ClientCert != "" {
		connInfo += fmt.Sprintf(" sslcert=%s", c.ClientCert)
	}

	if c.ClientKey != "" {
		connInfo += fmt.Sprintf(" sslkey=%s", c.ClientKey)
	}

	return connInfo
}

// GetHeader retrieves the file header
func (dbs *SQLdb) GetHeader(fileID int) ([]byte, error) {
	db := dbs.Db
	const query = "SELECT header from local_ega.files WHERE id = $1"

	var hexString string
	if err := db.QueryRow(query, fileID).Scan(&hexString); err != nil {
		log.Error(err)
		return nil, err
	}

	header, err := hex.DecodeString(hexString)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return header, nil
}

// MarkCompleted marks the file as "COMPLETED"
func (dbs *SQLdb) MarkCompleted(checksum string, fileID int) error {
	db := dbs.Db
	const completed = "UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = $1, archive_file_checksum_type = 'SHA256'  WHERE id = $2;"
	result, err := db.Exec(completed, checksum, fileID)
	if err != nil {
		log.Errorf("something went wrong with the DB query: %s", err)
		return err
	}
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		log.Errorln("something went wrong with the query zero rows were changed")
	}
	return err
}

// InsertFile inserts a file in the database
func (dbs *SQLdb) InsertFile(filename, user string) (int64, error) {
	db := dbs.Db
	const query = "INSERT INTO local_ega.main(submission_file_path, submission_file_extension, submission_user, status, encryption_method) VALUES($1, $2, $3,'INIT', 'CRYPT4GH') RETURNING id;"
	var fileID int64
	err := db.QueryRow(query, filename, strings.Replace(filepath.Ext(filename), ".", "", -1), user).Scan(&fileID)
	if err != nil {
		log.Errorf("something went wrong with the DB query: %s", err)
	}

	return fileID, nil
}

// StoreHeader stores the file header in the database
func (dbs *SQLdb) StoreHeader(header []byte, id int64) error {
	db := dbs.Db
	const query = "UPDATE local_ega.files SET header = $1 WHERE id = $2;"
	result, err := db.Exec(query, hex.EncodeToString(header), id)
	if err != nil {
		log.Errorf("something went wrong with the DB query: %s", err)
		return err
	}
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		log.Errorln("something went wrong with the query zero rows were changed")
	}
	return err
}

// SetArchived markes the file as 'ARCHIVED'
func (dbs *SQLdb) SetArchived(file FileInfo, id int64) error {
	db := dbs.Db
	const query = "UPDATE local_ega.files SET status = 'ARCHIVED', archive_path = $1, archive_filesize = $2, inbox_file_checksum = $3, inbox_file_checksum_type = 'SHA256' WHERE id = $4;"
	result, err := db.Exec(query, file.Path, file.Size, file.Checksum, id)
	if err != nil {
		log.Errorf("something went wrong with the DB query: %s", err)
		return err
	}
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		log.Errorln("something went wrong with the query zero rows were changed")
	}
	return err
}

// MarkReady markes the file as "READY"
func (dbs *SQLdb) MarkReady(accessionID, user, filepath, checksum string) error {
	db := dbs.Db
	const ready = "UPDATE local_ega.files SET status = 'READY', stable_id = $1 WHERE elixir_id = $2 and inbox_path = $3 and inbox_file_checksum = $4 and status != 'DISABLED';"
	result, err := db.Exec(ready, accessionID, user, filepath, checksum)
	if err != nil {
		log.Errorf("something went wrong with the DB query: %s", err)
		return err
	}
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		log.Errorln("something went wrong with the query zero rows were changed")
	}
	return err
}

// MapFilesToDataset maps a set of files to a dataset in the database
func (dbs *SQLdb) MapFilesToDataset(datasetID string, accessionIDs []string) error {
	const getID = "SELECT file_id FROM local_ega.archive_files WHERE stable_id = $1"
	const mapping = "INSERT INTO local_ega_ebi.filedataset (file_id, dataset_stable_id) VALUES ($1, $2);"
	db := dbs.Db
	var fileID int64

	transaction, _ := db.Begin()
	for _, accessionID := range accessionIDs {

		err := db.QueryRow(getID, accessionID).Scan(&fileID)
		if err != nil {
			log.Errorf("something went wrong with the DB query: %s", err)
			if e := transaction.Rollback(); e != nil {
				log.Errorf("failed to rollback the transaction: %s", e)
			}
			return err
		}

		_, err = transaction.Exec(mapping, fileID, datasetID)
		if err != nil {
			log.Errorf("something went wrong with the DB query: %s", err)
			if e := transaction.Rollback(); e != nil {
				log.Errorf("failed to rollback the transaction: %s", e)
			}
			return err
		}
	}
	err := transaction.Commit()
	return err
}

// Close terminates the conmnection with the database
func (dbs *SQLdb) Close() {
	db := dbs.Db
	db.Close()
}
