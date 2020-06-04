package postgres

import (
	"database/sql"
	"encoding/hex"
	"fmt"

	log "github.com/sirupsen/logrus"

	// Needed implicitly to enable Postgres driver
	_ "github.com/lib/pq"
)

// SQLdb is
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

// NewDB creates a new DB connection
func NewDB(c Pgconf) (*sql.DB, error) {
	var err error

	connInfo := buildConnInfo(c)

	log.Debugf("Connecting to DB with <%s>", connInfo)
	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		log.Errorf("PostgresErrMsg 1: %s", err)
		panic(err)
	}

	if err = db.Ping(); err != nil {
		log.Errorf("Couldn't ping postgres database (%s)", err)
		panic(err)
	}

	return db, err
}

func buildConnInfo(c Pgconf) string {
	connInfo := ""
	if c.SslMode == "verify-full" {
		connInfo = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s sslrootcert=%s sslcert=%s sslkey=%s",
			c.Host, c.Port, c.User, c.Password, c.Database, c.SslMode, c.Cacert, c.ClientCert, c.ClientKey)
	} else if c.SslMode != "disable" {
		connInfo = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s sslrootcert=%s",
			c.Host, c.Port, c.User, c.Password, c.Database, c.SslMode, c.Cacert)
	} else {
		connInfo = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			c.Host, c.Port, c.User, c.Password, c.Database, c.SslMode)
	}

	return connInfo
}

// GetHeader retrieves the file header
func GetHeader(db *sql.DB, fileID int) ([]byte, error) {
	const getHeader = "SELECT header from local_ega.files WHERE id = $1"

	var hexString string
	if err := db.QueryRow(getHeader, fileID).Scan(&hexString); err != nil {
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

// MarkCompleted markes the file as "COMPLETED"
func MarkCompleted(db *sql.DB, checksum string, fileID int) (error) {
	const completed = "UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = $1, archive_file_checksum_type = 'SHA256'  WHERE id = $2;"
	result, err := db.Exec(completed, checksum, fileID)
	if err != nil {
		log.Errorf("something went wrong with the DB qurey: %s", err)
	}
	if rowsAffected, _ := result.RowsAffected(); rowsAffected == 0 {
		log.Errorln("something went wrong with the query zero rows where changed")
	}
	return err
}
