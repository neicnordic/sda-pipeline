package postgres

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"

	// Needed implicitly to enable Postgres driver
	_ "github.com/lib/pq"
)

// SQLdb is
type SQLdb struct {
	Db    *sql.DB
	Error error
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
func NewDB(c Pgconf) *SQLdb {
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

	dbs := SQLdb{Db: db, Error: err}

	return &dbs
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
