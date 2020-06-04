package storage

import (
	"io"
	"os"
	log "github.com/sirupsen/logrus"
)

// Conf stores information about the storage backend
type Conf struct {
	Type string
	// S3
	URL       string
	Port      int
	AccessKey string
	SecretKey string
	Bucket    string
	Chunksize int
	Cacert    string
	// posix
	Location string
	Mode     int
	UID      int
	GID      int
}
