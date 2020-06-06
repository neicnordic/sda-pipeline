package storage

import (
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

// Backend defines methods to be implemented by PosixReader and S3Reader
type Backend interface {
	ReadFile(filePath string) io.Reader
}

// PosixReader encapsulates an io.Reader instance
type PosixReader struct {
	FileReader io.Reader
}

// PosixConf stores information about the POSIX storage backend
type PosixConf struct {
	Location string
	Mode     int
	UID      int
	GID      int
}

// NewPosixReader returns a PosixReader struct
func NewPosixReader(c PosixConf) *PosixReader {
	var reader io.Reader
	return &PosixReader{FileReader: reader}
}

// ReadFile returns an io.Reader instance
func (pr *PosixReader) ReadFile(filePath string) io.Reader {
	file, err := os.Open(filePath)
	if err != nil {
		log.Error(err)
	}
	pr.FileReader = file

	return pr.FileReader
}

// S3Reader encapsulates a S3 reader instance
type S3Reader struct {
	ObjectReader io.Reader
}

// S3Conf stores information about the S3 storage backend
type S3Conf struct {
	URL       string
	Port      int
	AccessKey string
	SecretKey string
	Bucket    string
	Chunksize int
	Cacert    string
}

// NewS3Reader returns a S3Reader struct
func NewS3Reader(c S3Conf) *S3Reader {
	return &S3Reader{}
}

// ReadFile returns an io.Reader instance
func (sr *S3Reader) ReadFile(filePath string) io.Reader {
	return sr.ObjectReader
}
