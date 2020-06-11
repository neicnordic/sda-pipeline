package storage

import (
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

// Backend defines methods to be implemented by PosixBackend and S3Backend
type Backend interface {
	ReadFile(filePath string) io.Reader
	WriteFile(filePath string, f io.Reader)
	GetSize(filepath string) int64
}

// PosixBackend encapsulates an io.Reader instance
type PosixBackend struct {
	FileReader io.Reader
	Location   string
}

// PosixConf stores information about the POSIX storage backend
type PosixConf struct {
	Location string
	Mode     int
	UID      int
	GID      int
}

// NewPosixBackend returns a PosixReader struct
func NewPosixBackend(c PosixConf) *PosixBackend {
	var reader io.Reader
	return &PosixBackend{FileReader: reader, Location: c.Location}
}

// ReadFile returns an io.Reader instance
func (pb *PosixBackend) ReadFile(filePath string) io.Reader {
	f := filepath.Join(filepath.Clean(pb.Location), filePath)
	file, err := os.Open(f)
	if err != nil {
		log.Error(err)
	}
	pb.FileReader = file

	return pb.FileReader
}

// WriteFile writes an io.Reader to a specific location
func (pb *PosixBackend) WriteFile(filePath string, f io.Reader) {
	// TODO
}

// GetSize returns the size of a specific file
func (pb *PosixBackend) GetSize(filePath string) int64 {
	// TODO
	return 0
}

// S3Backend encapsulates a S3 client instance
type S3Backend struct {
	Client   *s3.S3
	Uploader *s3manager.Uploader
	Bucket   string
}

// S3Conf stores information about the S3 storage backend
type S3Conf struct {
	URL               string
	Port              int
	AccessKey         string
	SecretKey         string
	Bucket            string
	UploadConcurrency int
	Chunksize         int
	Cacert            string
}

// NewS3Backend returns a S3Backend struct
func NewS3Backend(c S3Conf, trConf http.RoundTripper) *S3Backend {
	client := http.Client{Transport: trConf}
	session := session.Must(session.NewSession(&aws.Config{
		HTTPClient: &client}))

	return &S3Backend{
		Bucket: c.Bucket,
		Uploader: s3manager.NewUploader(session, func(u *s3manager.Uploader) {
			u.PartSize = int64(c.Chunksize)
			u.Concurrency = c.UploadConcurrency
		}),
		Client: s3.New(session)}
}

// ReadFile returns an io.Reader instance
func (sb *S3Backend) ReadFile(filePath string) io.Reader {
	r, err := sb.Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(sb.Bucket),
		Key:    aws.String(filePath)})

	if err != nil {
		log.Println(err)
	}

	return r.Body
}

// WriteFile uploads the contents of an io.Reader to a S3 bucket
func (sb *S3Backend) WriteFile(filePath string, f io.Reader) {
	result, err := sb.Uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(sb.Bucket),
		Key:    aws.String(filePath),
		Body:   f,
	})

	if err != nil {
		log.Println("failed to upload file", err)
	} else {
		log.Println("File uploaded to", result.Location)
	}
}

// GetSize returns the size of a specific object
func (sb *S3Backend) GetSize(filePath string) int64 {
	r, err := sb.Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(sb.Bucket),
		Key:    aws.String(filePath)})

	if err != nil {
		log.Println(err)
	}

	return *r.ContentLength
}
