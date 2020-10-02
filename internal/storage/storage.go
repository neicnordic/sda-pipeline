// Package storage provides interface for storage areas, e.g. s3 or POSIX file system.
package storage

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	log "github.com/sirupsen/logrus"
)

// Backend defines methods to be implemented by PosixBackend and S3Backend
type Backend interface {
	GetFileSize(filePath string) (int64, error)
	NewFileReader(filePath string) (io.ReadCloser, error)
	NewFileWriter(filePath string) (io.WriteCloser, error)
}

// Conf is a wrapper for the storage config
type Conf struct {
	Type  string
	S3    S3Conf
	Posix posixConf
}

type posixBackend struct {
	FileReader io.Reader
	FileWriter io.Writer
	Location   string
}

type posixConf struct {
	Location string
}

// NewBackend initiates a storage backend
func NewBackend(config Conf) Backend {
	switch config.Type {
	case "s3":
		return newS3Backend(config.S3)
	default:
		return newPosixBackend(config.Posix)
	}
}

func newPosixBackend(config posixConf) *posixBackend {
	return &posixBackend{Location: config.Location}
}

// NewFileReader returns an io.Reader instance
func (pb *posixBackend) NewFileReader(filePath string) (io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(filepath.Clean(pb.Location), filePath))
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return file, nil
}

// NewFileWriter returns an io.Writer instance
func (pb *posixBackend) NewFileWriter(filePath string) (io.WriteCloser, error) {
	file, err := os.OpenFile(filepath.Join(filepath.Clean(pb.Location), filePath), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0640)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return file, nil
}

// GetFileSize returns the size of the file
func (pb *posixBackend) GetFileSize(filePath string) (int64, error) {
	stat, err := os.Stat(filepath.Join(filepath.Clean(pb.Location), filePath))
	if err != nil {
		log.Error(err)
		return 0, err
	}

	return stat.Size(), nil
}

type s3Backend struct {
	Client     *s3.S3
	Downloader *s3manager.Downloader
	Uploader   *s3manager.Uploader
	Bucket     string
}

// S3Conf stores information about the S3 storage backend
type S3Conf struct {
	URL       string
	Port      int
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	Chunksize int
	Cacert    string
}

func newS3Backend(config S3Conf) *s3Backend {
	s3Transport := transportConfigS3(config)
	client := http.Client{Transport: s3Transport}
	s3Session := session.Must(session.NewSession(
		&aws.Config{
			Endpoint:         aws.String(fmt.Sprintf("%s:%d", config.URL, config.Port)),
			Region:           aws.String(config.Region),
			HTTPClient:       &client,
			S3ForcePathStyle: aws.Bool(true),
			DisableSSL:       aws.Bool(strings.HasPrefix(config.URL, "http:")),
			Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		},
	))

	// Attempt to create a bucket, but we really expect an error here
	// (BucketAlreadyOwnedByYou)
	_, err := s3.New(s3Session).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(config.Bucket),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {

			if aerr.Code() != s3.ErrCodeBucketAlreadyOwnedByYou &&
				aerr.Code() != s3.ErrCodeBucketAlreadyExists {
				log.Error("Unexpected issue while creating bucket", err)
			}
		}
	}

	s3client := s3.New(s3Session)

	return &s3Backend{
		Bucket: config.Bucket,
		Uploader: s3manager.NewUploader(s3Session, func(u *s3manager.Uploader) {
			u.PartSize = int64(config.Chunksize)
			u.Concurrency = 1
			u.LeavePartsOnError = false
		}),
		Client: s3client,
		Downloader: s3manager.NewDownloaderWithClient(s3client, func(d *s3manager.Downloader) {
			d.PartSize = int64(config.Chunksize)
			d.Concurrency = 1
		})}
}

// Helper writer to be used for downloader without concurrency
type downloadWriterAt struct {
	w       io.Writer
	written int64
}

// Simple WriteAt that can only be used to channel through to a non-seekable Writer
func (dwa downloadWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// Verify offset so we get
	if offset != dwa.written {
		log.Errorf("Received write to unexpected offset for pipe")
		return 0, fmt.Errorf("Can't do out-of-order writes to pipe, adjust concurrency")
	}

	writtenNow, err := dwa.w.Write(p)

	dwa.written += int64(writtenNow)

	return writtenNow, err
}

// Helper type to give a fake Close method
type downloadReader struct {
	r io.Reader
}

// Pass-through Read method for downloadReader
func (dr downloadReader) Read(p []byte) (n int, err error) {
	return dr.r.Read(p)
}

// Fake Closer, never fails
func (dr downloadReader) Close() (err error) {
	return nil
}

// NewFileReader returns an io.ReadCloser instance that's fed from the
// object
func (sb *s3Backend) NewFileReader(filePath string) (io.ReadCloser, error) {
	_, err := sb.GetFileSize(filePath)

	// Bail out early if the object does not exist, adds one roundtrip
	// but probably still worth it
	if err != nil {
		log.Error(err)
		return nil, err
	}
	var reader io.ReadCloser
	var writer io.WriterAt

	if sb.Downloader.Concurrency != 1 {
		return nil, fmt.Errorf("Concurrency is not supported")
	}

	// No concurrency - use a pipe
	var pipeWriter io.Writer
	reader, pipeWriter = io.Pipe()
	writer = downloadWriterAt{pipeWriter, 0}

	go func() {
		_, err := sb.Downloader.Download(writer, &s3.GetObjectInput{
			Bucket: aws.String(sb.Bucket),
			Key:    aws.String(filePath),
		})

		if err != nil {
			log.Error(err)
		}

	}()

	return reader, nil
}

// NewFileWriter uploads the contents of an io.Reader to a S3 bucket
func (sb *s3Backend) NewFileWriter(filePath string) (io.WriteCloser, error) {
	reader, writer := io.Pipe()
	go func() {
		_, err := sb.Uploader.Upload(&s3manager.UploadInput{
			Body:            reader,
			Bucket:          aws.String(sb.Bucket),
			Key:             aws.String(filePath),
			ContentEncoding: aws.String("application/octet-stream"),
		})
		if err != nil {
			_ = reader.CloseWithError(err)
		}
	}()
	return writer, nil
}

// GetFileSize returns the size of a specific object
func (sb *s3Backend) GetFileSize(filePath string) (int64, error) {
	r, err := sb.Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(sb.Bucket),
		Key:    aws.String(filePath)})

	if err != nil {
		log.Errorln(err)
		return 0, err
	}

	return *r.ContentLength, nil
}

// transportConfigS3 is a helper method to setup TLS for the S3 client.
func transportConfigS3(config S3Conf) http.RoundTripper {
	cfg := new(tls.Config)

	// Enforce TLS1.2 or higher
	cfg.MinVersion = 2

	// Read system CAs
	var systemCAs, _ = x509.SystemCertPool()
	if reflect.DeepEqual(systemCAs, x509.NewCertPool()) {
		log.Debug("creating new CApool")
		systemCAs = x509.NewCertPool()
	}
	cfg.RootCAs = systemCAs

	if config.Cacert != "" {
		cacert, e := ioutil.ReadFile(config.Cacert) // #nosec this file comes from our config
		if e != nil {
			log.Fatalf("failed to append %q to RootCAs: %v", cacert, e)
		}
		if ok := cfg.RootCAs.AppendCertsFromPEM(cacert); !ok {
			log.Debug("no certs appended, using system certs only")
		}
	}

	var trConfig http.RoundTripper = &http.Transport{
		TLSClientConfig:   cfg,
		ForceAttemptHTTP2: true}

	return trConfig
}
