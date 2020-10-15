package storage

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/johannesboyne/gofakes3"
	"github.com/stretchr/testify/assert"

	log "github.com/sirupsen/logrus"
)

var testS3Conf = S3Conf{
	"http://127.0.0.1",
	9000,
	"accesskey",
	"secretkey",
	"bucket",
	"region",
	10,
	5 * 1024 * 1024,
	"../../dev_utils/certs/ca.pem"}

var testConf = Conf{"posix", testS3Conf, testPosixConf}

var posixDoesNotExist = "/this/does/not/exist"
var posixNotCreatable = posixDoesNotExist

var ts *httptest.Server

var s3DoesNotExist = "nothing such"
var s3Creatable = "somename"

var writeData = []byte(strings.Repeat("this is a test, we want to use a not too small object to test some limits", 750000))

var cleanupFilesBack [1000]string
var cleanupFiles []string = cleanupFilesBack[0:0]

var testPosixConf = posixConf{
	"/"}

func writeName() (name string, err error) {
	f, err := ioutil.TempFile("", "writablefile")

	if err != nil {
		return "", err
	}

	name = f.Name()

	// Add to cleanup
	cleanupFiles = append(cleanupFiles, name)
	return name, err
}

func doCleanup() {
	for _, name := range cleanupFiles {
		os.Remove(name)
	}

	cleanupFiles = cleanupFilesBack[0:0]
}
func TestNewBackend(t *testing.T) {

	testConf.Type = "posix"
	p := NewBackend(testConf)

	testConf.Type = "s3"
	s := NewBackend(testConf)

	assert.IsType(t, p, &posixBackend{}, "Wrong type from NewBackend with posix")
	assert.IsType(t, s, &s3Backend{}, "Wrong type from NewBackend with S3")

	// test some extra ssl handling
	testConf.S3.Cacert = "/dev/null"
	s = NewBackend(testConf)

	assert.IsType(t, s, &s3Backend{}, "Wrong type from NewBackend with S3")
}

func TestMain(m *testing.M) {

	err := setupFakeS3()

	if err != nil {
		log.Error("Setup of fake s3 failed, bailing out")
		os.Exit(1)
	}

	ret := m.Run()
	ts.Close()
	os.Exit(ret)
}
func TestPosixBackend(t *testing.T) {

	defer doCleanup()
	testConf.Type = "posix"
	backend := NewBackend(testConf)
	var buf bytes.Buffer

	assert.IsType(t, backend, &posixBackend{}, "Wrong type from NewBackend with posix")

	log.SetOutput(os.Stdout)

	writable, err := writeName()
	if err != nil {
		t.Error("could not find a writable name, bailing out from test")
		return
	}

	writer, err := backend.NewFileWriter(writable)

	assert.NotNil(t, writer, "Got a nil reader for writer from posix")
	assert.Nil(t, err, "posix NewFileWriter failed when it shouldn't")

	written, err := writer.Write(writeData)

	assert.Nil(t, err, "Failure when writing to posix writer")
	assert.Equal(t, len(writeData), written, "Did not write all writeData")
	writer.Close()

	log.SetOutput(&buf)
	writer, err = backend.NewFileWriter(posixNotCreatable)

	assert.Nil(t, writer, "Got a non-nil reader for writer from posix")
	assert.NotNil(t, err, "posix NewFileWriter worked when it shouldn't")
	assert.NotZero(t, buf.Len(), "Expected warning missing")

	log.SetOutput(os.Stdout)

	reader, err := backend.NewFileReader(writable)
	assert.Nil(t, err, "posix NewFileReader failed when it should work")
	assert.NotNil(t, reader, "Got a nil reader for posix")

	if reader == nil {
		t.Error("reader that should be usable is not, bailing out")
		return
	}

	var readBackBuffer [4096]byte
	for offset := 0; offset < len(writeData); {

		readBack, err := reader.Read(readBackBuffer[0:4096])

		assert.Equal(t, writeData[offset:offset+readBack], readBackBuffer[:readBack], "did not read back data as expected")
		assert.Nil(t, err, "unexpected error when reading back data")

		offset += readBack
	}

	size, err := backend.GetFileSize(writable)
	assert.Nil(t, err, "posix NewFileReader failed when it should work")
	assert.NotNil(t, size, "Got a nil size for posix")

	log.SetOutput(&buf)

	reader, err = backend.NewFileReader(posixDoesNotExist)
	assert.NotNil(t, err, "posix NewFileReader worked when it should not")
	assert.Nil(t, reader, "Got a non-nil reader for posix")
	assert.NotZero(t, buf.Len(), "Expected warning missing")

	buf.Reset()

	_, err = backend.GetFileSize(posixDoesNotExist) // nolint
	assert.NotNil(t, err, "posix GetFileSize worked when it should not")
	assert.NotZero(t, buf.Len(), "Expected warning missing")

	buf.Reset()

}

func setupFakeS3() (err error) {
	// fake s3

	if ts != nil {
		// Setup done already?
		return
	}

	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts = httptest.NewServer(faker.Server())

	portAt := strings.LastIndex(ts.URL, ":")

	testConf.S3.URL = ts.URL[:portAt]
	testConf.S3.Port, err = strconv.Atoi(ts.URL[portAt+1:])
	testConf.Type = "s3"

	if err != nil {
		log.Error("Unexpected error while setting up fake s3")
		return err
	}

	s3back := NewBackend(testConf).(*s3Backend)

	_, err = s3back.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(testConf.S3.Bucket)})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {

			if aerr.Code() != s3.ErrCodeBucketAlreadyOwnedByYou &&
				aerr.Code() != s3.ErrCodeBucketAlreadyExists {
				log.Error("Unexpected issue while creating bucket: ", err)
			} else {
				// Do not flag an error for this
				err = nil
			}
		}
	}

	return err
}

func TestDownloadWriterAtOutOfOrder(t *testing.T) {

	mutex := sync.Mutex{}
	cond := sync.NewCond(&mutex)
	var writer io.WriterAt

	var runFirst, runSecond, runThird sync.WaitGroup

	runFirst.Add(1)
	runSecond.Add(1)
	runThird.Add(1)

	pipeReader, pipeWriter := io.Pipe()
	writer = &downloadWriterAt{pipeWriter, 0, 58, cond}

	// Some goroutines to do delayed writes
	go func() {
		runThird.Wait()
		_, _ = writer.WriteAt([]byte("This goes first. "), 0)
	}()

	go func() {
		runSecond.Wait()
		_, _ = writer.WriteAt([]byte("This goes last. "), 42)

	}()

	go func() {
		runFirst.Wait()
		_, _ = writer.WriteAt([]byte("This goes in the middle. "), 17)
	}()

	// Let them loose in our desired order
	runFirst.Done()
	time.Sleep(15000)
	runSecond.Done()
	time.Sleep(15000)
	runThird.Done()

	// Hoping things are down when we get here

	buf := make([]byte, 4096)

	readBuf := buf
	totRead := 0
	readNow := 0
	var err error

	for err == nil && totRead < 58 {

		readNow, err = pipeReader.Read(readBuf)
		readBuf = readBuf[readNow:]
		totRead += readNow
	}

	assert.Nil(t, err, "Reading from pipe failed")
	assert.Equal(t, 58, totRead, "Not expected amount of bytes written")
	assert.Equal(t, []byte("This goes first. This goes in the middle. This goes last. "),
		buf[:totRead],
		"Out of order writes do not appear as expected")
}

func TestS3Backend(t *testing.T) {

	for conc := 0; conc < 5; conc++ {
		testConf.Type = "s3"
		testConf.S3.Concurrency = conc
		backend := NewBackend(testConf).(*s3Backend)

		var buf bytes.Buffer

		assert.IsType(t, backend, &s3Backend{}, "Wrong type from NewBackend with s3")

		writer, err := backend.NewFileWriter(s3Creatable)

		assert.NotNil(t, writer, "Got a nil reader for writer from s3")
		assert.Nil(t, err, "posix NewFileWriter failed when it shouldn't")

		written, err := writer.Write(writeData)

		assert.Nil(t, err, "Failure when writing to s3 writer")
		assert.Equal(t, len(writeData), written, "Did not write all writeData")
		writer.Close()

		// Give things some time to happen.
		time.Sleep(1e9)

		reader, err := backend.NewFileReader(s3Creatable)
		assert.Nil(t, err, "s3 NewFileReader failed when it should work")
		assert.NotNil(t, reader, "Got a nil reader for s3")

		size, err := backend.GetFileSize(s3Creatable)
		assert.Nil(t, err, "s3 GetFileSize failed when it should work")
		assert.Equal(t, int64(len(writeData)), size, "Got an incorrect file size")

		if reader == nil {
			t.Error("reader that should be usable is not, bailing out")
			return
		}

		var readBackBuffer [4096]byte
		for offset := 0; offset < len(writeData); {

			readBack, err := reader.Read(readBackBuffer[0:4096])

			assert.Equal(t, writeData[offset:offset+readBack], readBackBuffer[:readBack], "did not read back data as expected")
			assert.Nil(t, err, "unexpected error when reading back data")

			offset += readBack
		}

		if err != nil && err != io.EOF {
			assert.Nil(t, err, "unexpected error when reading back data")
		}

		buf.Reset()

		log.SetOutput(&buf)

		_, err = backend.GetFileSize(s3DoesNotExist)
		assert.NotNil(t, err, "s3 GetFileSize worked when it should not")
		assert.NotZero(t, buf.Len(), "Expected warning missing")

		buf.Reset()

		reader, err = backend.NewFileReader(s3DoesNotExist)
		assert.NotNil(t, err, "s3 NewFileReader worked when it should not")
		assert.Nil(t, reader, "Got a non-nil reader for s3")
		assert.NotZero(t, buf.Len(), "Expected warning missing")

		log.SetOutput(os.Stdout)
	}
}
