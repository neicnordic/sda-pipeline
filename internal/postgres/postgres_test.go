package postgres

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/DATA-DOG/go-sqlmock"
)

/*
 * Tests for database usage.
 *
 * We do not try to be smart but rather verify specific SQL being run from
 * the various functions.
 */

var testPgconf = Pgconf{"localhost",
	42,
	"user",
	"password",
	"database",
	"cacert",
	"verify-full",
	"clientcert",
	"clientkey"}

const testConnInfo = "host=localhost port=42 user=user password=password dbname=database sslmode=verify-full sslrootcert=cacert sslcert=clientcert sslkey=clientkey"

func TestBuildConnInfo(t *testing.T) {

	s := buildConnInfo(testPgconf)

	if s != testConnInfo {
		t.Error("Bad string for verify-full: " + s)
	}

	noSslConf := testPgconf
	noSslConf.SslMode = "disable"

	s = buildConnInfo(noSslConf)

	if s != "host=localhost port=42 user=user password=password dbname=database sslmode=disable" {
		t.Error("Bad string for disable: " + s)
	}
}

func TestNewDB(t *testing.T) {

	db, mock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))

	mock.ExpectPing()

	sqlOpen = func(dbName string, connInfo string) (*sql.DB, error) {
		if dbName != "postgres" {
			t.Error("Unexpected database name '" + dbName + "' while expecting 'postgres'")
			return nil, errors.New("Unexpected dbname")
		}

		if connInfo != testConnInfo {
			t.Error("Unexpected connection info '" + connInfo + "' while expecting '" + testConnInfo + "'")
			return nil, errors.New("Unexpected conninfo")
		}

		return db, nil
	}

	NewDB(testPgconf)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// Helper function for "simple" sql tests
func sqlTesterHelper(t *testing.T, f func(sqlmock.Sqlmock, *SQLdb) error) error {
	db, mock, err := sqlmock.New()

	sqlOpen = func(_ string, _ string) (*sql.DB, error) {
		return db, err
	}

	testDb, _ := NewDB(testPgconf)

	returnErr := f(mock, testDb)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	return returnErr
}

func TestMarkCompleted(t *testing.T) {
	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		r := sqlmock.NewResult(10, 1)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = \\$1, archive_file_checksum_type = 'SHA256'  WHERE id = \\$2").WithArgs("1", 10).WillReturnResult(r)

		return testDb.MarkCompleted("1", 10)
	})

	if r != nil {
		t.Errorf("MarkCompleted failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)
	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		r := sqlmock.NewResult(11, 0)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = \\$1, archive_file_checksum_type = 'SHA256'  WHERE id = \\$2").WithArgs("1", 10).WillReturnResult(r)

		return testDb.MarkCompleted("1", 10)
	})

	if r != nil {
		t.Errorf("MarkCompleted failed unexpectedly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	buf.Reset()
	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		mock.ExpectExec("UPDATE local_ega.files SET status = 'COMPLETED', archive_file_checksum = \\$1, archive_file_checksum_type = 'SHA256'  WHERE id = \\$2").
			WithArgs("1", 10).
			WillReturnError(fmt.Errorf("error for testing"))

		return testDb.MarkCompleted("1", 10)
	})

	if r == nil {
		t.Errorf("MarkCompleted did not fail when expected")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}

func TestInsertFile(t *testing.T) {
	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectQuery("INSERT INTO local_ega.main\\(submission_file_path, submission_file_extension, submission_user, status, encryption_method\\) VALUES\\(\\$1, \\$2, \\$3,'INIT', 'CRYPT4GH'\\) RETURNING id;").
			WithArgs("/tmp/file.c4gh", "c4gh", "nobody").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))

		_, err := testDb.InsertFile("/tmp/file.c4gh", "nobody")
		return err
	})
	if r != nil {
		t.Errorf("InsertFile failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)
	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		// 0 rows in response?
		mock.ExpectQuery("INSERT INTO local_ega.main\\(submission_file_path, submission_file_extension, submission_user, status, encryption_method\\) VALUES\\(\\$1, \\$2, \\$3,'INIT', 'CRYPT4GH'\\) RETURNING id;").
			WithArgs("/tmp/file.c4gh", "c4gh", "nobody").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		_, err := testDb.InsertFile("/tmp/file.c4gh", "nobody")
		return err
	})

	// Assume it should report failure for now
	if r == nil {
		t.Errorf("InsertFile returned no fileId but did not fail")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	buf.Reset()

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		// 0 rows in response?
		mock.ExpectQuery("INSERT INTO local_ega.main\\(submission_file_path, submission_file_extension, submission_user, status, encryption_method\\) VALUES\\(\\$1, \\$2, \\$3,'INIT', 'CRYPT4GH'\\) RETURNING id;").
			WithArgs("/tmp/file.c4gh", "c4gh", "nobody").
			WillReturnError(fmt.Errorf("error for testing"))

		_, err := testDb.InsertFile("/tmp/file.c4gh", "nobody")
		return err
	})

	if r == nil {
		t.Errorf("InsertFile did not fail correctly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}

func TestGetHeader(t *testing.T) {
	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		header := []byte{15, 64}
		mock.ExpectQuery("SELECT header from local_ega.files WHERE id = \\$1").
			WithArgs(42).
			WillReturnRows(sqlmock.NewRows([]string{"header"}).AddRow("0f40"))

		x, err := testDb.GetHeader(42)

		if !bytes.Equal(x, header) {
			t.Errorf("did not get expected header")
		}
		return err
	})

	if r != nil {
		t.Errorf("GetHeader failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectQuery("SELECT header from local_ega.files WHERE id = \\$1").
			WithArgs(42).
			WillReturnRows(sqlmock.NewRows([]string{"header"}).AddRow("somethingwrong"))

		_, err := testDb.GetHeader(42)

		return err
	})

	if r == nil {
		t.Errorf("GetHeader did not fail as expected")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectQuery("SELECT header from local_ega.files WHERE id = \\$1").
			WithArgs(42).
			WillReturnRows(sqlmock.NewRows([]string{"header"}))

		_, err := testDb.GetHeader(42)

		return err
	})

	if r == nil {
		t.Errorf("GetHeader did not fail as expected")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	buf.Reset()

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectQuery("SELECT header from local_ega.files WHERE id = \\$1").
			WithArgs(42).
			WillReturnError(fmt.Errorf("error for testing"))

		_, err := testDb.GetHeader(42)

		return err
	})

	if r == nil {
		t.Errorf("GetHeader did not fail as expected")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}

func TestStoreHeader(t *testing.T) {
	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		header := []byte{15, 45, 20, 40, 48}
		r := sqlmock.NewResult(10, 1)

		mock.ExpectExec("UPDATE local_ega.files SET header = \\$1 WHERE id = \\$2;").
			WithArgs("0f2d142830", 42).
			WillReturnResult(r)

		return testDb.StoreHeader(header, 42)
	})

	if r != nil {
		t.Errorf("StoreHeader failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		header := []byte{15, 45, 20, 40, 48}
		r := sqlmock.NewResult(10, 0)

		// No rows modified, should we fail here?
		mock.ExpectExec("UPDATE local_ega.files SET header = \\$1 WHERE id = \\$2;").
			WithArgs("0f2d142830", 42).
			WillReturnResult(r)

		return testDb.StoreHeader(header, 42)
	})

	if r != nil {
		t.Errorf("StoreHeader failed unexpectedly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	buf.Reset()

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {
		header := []byte{15, 45, 20, 40, 48}

		// No rows modified, should we fail here?
		mock.ExpectExec("UPDATE local_ega.files SET header = \\$1 WHERE id = \\$2;").
			WithArgs("0f2d142830", 42).
			WillReturnError(fmt.Errorf("error for testing"))

		return testDb.StoreHeader(header, 42)
	})

	if r == nil {
		t.Errorf("StoreHeader did not fail correctly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}

func TestSetArchived(t *testing.T) {

	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		file := FileInfo{"10", 1000, "/tmp/file.c4gh"}
		r := sqlmock.NewResult(10, 1)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'ARCHIVED', archive_path = \\$1, archive_filesize = \\$2, inbox_file_checksum = \\$3, inbox_file_checksum_type = 'SHA256' WHERE id = \\$4;").
			WithArgs(file.Path, file.Size, file.Checksum, 42).
			WillReturnResult(r)

		return testDb.SetArchived(file, 42)
	})

	if r != nil {
		t.Errorf("SetArchived failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)

	// How to handle 0 rows changed?
	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		file := FileInfo{"10", 1000, "/tmp/file.c4gh"}
		r := sqlmock.NewResult(10, 0)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'ARCHIVED', archive_path = \\$1, archive_filesize = \\$2, inbox_file_checksum = \\$3, inbox_file_checksum_type = 'SHA256' WHERE id = \\$4;").
			WithArgs(file.Path, file.Size, file.Checksum, 42).
			WillReturnResult(r)

		return testDb.SetArchived(file, 42)
	})

	if r != nil {
		t.Errorf("SetArchived failed unexpectedly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	buf.Reset()

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		file := FileInfo{"10", 1000, "/tmp/file.c4gh"}

		mock.ExpectExec("UPDATE local_ega.files SET status = 'ARCHIVED', archive_path = \\$1, archive_filesize = \\$2, inbox_file_checksum = \\$3, inbox_file_checksum_type = 'SHA256' WHERE id = \\$4;").
			WithArgs(file.Path, file.Size, file.Checksum, 42).
			WillReturnError(fmt.Errorf("error for testing"))

		return testDb.SetArchived(file, 42)
	})

	if r == nil {
		t.Errorf("SetArchived did not fail correctly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}

func TestMarkReady(t *testing.T) {
	r := sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		r := sqlmock.NewResult(10, 1)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'READY', stable_id = \\$1 WHERE elixir_id = \\$2 and inbox_path = \\$3 and inbox_file_checksum = \\$4 and status != 'DISABLED';").
			WithArgs("accessionId", "nobody", "/tmp/file.c4gh", "checksum").
			WillReturnResult(r)

		return testDb.MarkReady("accessionId", "nobody", "/tmp/file.c4gh", "checksum")
	})

	if r != nil {
		t.Errorf("MarkReady failed unexpectedly")
	}

	var buf bytes.Buffer
	log.SetOutput(&buf)

	// How to handle 0 rows affected?
	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		r := sqlmock.NewResult(10, 0)

		mock.ExpectExec("UPDATE local_ega.files SET status = 'READY', stable_id = \\$1 WHERE elixir_id = \\$2 and inbox_path = \\$3 and inbox_file_checksum = \\$4 and status != 'DISABLED';").
			WithArgs("accessionId", "nobody", "/tmp/file.c4gh", "checksum").
			WillReturnResult(r)

		return testDb.MarkReady("accessionId", "nobody", "/tmp/file.c4gh", "checksum")
	})

	if r != nil {
		t.Errorf("MarkReady failed unexpectedly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	r = sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectExec("UPDATE local_ega.files SET status = 'READY', stable_id = \\$1 WHERE elixir_id = \\$2 and inbox_path = \\$3 and inbox_file_checksum = \\$4 and status != 'DISABLED';").
			WithArgs("accessionId", "nobody", "/tmp/file.c4gh", "checksum").
			WillReturnError(fmt.Errorf("error for testing"))

		return testDb.MarkReady("accessionId", "nobody", "/tmp/file.c4gh", "checksum")
	})

	if r == nil {
		t.Errorf("MarkReady failed unexpectedly")
	}

	if buf.Len() == 0 {
		t.Errorf("Expected warning missing")
	}

	log.SetOutput(os.Stdout)
}
func TestMapFilesToDataset(t *testing.T) {
	sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		// Set up a few file sets with different accession ids.
		accessions := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"}

		diSet := map[string][]string{"dataset1": accessions[0:3],
			"dataset2": accessions[4:5],
			"dataset3": accessions[6:8],
			"dataset4": accessions[9:9]}

		success := sqlmock.NewResult(1, 1)

		for di, acs := range diSet {
			mock.ExpectBegin()
			for _, aId := range acs {
				r := sqlmock.NewRows([]string{"file_id"})

				fileId, _ := strconv.Atoi(aId)

				r.AddRow(fileId)

				mock.ExpectQuery("SELECT file_id FROM local_ega.archive_files WHERE stable_id = \\$1").
					WithArgs(aId).WillReturnRows(r)

				mock.ExpectExec("INSERT INTO local_ega_ebi.filedataset \\(file_id, dataset_stable_id\\) VALUES \\(\\$1, \\$2\\);").
					WithArgs(fileId, di).WillReturnResult(success)

			}
			mock.ExpectCommit()

			testDb.MapFilesToDataset(di, acs)
		}

		return nil
	})
}

func TestClose(t *testing.T) {
	sqlTesterHelper(t, func(mock sqlmock.Sqlmock, testDb *SQLdb) error {

		mock.ExpectClose()
		testDb.Close()
		return nil
	})
}
