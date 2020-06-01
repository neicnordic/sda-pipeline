package main

import (
	"fmt"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/postgres"
	"sda-pipeline/internal/storage"

	"github.com/spf13/viper"
)

var (
	requiredConfVars = []string{
		"broker.host", "broker.port", "broker.user", "broker.password", "broker.queue",
		"db.host", "db.port", "db.user", "db.password", "db.database",
		"c4gh.passphrase", "c4gh.filepath",
	}
)

// Config is a parent object for all the different configuration parts
type Config struct {
	Broker   broker.Mqconf
	Postgres postgres.Pgconf
	Archvie  interface{}
}

// NewConfig initializes and parses the config file and/or environment using
// the viper library.
func NewConfig() *Config {
	parseConfig()

	if viper.GetString("archive.type") == "s3" {
		s3ConfVars := []string{
			"archive.url", "archive.acesskey", "archive.secretkey", "archive.bucket",
		}
		requiredConfVars = append(requiredConfVars, s3ConfVars...)
	} else {
		posixConfVars := []string{
			"archive.location", "archive.user",
		}
		requiredConfVars = append(requiredConfVars, posixConfVars...)
	}

	for _, s := range requiredConfVars {
		if !viper.IsSet(s) {
			log.Fatalf("%s not set", s)
		}
	}

	c := &Config{}
	c.readConfig()

	return c
}

func (c *Config) readConfig() {

	// Setup broker
	b := broker.Mqconf{}

	b.Host = viper.GetString("broker.host")
	b.Port = viper.GetString("broker.port")
	b.User = viper.GetString("broker.user")
	b.Password = viper.GetString("broker.password")
	b.Queue = viper.GetString("broker.queue")
	b.ServerName = viper.GetString("broker.serverName")

	if viper.IsSet("broker.vhost") {
		if strings.HasPrefix(viper.GetString("broker.vhost"), "/") {
			b.Vhost = viper.GetString("broker.vhost")
		} else {
			b.Vhost = "/" + viper.GetString("broker.vhost")
		}
	} else {
		b.Vhost = "/"
	}

	if viper.IsSet("broker.ssl") {
		b.Ssl = viper.GetBool("broker.ssl")
	}
	if viper.IsSet("broker.verifyPeer") {
		b.VerifyPeer = viper.GetBool("broker.verifyPeer")
		// Since verifyPeer is specified, these are required.
		if !(viper.IsSet("broker.clientCert") && viper.IsSet("broker.clientKey")) {
			log.Fatalln("when broker.verifyPeer is set both broker.clientCert and broker.clientKey is needed")
		}
		b.ClientCert = viper.GetString("broker.clientCert")
		b.ClientKey = viper.GetString("broker.clientKey")
	}
	if viper.IsSet("broker.cacert") {
		b.Cacert = viper.GetString("broker.cacert")
	}

	c.Broker = b

	db := postgres.Pgconf{}

	// All these are required
	db.Host = viper.GetString("db.host")
	db.Port = viper.GetString("db.port")
	db.User = viper.GetString("db.user")
	db.Password = viper.GetString("db.password")
	db.Database = viper.GetString("db.database")
	db.SslMode = viper.GetString("db.sslmode")

	// Optional settings
	if viper.IsSet("db.sslMode") && viper.GetString("db.sslMode") == "verify-full" {
		db.SslMode = viper.GetString("db.sslMode")
		// Since verify-full is specified, these are required.
		if !(viper.IsSet("db.clientCert") && viper.IsSet("db.clientKey")) {
			panic(fmt.Errorf("when db.sslMode is set to verify-full both db.clientCert and db.clientKey are needed"))
		}
		db.ClientCert = viper.GetString("db.clientCert")
		db.ClientKey = viper.GetString("db.clientKey")
	}
	if viper.IsSet("db.cacert") {
		db.Cacert = viper.GetString("db.cacert")
	}

	c.Postgres = db

	if viper.IsSet("log.level") {
		switch strings.ToLower(viper.GetString("log.level")) {
		case "info":
			log.SetLevel(log.InfoLevel)
		case "debug":
			log.SetLevel(log.DebugLevel)
		case "warn":
			log.SetLevel(log.WarnLevel)
		case "error":
			log.SetLevel(log.ErrorLevel)
		}
		log.Printf("Setting loglevel to %s", strings.ToLower(viper.GetString("log.level")))
	}

	if viper.GetString("archive.type") == "s3" {
		s3 := storage.S3Conf{}

		// All these are required
		s3.URL = viper.GetString("archive.url")
		s3.AccessKey = viper.GetString("archive.accesskey")
		s3.SecretKey = viper.GetString("archive.secretkey")
		s3.Bucket = viper.GetString("archive.bucket")

		if viper.IsSet("archive.port") {
			s3.Port = viper.GetString("archive.port")
		}

		if viper.IsSet("archive.chunksize") {
			s3.Chunksize = viper.GetInt("archive.chunksize")
		}

		if viper.IsSet("archive.cacert") {
			s3.Port = viper.GetString("archive.cacert")
		}

		c.Archvie = s3

	} else {
		file := storage.PosixConf{}

		file.Location = viper.GetString("archive.location")
		file.User = viper.GetString("archive.user")

		if viper.IsSet("archive.mode") {
			file.Mode = viper.GetInt("archive.mode")
		} else {
			file.Mode = 2750
		}

		if viper.IsSet("archive.separator") {
			file.Separator = viper.GetString("archive.separator")
		} else {
			file.Separator = "/"
		}

		c.Archvie = file

	}

}

func parseConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigType("yaml")
	if viper.IsSet("configPath") {
		cp := viper.GetString("configPath")
		ss := strings.Split(strings.TrimLeft(cp, "/"), "/")
		viper.AddConfigPath(path.Join(ss...))
	}
	if viper.IsSet("configFile") {
		viper.SetConfigFile(viper.GetString("configFile"))
	}
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Infoln("No config file found, using ENVs only")
		} else {
			log.Fatalf("fatal error config file: %s", err)
		}
	}
}
