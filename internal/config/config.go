package config

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
		"broker.host", "broker.port", "broker.user", "broker.password", "broker.queue", "broker.routingkey",
		"db.host", "db.port", "db.user", "db.password", "db.database",
	}
)

// Config is a parent object for all the different configuration parts
type Config struct {
	Archive  storage.Conf
	Broker   broker.Mqconf
	Crypt4gh Crypt4gh
	Inbox    storage.Conf
	Postgres postgres.Pgconf
}

// Crypt4gh holds c4gh related config info
type Crypt4gh struct {
	KeyPath    string
	Passphrase string
}

// New initializes and parses the config file and/or environment using
// the viper library.
func New(app string) *Config {
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

	if viper.GetString("archive.type") == "s3" {
		s3ConfVars := []string{
			"archive.url", "archive.accesskey", "archive.secretkey", "archive.bucket",
		}
		requiredConfVars = append(requiredConfVars, s3ConfVars...)
	} else {
		posixConfVars := []string{
			"archive.location",
		}
		requiredConfVars = append(requiredConfVars, posixConfVars...)
	}

	if viper.GetString("inbox.type") == "s3" {
		s3ConfVars := []string{
			"inbox.url", "inbox.accesskey", "inbox.secretkey", "inbox.bucket",
		}
		requiredConfVars = append(requiredConfVars, s3ConfVars...)
	} else {
		posixConfVars := []string{
			"inbox.location",
		}
		requiredConfVars = append(requiredConfVars, posixConfVars...)
	}

	for _, s := range requiredConfVars {
		if !viper.IsSet(s) {
			log.Fatalf("%s not set", s)
		}
	}

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

	c := &Config{}

	switch app {
	case "finalize":
		c.configBroker()
		c.configDatabase()
	case "ingest":
		c.configArchive()
		c.configBroker()
		c.configCrypt4gh()
		c.configDatabase()
		c.configInbox()
	case "mapper":
		c.configBroker()
		c.configDatabase()
	case "verify":
		c.configArchive()
		c.configBroker()
		c.configCrypt4gh()
		c.configDatabase()
	}

	return c
}

func configS3Storage(prefix string) storage.S3Conf {

	s3 := storage.S3Conf{}
	// All these are required
	s3.URL = viper.GetString(prefix + ".url")
	s3.AccessKey = viper.GetString(prefix + ".accesskey")
	s3.SecretKey = viper.GetString(prefix + ".secretkey")
	s3.Bucket = viper.GetString(prefix + ".bucket")

	// Defaults (move to viper?)

	s3.Port = 443
	s3.Region = "us-east-1"

	if viper.IsSet(prefix + ".port") {
		s3.Port = viper.GetInt(prefix + ".port")
	}

	if viper.IsSet(prefix + ".region") {
		s3.Region = viper.GetString(prefix + ".region")
	}

	if viper.IsSet(prefix + ".chunksize") {
		s3.Chunksize = viper.GetInt(prefix+".chunksize") * 1024 * 1024
	}

	if viper.IsSet(prefix + ".cacert") {
		s3.Cacert = viper.GetString(prefix + ".cacert")
	}

	return s3
}

func (c *Config) configArchive() {
	if viper.GetString("archive.type") == "s3" {
		c.Archive.Type = "s3"
		c.Archive.S3 = configS3Storage("archive")
	} else {
		c.Archive.Type = "posix"
		c.Archive.Posix.Location = viper.GetString("archive.location")
	}
}

func (c *Config) configInbox() {

	if viper.GetString("inbox.type") == "s3" {
		c.Inbox.Type = "s3"
		c.Inbox.S3 = configS3Storage("inbox")
	} else {
		c.Inbox.Type = "Posix"
		c.Inbox.Posix.Location = viper.GetString("inbox.location")
	}
}

func (c *Config) configBroker() {
	// Setup broker
	b := broker.Mqconf{}

	b.Host = viper.GetString("broker.host")
	b.Port = viper.GetInt("broker.port")
	b.User = viper.GetString("broker.user")
	b.Password = viper.GetString("broker.password")
	b.RoutingKey = viper.GetString("broker.routingkey")
	b.Queue = viper.GetString("broker.queue")
	b.ServerName = viper.GetString("broker.serverName")

	if viper.IsSet("broker.durable") {
		b.Durable = viper.GetBool("broker.durable")
	}
	if viper.IsSet("broker.routingerror") {
		b.RoutingError = viper.GetString("broker.routingerror")
	}
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
}

func (c *Config) configDatabase() {
	db := postgres.Pgconf{}

	// All these are required
	db.Host = viper.GetString("db.host")
	db.Port = viper.GetInt("db.port")
	db.User = viper.GetString("db.user")
	db.Password = viper.GetString("db.password")
	db.Database = viper.GetString("db.database")
	db.SslMode = viper.GetString("db.sslmode")

	// Optional settings
	if db.SslMode == "verify-full" {
		// Since verify-full is specified, these are required.
		if !(viper.IsSet("db.clientCert") && viper.IsSet("db.clientKey")) {
			panic(fmt.Errorf("when db.sslMode is set to verify-full both db.clientCert and db.clientKey are needed"))
		}
	}
	if viper.IsSet("db.clientKey") {
	   	db.ClientKey = viper.GetString("db.clientKey")
	}
	if viper.IsSet("db.clientCert") {
	   	db.ClientCert = viper.GetString("db.clientCert")
	}
	if viper.IsSet("db.cacert") {
		db.Cacert = viper.GetString("db.cacert")
	}

	c.Postgres = db
}

func (c *Config) configCrypt4gh() {
	c.Crypt4gh.KeyPath = viper.GetString("c4gh.filepath")
	c.Crypt4gh.Passphrase = viper.GetString("c4gh.passphrase")
}
