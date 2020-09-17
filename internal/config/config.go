// Package config manages configuration
package config

import (
	"fmt"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"

	"sda-pipeline/internal/broker"
	"sda-pipeline/internal/database"
	"sda-pipeline/internal/storage"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const POSIX = "posix"
const S3 = "s3"

var (
	// requiredConfVars determines what needs to be provided as a minimum
	requiredConfVars = []string{
		"broker.host", "broker.port", "broker.user", "broker.password", "broker.queue", "broker.routingkey",
		"db.host", "db.port", "db.user", "db.password", "db.database",
	}
)

// Config is a parent object for all the different configuration parts
type Config struct {
	Archive  storage.Conf
	Broker   broker.MQConf
	Crypt4gh Crypt4gh
	Inbox    storage.Conf
	Database database.DBConf
}

// Crypt4gh holds c4gh related config info
type Crypt4gh struct {
	KeyPath    string
	Passphrase string
}

// NewConfig initializes and parses the config file and/or environment using
// the viper library.
func NewConfig(app string) (*Config, error) {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigType("yaml")
	if viper.IsSet("configPath") {
		configPath := viper.GetString("configPath")
		splitPath := strings.Split(strings.TrimLeft(configPath, "/"), "/")
		viper.AddConfigPath(path.Join(splitPath...))
	}

	if viper.IsSet("configFile") {
		viper.SetConfigFile(viper.GetString("configFile"))
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Infoln("No config file found, using ENVs only")
		} else {
			return nil, err
		}
	}

	if viper.GetString("archive.type") == S3 {
		requiredConfVars = append(requiredConfVars, []string{"archive.url", "archive.accesskey", "archive.secretkey", "archive.bucket"}...)
	} else if viper.GetString("archive.type") == POSIX {
		requiredConfVars = append(requiredConfVars, []string{"archive.location"}...)
	}

	if viper.GetString("inbox.type") == S3 {
		requiredConfVars = append(requiredConfVars, []string{"inbox.url", "inbox.accesskey", "inbox.secretkey", "inbox.bucket"}...)
	} else if viper.GetString("inbox.type") == POSIX {
		requiredConfVars = append(requiredConfVars, []string{"inbox.location"}...)
	}

	for _, s := range requiredConfVars {
		if !viper.IsSet(s) {
			return nil, fmt.Errorf("%s not set", s)
		}
	}

	if viper.IsSet("log.level") {
		stringLevel := viper.GetString("log.level")
		intLevel, err := log.ParseLevel(stringLevel)
		if err != nil {
			log.Printf("Log level '%s' not supported, setting to 'trace'", stringLevel)
			intLevel = log.TraceLevel
		}
		log.SetLevel(intLevel)
		log.Printf("Setting log level to '%s'", stringLevel)
	}

	c := &Config{}
	err := c.configBroker()
	if err != nil {
		return nil, err
	}
	switch app {
	case "ingest":
		c.configInbox()
		c.configArchive()
		c.configCrypt4gh()
		err = c.configDatabase()
		if err != nil {
			return nil, err
		}
		return c, nil
	case "intercept":
		return c, nil
	case "verify":
		c.configArchive()
		c.configCrypt4gh()
		err = c.configDatabase()
		if err != nil {
			return nil, err
		}
		return c, nil
	case "finalize":
		err = c.configDatabase()
		if err != nil {
			return nil, err
		}
		return c, nil
	case "mapper":
		err = c.configDatabase()
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	return nil, fmt.Errorf("application '%s' doesn't exist", app)
}

// configS3Storage populates and returns a S3Conf from the
// configuration
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

// configArchive provides configuration for the archive storage
func (c *Config) configArchive() {
	if viper.GetString("archive.type") == S3 {
		c.Archive.Type = S3
		c.Archive.S3 = configS3Storage("archive")
	} else {
		c.Archive.Type = POSIX
		c.Archive.Posix.Location = viper.GetString("archive.location")
	}
}

// configInbox provides configuration for the inbox storage
func (c *Config) configInbox() {
	if viper.GetString("inbox.type") == S3 {
		c.Inbox.Type = S3
		c.Inbox.S3 = configS3Storage("inbox")
	} else {
		c.Inbox.Type = POSIX
		c.Inbox.Posix.Location = viper.GetString("inbox.location")
	}
}

// configBroker provides configuration for the message broker
func (c *Config) configBroker() error {
	// Setup broker
	broker := broker.MQConf{}

	broker.Host = viper.GetString("broker.host")
	broker.Port = viper.GetInt("broker.port")
	broker.User = viper.GetString("broker.user")
	broker.Password = viper.GetString("broker.password")
	broker.RoutingKey = viper.GetString("broker.routingkey")
	broker.Queue = viper.GetString("broker.queue")
	broker.ServerName = viper.GetString("broker.serverName")

	if viper.IsSet("broker.durable") {
		broker.Durable = viper.GetBool("broker.durable")
	}
	if viper.IsSet("broker.routingerror") {
		broker.RoutingError = viper.GetString("broker.routingerror")
	}
	if viper.IsSet("broker.vhost") {
		if strings.HasPrefix(viper.GetString("broker.vhost"), "/") {
			broker.Vhost = viper.GetString("broker.vhost")
		} else {
			broker.Vhost = "/" + viper.GetString("broker.vhost")
		}
	} else {
		broker.Vhost = "/"
	}

	if viper.IsSet("broker.ssl") {
		broker.Ssl = viper.GetBool("broker.ssl")
	}
	if viper.IsSet("broker.verifyPeer") {
		broker.VerifyPeer = viper.GetBool("broker.verifyPeer")
		if broker.VerifyPeer {
			// Since verifyPeer is specified, these are required.
			if !(viper.IsSet("broker.clientCert") && viper.IsSet("broker.clientKey")) {
				return errors.New("when broker.verifyPeer is set both broker.clientCert and broker.clientKey is needed")
			}
			broker.ClientCert = viper.GetString("broker.clientCert")
			broker.ClientKey = viper.GetString("broker.clientKey")
		}
	}
	if viper.IsSet("broker.cacert") {
		broker.CACert = viper.GetString("broker.cacert")
	}

	c.Broker = broker

	return nil
}

// configDatabase provides configuration for the database
func (c *Config) configDatabase() error {
	db := database.DBConf{}

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
			return errors.New("when db.sslMode is set to verify-full both db.clientCert and db.clientKey are needed")
		}
	}
	if viper.IsSet("db.clientKey") {
		db.ClientKey = viper.GetString("db.clientKey")
	}
	if viper.IsSet("db.clientCert") {
		db.ClientCert = viper.GetString("db.clientCert")
	}
	if viper.IsSet("db.cacert") {
		db.CACert = viper.GetString("db.cacert")
	}

	c.Database = db
	return nil
}

// configCrypt4gh provides configuration for c4gh operations
func (c *Config) configCrypt4gh() {
	c.Crypt4gh.KeyPath = viper.GetString("c4gh.filepath")
	c.Crypt4gh.Passphrase = viper.GetString("c4gh.passphrase")
}
