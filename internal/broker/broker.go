// Package broker contains logic for communicating with a message broker
package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

// This is an internal helper variable to make testing easier
var logFatalf = log.Fatalf

// The AMQPChannel interface gives access to the functions provided
type AMQPChannel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

// AMQPBroker is a Broker that reads messages from an AMQP broker
type AMQPBroker struct {
	Connection *amqp.Connection
	Channel    AMQPChannel
}

// MQConf stores information about the message broker
type MQConf struct {
	Host         string
	Port         int
	User         string
	Password     string
	Vhost        string
	Queue        string
	Exchange     string
	RoutingKey   string
	RoutingError string
	Ssl          bool
	VerifyPeer   bool
	CACert       string
	ClientCert   string
	ClientKey    string
	ServerName   string
	Durable      bool
}

// NewMQ creates a new Broker that can communicate with a backend
// amqp server.
func NewMQ(config MQConf) (*AMQPBroker, error) {
	brokerURI := buildMQURI(config.Host, config.User, config.Password, config.Vhost, config.Port, config.Ssl)

	var Connection *amqp.Connection
	var Channel *amqp.Channel
	var err error

	log.Debugf("Connecting to broker with <%s>", brokerURI)
	if config.Ssl {
		var tlsConfig *tls.Config
		tlsConfig, err = TLSConfigBroker(config)
		if err != nil {
			return nil, err
		}
		Connection, err = amqp.DialTLS(brokerURI, tlsConfig)
	} else {
		Connection, err = amqp.Dial(brokerURI)
	}
	if err != nil {
		return nil, err
	}

	Channel, err = Connection.Channel()
	if err != nil {
		return nil, err
	}

	// The queues already exists so we can safely do a passive declaration
	_, err = Channel.QueueDeclarePassive(
		config.Queue, // name
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, err
	}

	return &AMQPBroker{Connection, Channel}, nil
}

// GetMessages reads messages from the queue
func GetMessages(broker *AMQPBroker, queue string) (<-chan amqp.Delivery, error) {
	ch := broker.Channel
	return ch.Consume(
		queue, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

// SendMessage sends a message to RabbitMQ
func SendMessage(broker *AMQPBroker, corrID, exchange, routingKey string, reliable bool, body []byte) error {
	if reliable {
		// Set channel
		if e := broker.Channel.Confirm(false); e != nil {
			logFatalf("channel could not be put into confirm mode: %s", e)
		}
		// Shouldn't this be setup once and for all?
		confirms := broker.Channel.NotifyPublish(make(chan amqp.Confirmation, 100))
		defer confirmOne(confirms)
	}
	err := broker.Channel.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentEncoding: "UTF-8",
			ContentType:     "application/json",
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			CorrelationId:   corrID,
			Priority:        0, // 0-9
			Body:            body,
			// a bunch of application/implementation-specific fields
		},
	)
	return err
}

// buildMQURI builds the MQ connection URI
func buildMQURI(mqHost, mqUser, mqPassword, mqVhost string, mqPort int, ssl bool) string {
	brokerURI := ""
	if ssl {
		brokerURI = fmt.Sprintf("amqps://%s:%s@%s:%d%s", mqUser, mqPassword, mqHost, mqPort, mqVhost)
	} else {
		brokerURI = fmt.Sprintf("amqp://%s:%s@%s:%d%s", mqUser, mqPassword, mqHost, mqPort, mqVhost)
	}
	return brokerURI
}

// TLSConfigBroker is a helper method to setup TLS for the message broker
func TLSConfigBroker(config MQConf) (*tls.Config, error) {
	// Read system CAs
	systemCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	tlsConfig := tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    systemCAs,
	}

	// Add CAs for broker and database
	for _, cacert := range []string{config.CACert} {
		if cacert == "" {
			continue
		}
		cacert, e := ioutil.ReadFile(cacert) // #nosec this file comes from our config
		if e != nil {
			return nil, err
		}
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM(cacert); !ok {
			log.Warnln("No certs appended, using system certs only")
		}
	}

	// If the server URI difers from the hostname in the certificate
	// we need to set the hostname to match our certificates against.
	if config.ServerName != "" {
		tlsConfig.ServerName = config.ServerName
	}
	//nolint:nestif
	if config.VerifyPeer {
		if config.ClientCert != "" && config.ClientKey != "" {
			cert, err := ioutil.ReadFile(config.ClientCert)
			if err != nil {
				return nil, err
			}
			key, err := ioutil.ReadFile(config.ClientKey)
			if err != nil {
				return nil, err
			}
			certs, err := tls.X509KeyPair(cert, key)
			if err != nil {
				return nil, err
			}
			tlsConfig.Certificates = append(tlsConfig.Certificates, certs)
		} else {
			logFatalf("No certificates supplied")
		}
	}
	return &tlsConfig, nil
}

// confirmOne accepts confirmation for a message sent with reliable.
//
// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	confirmed := <-confirms
	if !confirmed.Ack {
		log.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
	log.Debugf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
}
