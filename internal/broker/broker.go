package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"reflect"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

type AMQPchannel interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// AMQPBroker is a Broker that reads messages from a local AMQP broker
type AMQPBroker struct {
	Connection *amqp.Connection
	Channel    AMQPchannel
}

// Mqconf stores information about the message broker
type Mqconf struct {
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
	Cacert       string
	ClientCert   string
	ClientKey    string
	ServerName   string
	Durable      bool
}

// New creates a new Broker that can communicate with a backend
// amqp server.
func New(c Mqconf) *AMQPBroker {
	brokerURI := buildMqURI(c.Host, c.User, c.Password, c.Vhost, c.Port, c.Ssl)

	var Connection *amqp.Connection
	var Channel *amqp.Channel
	var err error

	log.Debugf("Connecting to broker with <%s>", brokerURI)
	if c.Ssl {
		tlsConfig := TLSConfigBroker(c)
		Connection, err = amqp.DialTLS(brokerURI, tlsConfig)
	} else {
		Connection, err = amqp.Dial(brokerURI)
	}
	if err != nil {
		log.Errorf("Broker Connection error: %s", err)
	}

	Channel, err = Connection.Channel()
	if err != nil {
		log.Errorf("Broker channel error: %s", err)
	}

	// The queuse already exists so we can safely do a passive declaration
	_, err = Channel.QueueDeclarePassive(
		c.Queue, // name
		true,    // durable
		false,   // auto-deleted
		false,   // internal
		false,   // noWait
		nil,     // arguments
	)
	if err != nil {
		log.Fatalf("Queue Declare: %s", err)
	}

	return &AMQPBroker{Connection, Channel}
}

// GetMessages reads messages from the queue
func GetMessages(b *AMQPBroker, queue string) <-chan amqp.Delivery {
	ch := b.Channel
	msgs, err := ch.Consume(
		queue, // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Errorf("Error reading from channel: %s", err)
	}

	return msgs
}

// SendMessage sends message to RabbitMQ if the upload is finished
func SendMessage(b *AMQPBroker, corrID, exchange, routingKey string, reliable bool, body []byte) error {
	if reliable {
		// Set channel
		if e := b.Channel.Confirm(false); e != nil {
			log.Fatalf("channel could not be put into confirm mode: %s", e)
		}
		// Shouldn't this be setup once and for all?
		confirms := b.Channel.NotifyPublish(make(chan amqp.Confirmation, 100))
		defer confirmOne(confirms)
	}
	err := b.Channel.Publish(
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

// buildMqURI builds the MQ URI
func buildMqURI(mqHost, mqUser, mqPassword, mqVhost string, mqPort int, ssl bool) string {
	brokerURI := ""
	if ssl {
		brokerURI = fmt.Sprintf("amqps://%s:%s@%s:%d%s", mqUser, mqPassword, mqHost, mqPort, mqVhost)
	} else {
		brokerURI = fmt.Sprintf("amqp://%s:%s@%s:%d%s", mqUser, mqPassword, mqHost, mqPort, mqVhost)
	}
	return brokerURI
}

// TLSConfigBroker is a helper method to setup TLS for the message broker
func TLSConfigBroker(b Mqconf) *tls.Config {
	cfg := new(tls.Config)

	// Enforce TLS1.2 or higher
	cfg.MinVersion = 2

	// Read system CAs
	var systemCAs, _ = x509.SystemCertPool()
	if reflect.DeepEqual(systemCAs, x509.NewCertPool()) {
		fmt.Println("creating new CApool")
		systemCAs = x509.NewCertPool()
	}
	cfg.RootCAs = systemCAs

	// Add CAs for broker and db
	for _, cacert := range []string{b.Cacert} {
		if cacert == "" {
			continue
		}

		cacert, e := ioutil.ReadFile(cacert) // #nosec this file comes from our configuration
		if e != nil {
			log.Fatalf("Failed to append %q to RootCAs: %v", cacert, e)
		}
		if ok := cfg.RootCAs.AppendCertsFromPEM(cacert); !ok {
			log.Errorln("No certs appended, using system certs only")
		}
	}

	// If the server URI difers from the hostname in the certificate
	// we need to set the hostname to match our certificates against.
	if b.ServerName != "" {
		cfg.ServerName = b.ServerName
	}
	//nolint:nestif
	if b.VerifyPeer {
		if b.ClientCert != "" && b.ClientKey != "" {
			cert, e := ioutil.ReadFile(b.ClientCert)
			if e != nil {
				log.Fatalf("Failed to append %q to RootCAs: %v", b.ClientKey, e)
			}
			key, e := ioutil.ReadFile(b.ClientKey)
			if e != nil {
				log.Fatalf("Failed to append %q to RootCAs: %v", b.ClientKey, e)
			}
			if certs, e := tls.X509KeyPair(cert, key); e == nil {
				cfg.Certificates = append(cfg.Certificates, certs)
			}
		} else {
			log.Fatalf("No certificates supplied")
		}
	}
	return cfg
}

// // One would typically keep a channel of publishings, a sequence number, and a
// // set of unacknowledged sequence numbers and loop until the publishing channel
// // is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	confirmed := <-confirms
	if !confirmed.Ack {
		log.Errorf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
	log.Debugf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
}
