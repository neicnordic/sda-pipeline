package broker

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const doesNotExist = "/does/not/exist"
const readableFile = "broker.go"

func TestMain(m *testing.M) {
	logFatalf = testLogFatalf
	code := m.Run()

	os.Exit(code)
}

func testLogFatalf(f string, args ...interface{}) {
	s := fmt.Sprintf(f, args...)
	panic(s)
}

type mockChannel struct {
	failConfirm    bool
	confirmChannel chan amqp.Confirmation
}

func (c *mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, fmt.Errorf("error")
}

func (c *mockChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{}, fmt.Errorf("error")
}

func (c *mockChannel) Confirm(noWait bool) error {
	if c.failConfirm {
		return fmt.Errorf("failing for testing")
	}

	return nil
}

func (c *mockChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	c.confirmChannel = confirm
	return confirm
}

func (c *mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	go func() {
		time.Sleep(10000)
		c.confirmChannel <- amqp.Confirmation{}
	}()

	return nil
}

func (*mockChannel) Close() error {
	return nil
}

func TestGetMessages_Error(t *testing.T) {
	b := AMQPBroker{}
	c := mockChannel{}
	b.Channel = &c

	var str bytes.Buffer
	log.SetOutput(&str)

	_, err := GetMessages(&b, "queue")
	assert.Error(t, err, "Must be an error")
}

func CatchSendMessage(b *AMQPBroker, corrID, exchange, routingkey string, reliable bool, body []byte) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("Caught panic")
		}
	}()
	err = SendMessage(b, corrID, exchange, routingkey, reliable, body)

	return err
}

func TestSendMessage(t *testing.T) {

	b := AMQPBroker{}
	c := mockChannel{}

	var err error
	c.failConfirm = true
	b.Channel = &c
	msg := []byte("Message")

	// Aborts run for some not yet understood reason

	err = CatchSendMessage(&b, "corrID1", "exchange", "routingkey", true, msg)
	assert.NotNil(t, err, "Unexpected non-error from SendMessage (reliable)")

	c.failConfirm = false

	err = SendMessage(&b, "corrID1", "exchange", "routingkey", false, msg)
	assert.Nil(t, err, "Unexpected error from SendMessage (not reliable)")

	err = SendMessage(&b, "corrID1", "exchange", "routingkey", true, msg)
	assert.Nil(t, err, "Unexpected error from SendMessage (reliable)")

}

var tMqconf = MQConf{"127.0.0.1",
	6565,
	"user",
	"password",
	"/vhost",
	"queue",
	"exchange",
	"routingkey",
	"routingError",
	true,
	true,
	"../../dev_utils/certs/ca.pem",
	"../../dev_utils/certs/client.pem",
	"../../dev_utils/certs/client-key.pem",
	"servername",
	true}

func TestBuildMqURI(t *testing.T) {
	amqps := buildMQURI("localhost", "user", "pass", "/vhost", 5555, true)
	assert.Equal(t, "amqps://user:pass@localhost:5555/vhost", amqps)
	amqp := buildMQURI("localhost", "user", "pass", "/vhost", 5555, false)
	assert.Equal(t, "amqp://user:pass@localhost:5555/vhost", amqp)
}

func TestNewMQ(t *testing.T) {
	noSSLPort := 5555 // + int(rand.Float64()*32768)
	sslPort := noSSLPort + 1

	s := startServer(t, noSSLPort)

	noSslConf := tMqconf
	noSslConf.Ssl = false
	noSslConf.VerifyPeer = false
	noSslConf.Port = noSSLPort

	go handleOneConnection(s.Sessions, false, false)
	b, _ := NewMQ(noSslConf)

	assert.NotNil(t, b, "NewMQ without ssl did not return a broker")

	// Fail the queuedeclarepassive
	go handleOneConnection(s.Sessions, false, true)
	errret := CatchNewMQPanic(t, noSslConf)
	assert.NotNil(t, errret, "NewMQ did fail as expected")

	// Fail the channel
	go handleOneConnection(s.Sessions, true, true)
	errret = CatchNewMQPanic(t, noSslConf)
	assert.NotNil(t, errret, "NewMQ did fail as expected")

	s.Close()

	sslConf := tMqconf
	sslConf.Ssl = true
	sslConf.VerifyPeer = false
	sslConf.Port = sslPort
	sslConf.ServerName = sslConf.Host

	serverTlsConfig := tlsServerConfig()
	serverTlsConfig.ClientAuth = tls.NoClientCert

	ss := startTLSServer(t, sslPort, serverTlsConfig)

	go handleOneConnection(ss.Sessions, false, false)

	b, _ = NewMQ(sslConf)
	assert.NotNil(t, b, "NewMQ with ssl did not return a broker")

	ss.Close()

}

func CatchNewMQPanic(t *testing.T, conf MQConf) (err error) {
	// Recover if NewMQ panics
	// Allow both panic and error return here, so use a custom function rather
	// than assert.Panics

	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("Caught panic")
		}
	}()

	b, _ := NewMQ(conf)

	if b == nil {
		return fmt.Errorf("NewMQ did not return a broker")
	}

	return nil
}

// Test connection refused as there is no running
// broker
func TestNewMQConn_Error(t *testing.T) {

	brokerURI := buildMQURI(tMqconf.Host, tMqconf.User, tMqconf.Password, tMqconf.Vhost, tMqconf.Port, tMqconf.Ssl)

	expectedMsg := "dial tcp 127.0.0.1:6565: connect: connection refused"

	_, err := amqp.Dial(brokerURI)

	assert.EqualError(t, err, expectedMsg)

	noSslConf := tMqconf
	noSslConf.Ssl = false
	noSslConf.VerifyPeer = false
	noSslConf.Port = 42

	newErr := CatchNewMQPanic(t, noSslConf)

	if newErr == nil {
		t.Errorf("New MQ did not report error when it should.")
	}
}

func CatchTLSConfigBrokerPanic(b MQConf) (cfg *tls.Config, err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("Caught panic")
		}
	}()

	cfg, _ = TLSConfigBroker(b)

	return cfg, nil
}

func TestTLSConfigBroker(t *testing.T) {
	tlsConfig, err := TLSConfigBroker(tMqconf)
	assert.NoError(t, err, "Unexpected error")
	assert.NotZero(t, tlsConfig.Certificates, "Expected warnings were missing")
	assert.EqualValues(t, tlsConfig.ServerName, "servername")

	noSslConf := tMqconf
	noSslConf.Ssl = false
	noSslConf.VerifyPeer = false
	noSslConf.CACert = ""
	noSslConf.ClientCert = ""
	noSslConf.ClientKey = ""

	notls, err := TLSConfigBroker(noSslConf)
	assert.NoError(t, err, "Unexpected error")
	assert.Zero(t, notls.Certificates, "Expected warnings were missing")

	sslConf := noSslConf
	sslConf.CACert = doesNotExist

	sslConf.CACert = readableFile

	_, _ = CatchTLSConfigBrokerPanic(sslConf)
	// Should we fail here?
	//	assert.NotNil(t, err, "Expected failure was missing")

	sslConf.VerifyPeer = true
	sslConf.CACert = ""
	sslConf.ClientKey = doesNotExist
	sslConf.ClientCert = doesNotExist

	_, _ = CatchTLSConfigBrokerPanic(sslConf)
	// Should we fail here?
	//	assert.NotNil(t, err, "Expected failure was missing")

	sslConf.VerifyPeer = true
	sslConf.CACert = ""
	sslConf.ClientKey = doesNotExist
	sslConf.ClientCert = readableFile

	_, _ = CatchTLSConfigBrokerPanic(sslConf)
	// Should we fail here?
	//	assert.NotNil(t, err, "Expected failure was missing")

	sslConf.CACert = ""
	sslConf.ClientKey = ""
	sslConf.ClientCert = ""

	_, _ = CatchTLSConfigBrokerPanic(sslConf)
	// Should we fail here?
	//	assert.NotNil(t, err, "Expected failure was missing")

}

func TestConfirmOne(t *testing.T) {

	var str bytes.Buffer
	log.SetOutput(&str)

	var wg sync.WaitGroup
	wg.Add(1)
	c := make(chan amqp.Confirmation)
	go func(c <-chan amqp.Confirmation) {
		confirmOne(c)
		assert.NotZero(t, str.Len(), "Expected warnings were missing")
		assert.Contains(t, str.String(), "failed delivery of delivery tag")
		wg.Done()
	}(c)
	c <- amqp.Confirmation{}

	wg.Wait()
}
