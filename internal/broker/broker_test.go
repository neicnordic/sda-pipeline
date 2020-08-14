package broker

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type mockChannel struct {
}

func (*mockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, fmt.Errorf("error")
}

func (*mockChannel) Confirm(noWait bool) error {
	return nil
}

func (*mockChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return nil
}

func (*mockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

func (*mockChannel) Close() error {
	return nil
}

func TestGetMessages(t *testing.T) {
	b := AMQPBroker{}
	c := mockChannel{}
	b.Channel = &c

	var str bytes.Buffer
	log.SetOutput(&str)

	GetMessages(&b, "queue")
	assert.NotZero(t, str.Len(), "Expected warnings were missing")
	assert.Contains(t, str.String(), "Error reading from channel")

}

func TestSendMessage(t *testing.T) {
	b := AMQPBroker{}
	c := mockChannel{}
	b.Channel = &c
	msg := []byte("Message")

	err := SendMessage(&b, "corrID1", "exchange", "routingkey", false, msg)

	if err != nil {
		log.Errorf("Error testing send message: %s", err)
	}
}

var tMqconf = Mqconf{"localhost",
	5672,
	"user",
	"password",
	"/vhost",
	"queue",
	"exchange",
	"routingkey",
	"routingError",
	false,
	false,
	"cacert",
	"clientcert",
	"clientkey",
	"servername",
	true}

func TestBuildMqURI(t *testing.T) {
	amqps := buildMqURI("localhost", "user", "pass", "/vhost", 5555, true)
	assert.Equal(t, "amqps://user:pass@localhost:5555/vhost", amqps)
	amqp := buildMqURI("localhost", "user", "pass", "/vhost", 5555, false)
	assert.Equal(t, "amqp://user:pass@localhost:5555/vhost", amqp)
}

func CatchNewMQPanic() (err error) {
	// Recover if NewMQ panics
	// Allow both panic and error return here, so use a custom function rather
	// than assert.Panics

	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("Caught panic")
		}
	}()

	_ = NewMQ(tMqconf)

	return err
}

// Test connection refused as there is no running
// broker
func TestNewMQConn_Error(t *testing.T) {

	brokerURI := buildMqURI(tMqconf.Host, tMqconf.User, tMqconf.Password, tMqconf.Vhost, tMqconf.Port, tMqconf.Ssl)

	expectedMsg := "dial tcp [::1]:5672: connect: connection refused"

	_, err := amqp.Dial(brokerURI)

	assert.EqualError(t, err, expectedMsg)

	newErr := CatchNewMQPanic()

	if newErr == nil {
		t.Errorf("New MQ did not report error when it should.")
	}
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
