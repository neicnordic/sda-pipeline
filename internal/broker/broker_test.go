package broker

import (
	"fmt"
	"testing"

	"github.com/streadway/amqp"
)

type myChannel struct {
}

func (*myChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	fmt.Println("this is mychannel")
	return nil, nil
}

func (*myChannel) Confirm(noWait bool) error {
	return nil
}

func (*myChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	return nil
}

func (*myChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return nil
}

func TestDialer(t *testing.T) {
	b := AMQPBroker{}

	c := myChannel{}

	b.Channel = &c
	GetMessages(&b, "hej")
}
