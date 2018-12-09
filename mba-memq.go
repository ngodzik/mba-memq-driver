package mbamemqdriver

import (
	"mba"

	"github.com/ngodzik/memq"
)

// Name of the mba memq driver.
const Name string = "memq"

// MmqDriver represents the memq driver context for the mba package.
type MmqDriver struct {
	broker *memq.Broker
}

// MmqPublisher represents the memq publisher context for the mba package.
type MmqPublisher struct {
	publisher *memq.Publisher
	topic     string

	// Work TODO think about a different api.
	broker *memq.Broker
}

// MmqSubscriber represents the memq subscriber context for the mba package.
type MmqSubscriber struct {
	subscriber *memq.Subscriber
}

// New returns an memq driver instance.
func New(queueSize int) (*MmqDriver, error) {
	// Create the message broker
	broker := memq.NewMessageBroker(queueSize)

	md := MmqDriver{broker: broker}

	mba.MustInitDriver(Name, md)

	return &MmqDriver{broker: broker}, nil
}

// NewPublisher returns a new memq Publisher
func (d MmqDriver) NewPublisher(topic string) (mba.Publisher, error) {

	p := d.broker.GetPublisher(topic)

	return MmqPublisher{publisher: p, topic: topic, broker: d.broker}, nil

}

// Send sends a message to the messages queue.
func (p MmqPublisher) Send(msg interface{}) error {
	p.publisher.Send(memq.Auto, msg)

	return nil
}

// Close will be close the underlying queues.
func (p MmqPublisher) Close() {
	p.broker.ClosePublisher(p.topic)
}

// NewSubscriber returns a new rabbitMQ Publisher
func (d MmqDriver) NewSubscriber() (mba.Subscriber, error) {

	// Blocking by default TODO
	s := d.broker.NewSubscriber(true)

	return MmqSubscriber{subscriber: s}, nil
}

// SetTopic sets the function to run when a message with the dedicated topic is received on the subsbriber.
func (d MmqDriver) SetTopic(mbs mba.Subscriber, topic string, f func(msg interface{}) error) error {

	mms := mbs.(MmqSubscriber)

	d.broker.AddTopic(mms.subscriber, topic, 1, f)

	return nil
}

// Receive blocks and runs the configured function when a message arrives.
// If the returned value ok is set to false, no more messages can be received with the current configuration.
func (s MmqSubscriber) Receive() (ok bool, err error) {

	return s.subscriber.WaitAndProcessMessage()
}

// Start starts the message broker.
func (d MmqDriver) Start() {
	d.broker.Start()
}
