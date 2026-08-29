package outbox

import "context"

// Header is a Kafka record header.
type Header struct {
	Key   string
	Value string
}

// Message is the broker-neutral record produced by the worker.
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers []Header
}

// Publisher sends a message and returns only after the broker acknowledges it.
type Publisher interface {
	Publish(context.Context, Message) error
}
