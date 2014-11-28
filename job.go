package kueue

import (
	"github.com/bitly/go-nsq"
)

// JobGenerator is a convenience type that processes the payload, and returns a stateful Job and error.
type JobGenerator func(*Payload) (Job, error)

// Job is the message processing interface.
type Job interface {
	Perform() error
}

// Payload holds the message body and original go-nsq Message.
type Payload struct {
	Body    []byte
	Delayed bool
	Message *nsq.Message
}
