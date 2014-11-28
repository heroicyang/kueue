package kueue

import (
	"encoding/json"
	"time"

	"github.com/bitly/go-nsq"
)

// An internal struct to implement go-nsq Handler interface.
type handler struct {
	topic        string
	channel      string
	jobGenerator JobGenerator
}

// An internal struct to hold message body and delay time.
type message struct {
	Body []byte
	At   time.Time
}

// Implements the go-nsq Handler interface.
func (h *handler) HandleMessage(nsqMessage *nsq.Message) (err error) {
	msg := message{}
	json.Unmarshal(nsqMessage.Body, &msg)

	enqueueAfter := msg.At.Sub(time.Now())
	delayed := enqueueAfter > 0
	payload := &Payload{msg.Body, delayed, nsqMessage}

	job, err := h.jobGenerator(payload)
	if err != nil {
		logger.Error("topic: %s, unable to get handler for message %v: %s", h.topic, nsqMessage.ID, err)
		return
	}

	if delayed {
		nsqMessage.Requeue(enqueueAfter)
		logger.Info("topic: %s requeued after %d", h.topic, enqueueAfter)
	} else {
		logger.Info("topic: %s starting...", h.topic)

		if err = job.Perform(); err != nil {
			logger.Error("topic: %s error: %s", h.topic, err)
		} else {
			logger.Info("topic: %s completed", h.topic)
		}
	}

	return
}
