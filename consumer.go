package kueue

import (
	"github.com/bitly/go-nsq"
)

// Consumer is a wrapper for go-nsq Consumer.
type Consumer struct {
	Topic       string
	Channel     string
	Concurrency int
	Config      *nsq.Config
	handler     *handler
	consumer    *nsq.Consumer
}

// Create a new Consumer with a given topic, channel, concurrency and JobGenerator.
func NewConsumer(topic, channel string, concurrency int, jobGenerator JobGenerator) (c *Consumer) {
	config := nsq.NewConfig()
	config.Set("max_in_flight", concurrency)

	return &Consumer{
		Topic:       topic,
		Channel:     channel,
		Concurrency: concurrency,
		Config:      config,
		handler:     &handler{topic, channel, jobGenerator},
	}
}

// Create the go-nsq Consumer, adds handler and connects to nsq.
func (c *Consumer) ConnectToNSQLookupd(lookupdAddr string) (err error) {
	if c.consumer, err = nsq.NewConsumer(c.Topic, c.Channel, c.Config); err != nil {
		return
	}

	c.consumer.AddConcurrentHandlers(c.handler, c.Concurrency)

	return c.consumer.ConnectToNSQLookupd(lookupdAddr)
}
