package kueue

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-nsq"
)

var producerPool []*nsq.Producer
var producerPoolSize int

// Publish a message to the specified topic with a given delay.
func Publish(topic string, delay time.Duration, v interface{}) (err error) {
	var buf []byte

	if buf, err = json.Marshal(v); err != nil {
		return
	}

	msg := message{
		Body: buf,
		At:   time.Now().Add(delay),
	}

	buf, _ = json.Marshal(msg)
	return getProducer().Publish(topic, buf)
}

// Publish multiple messages to the specified topic with a given delay.
func MultiPublish(topic string, delay time.Duration, vs ...interface{}) (err error) {
	body := make([][]byte, 0)

	for _, v := range vs {
		var buf []byte
		if buf, err = json.Marshal(v); err != nil {
			return
		}

		msg := message{
			Body: buf,
			At:   time.Now().Add(delay),
		}

		buf, _ = json.Marshal(msg)
		body = append(body, buf)
	}

	return getProducer().MultiPublish(topic, body)
}

// Setup producers with the given nsqdAddr and poolSize.
func SetupProducers(nsqdAddr string, poolSize int) (err error) {
	hostPort := strings.Split(nsqdAddr, ":")
	nsqdHostname := hostPort[0]
	nsqdPort, err := strconv.Atoi(hostPort[1])
	if err != nil {
		nsqdPort = 5000
	}

	producerPool = make([]*nsq.Producer, 0)
	producerPoolSize = 1

	if poolSize > 0 {
		producerPoolSize = poolSize
	}

	for i := 0; i < producerPoolSize; i++ {
		addr := nsqdHostname + ":" + strconv.Itoa(nsqdPort)
		p, err := nsq.NewProducer(addr, nsq.NewConfig())

		if err != nil {
			return err
		}

		producerPool = append(producerPool, p)
		nsqdPort += 2
	}

	return
}

func getProducer() (p *nsq.Producer) {
	return producerPool[rand.Intn(producerPoolSize)]
}
