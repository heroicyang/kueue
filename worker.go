package kueue

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/go-log"
)

var logger = log.Log.New("kueue")

// Worker start and stop multiple consumers.
type Worker struct {
	StopTimeout time.Duration
	consumers   []*Consumer
	wg          sync.WaitGroup
}

// Create new worker.
func NewWorker() *Worker {
	return &Worker{
		StopTimeout: 1 * time.Minute,
	}
}

// Add a consumer to this worker.
func (m *Worker) AddConsumer(c *Consumer) {
	m.consumers = append(m.consumers, c)
}

// Startup this worker, and waiting for a signal to shutdown.
func (m *Worker) Startup(lookupdAddr string) (err error) {
	for _, c := range m.consumers {
		if err = c.ConnectToNSQLookupd(lookupdAddr); err != nil {
			return
		}

		m.wg.Add(1)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for {
			sig := <-c
			logger.Info("receive signal: %s", sig)

			for _, c := range m.consumers {
				logger.Info("stopping consumer... topic: %s, channel: %s...", c.Topic, c.Channel)
				c.consumer.Stop()

				select {
				case <-c.consumer.StopChan:
					logger.Info("consumer stopped. topic: %s, channel: %s", c.Topic, c.Channel)
					m.wg.Done()
				case <-time.After(m.StopTimeout):
					logger.Warning("timeout while stopping consumer. topic: %s, channel: %s (waited %v)", c.Topic, c.Channel, m.StopTimeout)
				}
			}
		}
	}()

	logger.Info("worker start successful.")
	m.wg.Wait()
	logger.Info("worker stopped.")

	return
}
