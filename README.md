kueue
=====

A simple producer&amp;consumer wrapper based on NSQ.

## Features

- Publish any type of message
- Support delay job
- Within producers pool
- Worker babysitter can take care of multiple consumers

## Usage

### Producers

```go
import (
    "time"

    "github.com/heroicyang/kueue"
)

kueue.SetupProducers(nsqdAddr, poolSize)

// publish any type of message
kueue.Publish("topic", 0, topicStruct)

// publish a delayed message
kueue.Publish("delayedTopic", 1 * time.Hour, topicStruct)
```

### Consumers

```go
import (
    "encoding/json"

    "github.com/heroicyang/kueue"
)

type TopicJob struct {
  Topic *TopicStruct
}

func (t *TopicJob) Perform() error {
  // perform your job
  return
}

func newTopicJob(payload *kueue.Payload) (kueue.Job, error) {
  job := new(TopicJob)

  err := json.Unmarshal(payload.Body, &job.Topic)

  return job, err
}

func main() {
  worker := kueue.NewWorker()

  consumer := kueue.NewConsumer("topic", "channel", 5, newTopicJob)
  worker.AddConsumer(consumer)

  worker.Startup()
}
```
