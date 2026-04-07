package kafka

import (
	"context"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher struct {
	config PublisherConfig
	client *kgo.Client
	logger watermill.LoggerAdapter

	closed bool
}

// NewPublisher creates a new Kafka Publisher backed by franz-go.
func NewPublisher(
	config PublisherConfig,
	logger watermill.LoggerAdapter,
) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(10),
		// Allow the broker to auto-create topics on first produce (if broker
		// has auto.create.topics.enable=true). Without this, franz-go returns
		// UNKNOWN_TOPIC_OR_PARTITION immediately instead of triggering creation.
		kgo.AllowAutoTopicCreation(),
	}
	opts = append(opts, config.KgoOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka producer")
	}

	return &Publisher{
		config: config,
		client: client,
		logger: logger,
	}, nil
}

type PublisherConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Marshaler is used to marshal messages from Watermill format into Kafka format.
	Marshaler Marshaler

	// KgoOpts are additional franz-go client options for the producer.
	// Use this to customize producer behavior (e.g., custom partitioner, compression).
	KgoOpts []kgo.Opt
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = DefaultMarshaler{}
	}
}

func (c PublisherConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Marshaler == nil {
		return errors.New("missing marshaler")
	}

	return nil
}

// Publish publishes message to Kafka.
//
// Publish is blocking and waits for ack from Kafka.
// When one of the messages delivery fails, the function returns immediately.
func (p *Publisher) Publish(topic string, msgs ...*message.Message) error {
	if p.closed {
		return errors.New("publisher closed")
	}

	logFields := make(watermill.LogFields, 4)
	logFields["topic"] = topic

	for _, msg := range msgs {
		logFields["message_uuid"] = msg.UUID
		p.logger.Trace("Sending message to Kafka", logFields)

		record, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return errors.Wrapf(err, "cannot marshal message %s", msg.UUID)
		}

		// ProduceSync blocks until the broker acks the record.
		results := p.client.ProduceSync(context.Background(), record)
		if err := results.FirstErr(); err != nil {
			return errors.Wrapf(err, "cannot produce message %s", msg.UUID)
		}

		r := results[0].Record
		logFields["kafka_partition"] = r.Partition
		logFields["kafka_partition_offset"] = r.Offset

		p.logger.Trace("Message sent to Kafka", logFields)
	}

	return nil
}

func (p *Publisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.client.Close()

	return nil
}
