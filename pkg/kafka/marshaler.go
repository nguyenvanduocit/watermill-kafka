package kafka

import (
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill/message"
)

const UUIDHeaderKey = "_watermill_message_uuid"

// Marshaler marshals Watermill's message to a Kafka record.
type Marshaler interface {
	Marshal(topic string, msg *message.Message) (*kgo.Record, error)
}

// Unmarshaler unmarshals a Kafka record to Watermill's message.
type Unmarshaler interface {
	Unmarshal(*kgo.Record) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
	if value := msg.Metadata.Get(UUIDHeaderKey); value != "" {
		return nil, errors.Errorf("metadata %s is reserved by watermill for message UUID", UUIDHeaderKey)
	}

	headers := []kgo.RecordHeader{{
		Key:   UUIDHeaderKey,
		Value: []byte(msg.UUID),
	}}
	for key, value := range msg.Metadata {
		headers = append(headers, kgo.RecordHeader{
			Key:   key,
			Value: []byte(value),
		})
	}

	return &kgo.Record{
		Topic:   topic,
		Value:   msg.Payload,
		Headers: headers,
	}, nil
}

func (DefaultMarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
	var messageID string
	metadata := make(message.Metadata, len(record.Headers))

	for _, header := range record.Headers {
		if header.Key == UUIDHeaderKey {
			messageID = string(header.Value)
		} else {
			metadata.Set(header.Key, string(header.Value))
		}
	}

	msg := message.NewMessage(messageID, record.Value)
	msg.Metadata = metadata

	return msg, nil
}

type GeneratePartitionKey func(topic string, msg *message.Message) (string, error)

type kafkaJsonWithPartitioning struct {
	DefaultMarshaler

	generatePartitionKey GeneratePartitionKey
}

func NewWithPartitioningMarshaler(generatePartitionKey GeneratePartitionKey) MarshalerUnmarshaler {
	return kafkaJsonWithPartitioning{generatePartitionKey: generatePartitionKey}
}

func (j kafkaJsonWithPartitioning) Marshal(topic string, msg *message.Message) (*kgo.Record, error) {
	record, err := j.DefaultMarshaler.Marshal(topic, msg)
	if err != nil {
		return nil, err
	}

	key, err := j.generatePartitionKey(topic, msg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate partition key")
	}
	record.Key = []byte(key)

	return record, nil
}
