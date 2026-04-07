package kafka_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/nguyenvanduocit/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	record, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	assert.Equal(t, "topic", record.Topic)
	assert.Equal(t, []byte("payload"), record.Value)

	unmarshaledMsg, err := m.Unmarshal(record)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func TestDefaultMarshaler_ReservedHeader(t *testing.T) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set(kafka.UUIDHeaderKey, "should-fail")

	_, err := m.Marshal("topic", msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved by watermill")
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		_, _ = m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	record, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, _ = m.Unmarshal(record)
	}
}

func TestWithPartitioningMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
		return msg.Metadata.Get("partition"), nil
	})

	partitionKey := "1"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("partition", partitionKey)

	record, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	assert.Equal(t, partitionKey, string(record.Key))

	unmarshaledMsg, err := m.Unmarshal(record)
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}
