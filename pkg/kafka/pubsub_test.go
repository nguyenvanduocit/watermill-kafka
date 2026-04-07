package kafka_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/nguyenvanduocit/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func kafkaBrokers() []string {
	brokers := os.Getenv("WATERMILL_TEST_KAFKA_BROKERS")
	if brokers != "" {
		return strings.Split(brokers, ",")
	}
	return []string{"localhost:9091", "localhost:9092", "localhost:9093"}
}

func skipIfNoKafka(t testing.TB) {
	t.Helper()
	brokers := kafkaBrokers()
	conn, err := net.DialTimeout("tcp", brokers[0], 3*time.Second)
	if err != nil {
		t.Skipf("Kafka not available at %s, skipping integration test", brokers[0])
	}
	conn.Close()
}

func newPubSub(
	t *testing.T,
	marshaler kafka.MarshalerUnmarshaler,
	consumerGroup string,
	kgoOpts ...kgo.Opt,
) (*kafka.Publisher, *kafka.Subscriber) {
	logger := watermill.NewStdLogger(false, false)

	var err error
	var publisher *kafka.Publisher

	retriesLeft := 5
	for {
		publisher, err = kafka.NewPublisher(kafka.PublisherConfig{
			Brokers:   kafkaBrokers(),
			Marshaler: marshaler,
		}, logger)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Publisher: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}
	require.NoError(t, err)

	var sub *kafka.Subscriber

	retriesLeft = 5
	for {
		sub, err = kafka.NewSubscriber(
			kafka.SubscriberConfig{
				Brokers:                          kafkaBrokers(),
				Unmarshaler:                      marshaler,
				ConsumerGroup:                    consumerGroup,
				ConsumeFromOldest:                true,
				InitializeTopicNumPartitions:     50,
				InitializeTopicReplicationFactor: 1,
				KgoOpts:                          kgoOpts,
			},
			logger,
		)
		if err == nil || retriesLeft == 0 {
			break
		}

		retriesLeft--
		fmt.Printf("cannot create kafka Subscriber: %s, retrying (%d retries left)", err, retriesLeft)
		time.Sleep(time.Second * 2)
	}

	require.NoError(t, err)

	return publisher, sub
}

func generatePartitionKey(topic string, msg *message.Message) (string, error) {
	return msg.Metadata.Get("partition_key"), nil
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, consumerGroup)
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, "test")
}

func createPartitionedPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.NewWithPartitioningMarshaler(generatePartitionKey), "test")
}

func createNoGroupPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return newPubSub(t, kafka.DefaultMarshaler{}, "")
}

func TestPublishSubscribe(t *testing.T) {
	skipIfNoKafka(t)
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     false,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_ordered(t *testing.T) {
	skipIfNoKafka(t)
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	t.Parallel()

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     true,
			Persistent:          true,
		},
		createPartitionedPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestNoGroupSubscriber(t *testing.T) {
	skipIfNoKafka(t)
	if testing.Short() {
		t.Skip("skipping long tests")
	}

	t.Parallel()

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                   false,
			ExactlyOnceDelivery:              false,
			GuaranteedOrder:                  false,
			Persistent:                       true,
			NewSubscriberReceivesOldMessages: true,
		},
		createNoGroupPubSub,
		nil,
	)
}

func TestCtxValues(t *testing.T) {
	skipIfNoKafka(t)
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	for i := 0; i < 20; i++ {
		id := watermill.NewUUID()
		messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))
	}
	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessages, all := subscriber.BulkReadWithDeduplication(messages, len(messagesToPublish), time.Second*10)
	require.True(t, all)

	expectedPartitionsOffsets := map[int32]int64{}
	for _, msg := range receivedMessages {
		partition, ok := kafka.MessagePartitionFromCtx(msg.Context())
		assert.True(t, ok)

		messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(msg.Context())
		assert.True(t, ok)

		kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(msg.Context())
		assert.True(t, ok)
		assert.NotZero(t, kafkaMsgTimestamp)

		_, ok = kafka.MessageKeyFromCtx(msg.Context())
		assert.True(t, ok)

		if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
			expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
		}
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}

func readAfterRetries(messagesCh <-chan *message.Message, retriesN int, timeout time.Duration) (receivedMessage *message.Message, ok bool) {
	retries := 0

MessagesLoop:
	for retries <= retriesN {
		select {
		case msg, ok := <-messagesCh:
			if !ok {
				break MessagesLoop
			}

			if retries > 0 {
				msg.Ack()
				return msg, true
			}

			msg.Nack()
			retries++
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return nil, false
}

func TestCtxValuesAfterRetry(t *testing.T) {
	skipIfNoKafka(t)
	pub, sub := newPubSub(t, kafka.DefaultMarshaler{}, "")
	topicName := "topic_" + watermill.NewUUID()

	var messagesToPublish []*message.Message

	id := watermill.NewUUID()
	messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

	err := pub.Publish(topicName, messagesToPublish...)
	require.NoError(t, err, "cannot publish message")

	messages, err := sub.Subscribe(context.Background(), topicName)
	require.NoError(t, err)

	receivedMessage, ok := readAfterRetries(messages, 1, time.Second)
	assert.True(t, ok)

	expectedPartitionsOffsets := map[int32]int64{}
	partition, ok := kafka.MessagePartitionFromCtx(receivedMessage.Context())
	assert.True(t, ok)

	messagePartitionOffset, ok := kafka.MessagePartitionOffsetFromCtx(receivedMessage.Context())
	assert.True(t, ok)

	kafkaMsgTimestamp, ok := kafka.MessageTimestampFromCtx(receivedMessage.Context())
	assert.True(t, ok)
	assert.NotZero(t, kafkaMsgTimestamp)

	if expectedPartitionsOffsets[partition] <= messagePartitionOffset {
		expectedPartitionsOffsets[partition] = messagePartitionOffset + 1
	}
	assert.NotEmpty(t, expectedPartitionsOffsets)

	offsets, err := sub.PartitionOffset(topicName)
	require.NoError(t, err)
	assert.NotEmpty(t, offsets)

	assert.EqualValues(t, expectedPartitionsOffsets, offsets)

	require.NoError(t, pub.Close())
}
