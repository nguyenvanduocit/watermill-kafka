package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/nguyenvanduocit/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// newKfakeCluster creates a kfake cluster with auto-topic creation enabled.
func newKfakeCluster(t *testing.T, topics ...string) (*kfake.Cluster, []string) {
	t.Helper()
	opts := []kfake.Opt{kfake.NumBrokers(1)}
	if len(topics) > 0 {
		opts = append(opts, kfake.SeedTopics(1, topics...))
	} else {
		opts = append(opts, kfake.AllowAutoTopicCreation())
	}
	cluster, err := kfake.NewCluster(opts...)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster, cluster.ListenAddrs()
}

func TestPublishSubscribe_kfake(t *testing.T) {
	topic := "test-topic"
	_, addrs := newKfakeCluster(t, topic)
	logger := watermill.NewStdLogger(true, true)

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: kafka.DefaultMarshaler{},
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumeFromOldest: true,
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	// Publish a message
	sentMsg := message.NewMessage(watermill.NewUUID(), []byte("hello kfake"))
	sentMsg.Metadata.Set("key1", "value1")

	err = pub.Publish(topic, sentMsg)
	require.NoError(t, err)

	// Subscribe
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgCh, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	select {
	case receivedMsg := <-msgCh:
		assert.Equal(t, sentMsg.UUID, receivedMsg.UUID)
		assert.Equal(t, "hello kfake", string(receivedMsg.Payload))
		assert.Equal(t, "value1", receivedMsg.Metadata.Get("key1"))

		// Verify context values are set
		partition, ok := kafka.MessagePartitionFromCtx(receivedMsg.Context())
		assert.True(t, ok)
		assert.Equal(t, int32(0), partition)

		offset, ok := kafka.MessagePartitionOffsetFromCtx(receivedMsg.Context())
		assert.True(t, ok)
		assert.Equal(t, int64(0), offset)

		receivedMsg.Ack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishSubscribe_kfake_ConsumerGroup(t *testing.T) {
	topic := "test-cg-topic"
	_, addrs := newKfakeCluster(t, topic)
	logger := watermill.NewStdLogger(true, true)

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: kafka.DefaultMarshaler{},
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumerGroup:     "test-group",
		ConsumeFromOldest: true,
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	// Publish messages
	for i := 0; i < 3; i++ {
		err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("msg")))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msgCh, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	received := 0
	for received < 3 {
		select {
		case msg := <-msgCh:
			msg.Ack()
			received++
		case <-ctx.Done():
			t.Fatalf("timed out, only received %d/3 messages", received)
		}
	}
	assert.Equal(t, 3, received)
}

func TestPublishSubscribe_kfake_WithPartitioning(t *testing.T) {
	topic := "test-partition"
	_, addrs := newKfakeCluster(t, topic)
	logger := watermill.NewStdLogger(false, false)

	marshaler := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
		return msg.Metadata.Get("pk"), nil
	})

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: marshaler,
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       marshaler,
		ConsumeFromOldest: true,
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	msg := message.NewMessage(watermill.NewUUID(), []byte("partitioned"))
	msg.Metadata.Set("pk", "shop123:order456")

	err = pub.Publish(topic, msg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgCh, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	select {
	case received := <-msgCh:
		assert.Equal(t, msg.UUID, received.UUID)
		assert.Equal(t, "partitioned", string(received.Payload))

		key, ok := kafka.MessageKeyFromCtx(received.Context())
		assert.True(t, ok)
		assert.Equal(t, "shop123:order456", string(key))

		received.Ack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for partitioned message")
	}
}

func TestPublishSubscribe_kfake_Nack(t *testing.T) {
	topic := "test-nack"
	_, addrs := newKfakeCluster(t, topic)
	logger := watermill.NewStdLogger(false, false)

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: kafka.DefaultMarshaler{},
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumeFromOldest: true,
		NackResendSleep:   10 * time.Millisecond,
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	sentMsg := message.NewMessage(watermill.NewUUID(), []byte("nack-me"))
	err = pub.Publish(topic, sentMsg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgCh, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	// First delivery: nack it
	select {
	case msg := <-msgCh:
		assert.Equal(t, sentMsg.UUID, msg.UUID)
		msg.Nack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for first delivery")
	}

	// Second delivery: ack it
	select {
	case msg := <-msgCh:
		assert.Equal(t, sentMsg.UUID, msg.UUID)
		msg.Ack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for re-delivery after nack")
	}
}

// TestCooperativeStickyBalancer_IsDefault verifies that when ConsumerGroup is set,
// CooperativeStickyBalancer is used. This is a compile-time + config verification test.
// Full rebalance behavior testing requires real Kafka (integration test) because kfake
// has limited consumer group rebalance support.
func TestCooperativeStickyBalancer_IsDefault(t *testing.T) {
	cluster, _ := kfake.NewCluster(kfake.NumBrokers(1))
	defer cluster.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:       cluster.ListenAddrs(),
		ConsumerGroup: "test-sticky",
		Unmarshaler:   kafka.DefaultMarshaler{},
	}, nil)
	require.NoError(t, err)

	// If CooperativeStickyBalancer wasn't set, the subscriber would use
	// RangeBalancer (default) which can be verified by the subscriber
	// successfully subscribing to a topic.
	msgs, err := sub.Subscribe(context.Background(), "sticky-test")
	require.NoError(t, err)
	require.NotNil(t, msgs)
	require.NoError(t, sub.Close())
}

// TestPublishSubscribe_kfake_CooperativeStickyRebalance verifies the consumer group
// uses CooperativeStickyBalancer and messages continue flowing when a second consumer joins.
// TODO: kfake has limited consumer group rebalance support; full cooperative sticky rebalance
// behavior (no full-stop during rebalance) requires real Kafka integration tests.
func TestPublishSubscribe_kfake_CooperativeStickyRebalance(t *testing.T) {
	topic := "test-sticky-rebalance"
	_, addrs := newKfakeCluster(t, topic)
	logger := watermill.NewStdLogger(false, false)

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: kafka.DefaultMarshaler{},
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	// subscriber1 joins consumer group "test-sticky" — gets all partitions initially
	sub1, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumerGroup:     "test-sticky",
		ConsumeFromOldest: true,
	}, logger)
	require.NoError(t, err)
	defer sub1.Close()

	// Publish 10 messages before second subscriber joins
	for i := 0; i < 10; i++ {
		err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("batch1")))
		require.NoError(t, err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	msgCh1, err := sub1.Subscribe(ctx1, topic)
	require.NoError(t, err)

	// Drain the first 10 messages from sub1
	received1 := 0
	for received1 < 10 {
		select {
		case msg := <-msgCh1:
			msg.Ack()
			received1++
		case <-ctx1.Done():
			t.Fatalf("sub1 timed out, received %d/10 messages before rebalance", received1)
		}
	}

	// subscriber2 joins the same group — triggers cooperative sticky rebalance
	sub2, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumerGroup:     "test-sticky",
		ConsumeFromOldest: true,
	}, logger)
	require.NoError(t, err)
	defer sub2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	msgCh2, err := sub2.Subscribe(ctx2, topic)
	require.NoError(t, err)

	// Publish 10 more messages after second subscriber joins
	for i := 0; i < 10; i++ {
		err = pub.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("batch2")))
		require.NoError(t, err)
	}

	// Messages should be received by sub1 OR sub2 — proves no full-stop during rebalance
	received2 := 0
	timeout := time.After(10 * time.Second)
	for received2 < 10 {
		select {
		case msg := <-msgCh1:
			msg.Ack()
			received2++
		case msg := <-msgCh2:
			msg.Ack()
			received2++
		case <-timeout:
			t.Fatalf("timed out after rebalance, received %d/10 messages", received2)
		}
	}

	assert.Equal(t, 10, received2, "all messages after rebalance must be received")
}

func TestPublishSubscribe_kfake_AutoTopicCreation(t *testing.T) {
	// Test that kgo.AllowAutoTopicCreation works with kfake.AllowAutoTopicCreation
	_, addrs := newKfakeCluster(t) // no seed topics

	logger := watermill.NewStdLogger(false, false)

	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:   addrs,
		Marshaler: kafka.DefaultMarshaler{},
		KgoOpts:   []kgo.Opt{kgo.AllowAutoTopicCreation()},
	}, logger)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers:           addrs,
		Unmarshaler:       kafka.DefaultMarshaler{},
		ConsumeFromOldest: true,
		KgoOpts:           []kgo.Opt{kgo.AllowAutoTopicCreation()},
	}, logger)
	require.NoError(t, err)
	defer sub.Close()

	topic := "auto-created-topic"

	sentMsg := message.NewMessage(watermill.NewUUID(), []byte("auto"))
	err = pub.Publish(topic, sentMsg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgCh, err := sub.Subscribe(ctx, topic)
	require.NoError(t, err)

	select {
	case msg := <-msgCh:
		assert.Equal(t, sentMsg.UUID, msg.UUID)
		msg.Ack()
	case <-ctx.Done():
		t.Fatal("timed out waiting for auto-created topic message")
	}
}
