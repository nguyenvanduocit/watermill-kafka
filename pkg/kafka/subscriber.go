package kafka

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Subscriber struct {
	config SubscriberConfig
	logger watermill.LoggerAdapter

	closing       chan struct{}
	subscribersWg sync.WaitGroup

	closed uint32
}

// NewSubscriber creates a new Kafka Subscriber backed by franz-go.
func NewSubscriber(
	config SubscriberConfig,
	logger watermill.LoggerAdapter,
) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	logger = logger.With(watermill.LogFields{
		"subscriber_uuid": watermill.NewShortUUID(),
	})

	return &Subscriber{
		config: config,
		logger: logger,

		closing: make(chan struct{}),
	}, nil
}

type SubscriberConfig struct {
	// Kafka brokers list.
	Brokers []string

	// Unmarshaler is used to unmarshal messages from Kafka format into Watermill format.
	Unmarshaler Unmarshaler

	// Kafka consumer group.
	// When empty, all messages from all partitions will be returned.
	ConsumerGroup string

	// How long after Nack message should be redelivered.
	NackResendSleep time.Duration

	// How long after unsuccessful reconnecting next reconnect will occur.
	ReconnectRetrySleep time.Duration

	// InitializeTopicNumPartitions sets the number of partitions when auto-creating topics.
	// If zero, SubscribeInitialize is a no-op.
	InitializeTopicNumPartitions int32

	// InitializeTopicReplicationFactor sets the replication factor when auto-creating topics.
	// Defaults to 1 if not set.
	InitializeTopicReplicationFactor int16

	// If true, SubscribeInitialize won't wait for all partitions to be created across brokers.
	DoNotWaitForTopicCreation bool

	// Timeout for waiting for topic creation.
	WaitForTopicCreationTimeout time.Duration

	// KgoOpts are additional franz-go client options.
	// Use this to customize consumer behavior (e.g., custom balancer, SASL).
	KgoOpts []kgo.Opt

	// ConsumeFromOldest controls initial offset. When true, starts from the oldest
	// available offset. When false (default), starts from the newest offset.
	ConsumeFromOldest bool
}

// NoSleep can be set to SubscriberConfig.NackResendSleep and SubscriberConfig.ReconnectRetrySleep.
const NoSleep time.Duration = -1

func (c *SubscriberConfig) setDefaults() {
	if c.NackResendSleep == 0 {
		c.NackResendSleep = time.Millisecond * 100
	}
	if c.ReconnectRetrySleep == 0 {
		c.ReconnectRetrySleep = time.Second
	}
	if c.Unmarshaler == nil {
		c.Unmarshaler = DefaultMarshaler{}
	}
	if c.WaitForTopicCreationTimeout == 0 {
		c.WaitForTopicCreationTimeout = 10 * time.Second
	}
	if c.InitializeTopicReplicationFactor == 0 {
		c.InitializeTopicReplicationFactor = 1
	}
}

func (c SubscriberConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("missing brokers")
	}
	if c.Unmarshaler == nil {
		return errors.New("missing unmarshaler")
	}

	return nil
}

// Subscribe subscribes for messages in Kafka.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, errors.New("subscriber closed")
	}

	s.subscribersWg.Add(1)

	logFields := watermill.LogFields{
		"provider":            "kafka",
		"topic":               topic,
		"consumer_group":      s.config.ConsumerGroup,
		"kafka_consumer_uuid": watermill.NewShortUUID(),
	}
	s.logger.Info("Subscribing to Kafka topic", logFields)

	// Unbuffered channel — don't consume from Kafka when consumer is not consuming.
	output := make(chan *message.Message)

	consumeClosed, err := s.consumeMessages(ctx, topic, output, logFields)
	if err != nil {
		s.subscribersWg.Done()
		return nil, err
	}

	go func() {
		s.handleReconnects(ctx, topic, output, consumeClosed, logFields)
		close(output)
		s.subscribersWg.Done()
	}()

	return output, nil
}

func (s *Subscriber) handleReconnects(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	consumeClosed chan struct{},
	logFields watermill.LogFields,
) {
	for {
		if consumeClosed != nil {
			<-consumeClosed
			s.logger.Debug("consumeMessages stopped", logFields)
		} else {
			s.logger.Debug("empty consumeClosed", logFields)
		}

		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, no reconnect needed", logFields)
			return
		case <-ctx.Done():
			s.logger.Debug("Ctx cancelled, no reconnect needed", logFields)
			return
		default:
			s.logger.Debug("Not closing, reconnecting", logFields)
		}

		s.logger.Info("Reconnecting consumer", logFields)

		var err error
		consumeClosed, err = s.consumeMessages(ctx, topic, output, logFields)
		if err != nil {
			s.logger.Error("Cannot reconnect messages consumer", err, logFields)

			if s.config.ReconnectRetrySleep != NoSleep {
				time.Sleep(s.config.ReconnectRetrySleep)
			}
			continue
		}
	}
}

func (s *Subscriber) consumeMessages(
	ctx context.Context,
	topic string,
	output chan *message.Message,
	logFields watermill.LogFields,
) (consumeMessagesClosed chan struct{}, err error) {
	s.logger.Info("Starting consuming", logFields)

	// Build kgo options for this consumer session.
	startOffset := kgo.NewOffset().AtEnd()
	if s.config.ConsumeFromOldest {
		startOffset = kgo.NewOffset().AtStart()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(s.config.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(startOffset),
	}

	if s.config.ConsumerGroup != "" {
		opts = append(opts,
			kgo.ConsumerGroup(s.config.ConsumerGroup),
			// #cooperative-rebalance — the whole point of this migration
			kgo.Balancers(kgo.CooperativeStickyBalancer()),
			kgo.AutoCommitMarks(),
		)
	}

	opts = append(opts, s.config.KgoOpts...)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			s.logger.Debug("Closing subscriber, cancelling consumeMessages", logFields)
			cancel()
		case <-ctx.Done():
			// avoid goroutine leak
		}
	}()

	client, err := kgo.NewClient(opts...)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "cannot create Kafka client")
	}

	consumeMessagesClosed = make(chan struct{})
	go func() {
		defer func() {
			client.Close()
			s.logger.Debug("Client closed", logFields)
			close(consumeMessagesClosed)
		}()

		s.pollLoop(ctx, client, output, logFields)
	}()

	return consumeMessagesClosed, nil
}

// pollLoop continuously polls Kafka for new records and processes them through
// the Watermill message pipeline (unmarshal → send to output → wait for ack/nack).
func (s *Subscriber) pollLoop(
	ctx context.Context,
	client *kgo.Client,
	output chan *message.Message,
	logFields watermill.LogFields,
) {
	for {
		select {
		case <-s.closing:
			s.logger.Debug("Subscriber is closing, stopping poll loop", logFields)
			return
		case <-ctx.Done():
			s.logger.Debug("Ctx was cancelled, stopping poll loop", logFields)
			return
		default:
		}

		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fetchErr := range errs {
				// Context cancellation is expected during shutdown.
				if errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
					return
				}
				s.logger.Error("Fetch error", fetchErr.Err, logFields.Add(watermill.LogFields{
					"fetch_topic":     fetchErr.Topic,
					"fetch_partition": fetchErr.Partition,
				}))
			}
		}

		var processErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if processErr != nil {
				return
			}
			if err := s.processRecord(ctx, client, record, output, logFields); err != nil {
				processErr = err
			}
		})
		if processErr != nil {
			s.logger.Error("Error processing record, breaking poll loop", processErr, logFields)
			return
		}

		// AllowRebalance lets the cooperative balancer complete pending rebalances
		// between poll iterations. #cooperative-rebalance
		client.AllowRebalance()
	}
}

func (s *Subscriber) processRecord(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	output chan *message.Message,
	messageLogFields watermill.LogFields,
) error {
	receivedMsgLogFields := messageLogFields.Add(watermill.LogFields{
		"kafka_partition_offset": record.Offset,
		"kafka_partition":        record.Partition,
	})

	s.logger.Trace("Received message from Kafka", receivedMsgLogFields)

	ctx = setPartitionToCtx(ctx, record.Partition)
	ctx = setPartitionOffsetToCtx(ctx, record.Offset)
	ctx = setMessageTimestampToCtx(ctx, record.Timestamp)
	ctx = setMessageKeyToCtx(ctx, record.Key)

	msg, err := s.config.Unmarshaler.Unmarshal(record)
	if err != nil {
		// Resend will make no sense — unmarshal error is permanent.
		return errors.Wrap(err, "message unmarshal failed")
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	receivedMsgLogFields = receivedMsgLogFields.Add(watermill.LogFields{
		"message_uuid": msg.UUID,
	})

ResendLoop:
	for {
		select {
		case output <- msg:
			s.logger.Trace("Message sent to consumer", receivedMsgLogFields)
		case <-s.closing:
			s.logger.Trace("Closing, message discarded", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			s.logger.Trace("Closing, ctx cancelled before sent to consumer", receivedMsgLogFields)
			return nil
		}

		select {
		case <-msg.Acked():
			// MarkCommitRecords tells the auto-committer that this offset is safe to commit.
			// Combined with AutoCommitMarks(), offsets are committed periodically.
			client.MarkCommitRecords(record)
			s.logger.Trace("Message Acked", receivedMsgLogFields)
			break ResendLoop
		case <-msg.Nacked():
			s.logger.Trace("Message Nacked", receivedMsgLogFields)

			// Reset acks, etc.
			msg = msg.Copy()
			msg.SetContext(ctx)

			if s.config.NackResendSleep != NoSleep {
				time.Sleep(s.config.NackResendSleep)
			}

			continue ResendLoop
		case <-s.closing:
			s.logger.Trace("Closing, message discarded before ack", receivedMsgLogFields)
			return nil
		case <-ctx.Done():
			s.logger.Trace("Closing, ctx cancelled before ack", receivedMsgLogFields)
			return nil
		}
	}

	return nil
}

func (s *Subscriber) Close() error {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return nil
	}

	close(s.closing)
	s.subscribersWg.Wait()

	s.logger.Debug("Kafka subscriber closed", nil)

	return nil
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	return s.SubscribeInitializeWithContext(context.Background(), topic)
}

func (s *Subscriber) SubscribeInitializeWithContext(ctx context.Context, topic string) (err error) {
	if s.config.InitializeTopicNumPartitions == 0 {
		return errors.New("s.config.InitializeTopicNumPartitions is zero, cannot SubscribeInitialize")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(s.config.Brokers...))
	if err != nil {
		return errors.Wrap(err, "cannot create admin client")
	}
	defer client.Close()

	admClient := kadm.NewClient(client)

	resp, err := admClient.CreateTopics(ctx, int32(s.config.InitializeTopicNumPartitions), s.config.InitializeTopicReplicationFactor, nil, topic)
	if err != nil {
		return errors.Wrap(err, "cannot create topic")
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil && !strings.Contains(r.Err.Error(), "TOPIC_ALREADY_EXISTS") {
			return errors.Wrap(r.Err, "cannot create topic")
		}
	}

	s.logger.Info("Created Kafka topic", watermill.LogFields{"topic": topic})

	if !s.config.DoNotWaitForTopicCreation {
		ctxTimeout, cancel := context.WithTimeout(ctx, s.config.WaitForTopicCreationTimeout)
		defer cancel()

		if err := s.waitForTopicCreation(ctxTimeout, admClient, topic); err != nil {
			return errors.Wrap(err, "failed to wait for topic creation")
		}
	}

	return nil
}

func (s *Subscriber) waitForTopicCreation(ctx context.Context, admClient *kadm.Client, topic string) error {
	logFields := watermill.LogFields{"topic": topic}
	s.logger.Debug("Waiting for topic creation to be confirmed", logFields)

	pollInterval := 500 * time.Millisecond
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for topic creation")
		default:
		}

		metadata, err := admClient.Metadata(ctx, topic)
		if err != nil {
			s.logger.Debug("Failed to get metadata", logFields.Add(watermill.LogFields{
				"attempt": attempt + 1,
				"error":   err.Error(),
			}))
		} else {
			topicMeta, exists := metadata.Topics[topic]
			if exists && topicMeta.Err == nil {
				allReady := true
				for _, p := range topicMeta.Partitions.Sorted() {
					if p.Leader == -1 {
						allReady = false
						break
					}
				}
				if allReady && len(topicMeta.Partitions) >= int(s.config.InitializeTopicNumPartitions) {
					s.logger.Debug("Topic and partitions creation confirmed", logFields.Add(watermill.LogFields{
						"attempt": attempt + 1,
					}))
					return nil
				}
			}
		}

		s.logger.Debug("Topic not yet available, retrying", logFields.Add(watermill.LogFields{
			"attempt":  attempt + 1,
			"retry_in": pollInterval.String(),
		}))

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for topic creation")
		case <-timer.C:
		}

		attempt++
	}
}

type PartitionOffset map[int32]int64

func (s *Subscriber) PartitionOffset(topic string) (PartitionOffset, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(s.config.Brokers...))
	if err != nil {
		return nil, errors.Wrap(err, "cannot create Kafka client")
	}
	defer client.Close()

	admClient := kadm.NewClient(client)

	offsets, err := admClient.ListEndOffsets(context.Background(), topic)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list end offsets")
	}

	result := make(PartitionOffset)
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			err = multierror.Append(err, o.Err)
			return
		}
		result[o.Partition] = o.Offset
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}
