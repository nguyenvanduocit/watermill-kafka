# Watermill Kafka Pub/Sub (franz-go)

Fork of [ThreeDotsLabs/watermill-kafka](https://github.com/ThreeDotsLabs/watermill-kafka) that replaces [IBM/sarama](https://github.com/IBM/sarama) with [twmb/franz-go](https://github.com/twmb/franz-go).

## Why this fork?

Sarama does not support the **cooperative rebalance protocol**. Every consumer group rebalance (rolling update, pod scale, etc.) causes a **full stop-the-world** pause across all consumers — all partitions are revoked and reassigned, even those unaffected by the change. In production with 6+ pods and 24 partitions, this creates 2,000–12,000 message backlogs per partition after each deployment.

franz-go supports `CooperativeStickyBalancer` natively — only affected partitions move during rebalance, and unaffected consumers keep processing without interruption.

## What changed

| | Sarama (upstream) | franz-go (this fork) |
|--|---|---|
| **Rebalance** | Eager only (Range/RoundRobin/Sticky) | **CooperativeSticky** (default) |
| **Consumer model** | Callback (`ConsumerGroupHandler`) | Poll (`PollFetches`) |
| **Producer** | `SyncProducer.SendMessage` | `Client.ProduceSync` |
| **Config** | `*sarama.Config` struct | `[]kgo.Opt` functional options |
| **Message type** | `ProducerMessage` + `ConsumerMessage` | `*kgo.Record` (unified) |
| **OTel** | `otelsarama` wrapper | `kotel` hooks |
| **Topic admin** | `sarama.ClusterAdmin` | `kadm.Client` |
| **Code size** | ~1,800 lines | ~1,400 lines (-22%) |

## Installation

```bash
go get github.com/nguyenvanduocit/watermill-kafka/v3
```

## Usage

### Publisher

```go
pub, err := kafka.NewPublisher(kafka.PublisherConfig{
    Brokers: []string{"localhost:9092"},
}, logger)

err = pub.Publish("my-topic", message.NewMessage(watermill.NewUUID(), []byte("hello")))
```

### Subscriber (consumer group)

CooperativeStickyBalancer is enabled by default when `ConsumerGroup` is set.

```go
sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
    Brokers:       []string{"localhost:9092"},
    ConsumerGroup: "my-group",
}, logger)

messages, err := sub.Subscribe(ctx, "my-topic")
```

### Custom kgo options

Use `KgoOpts` for advanced configuration (SASL, TLS, custom balancer, etc.):

```go
sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
    Brokers:       []string{"localhost:9092"},
    ConsumerGroup: "my-group",
    KgoOpts: []kgo.Opt{
        kgo.SASL(scram.Auth{User: "user", Pass: "pass"}.AsSHA256Mechanism()),
        kgo.DialTLSConfig(&tls.Config{}),
    },
}, logger)
```

### Marshaler

The marshaler interface uses `*kgo.Record` (not Sarama types):

```go
type Marshaler interface {
    Marshal(topic string, msg *message.Message) (*kgo.Record, error)
}

type Unmarshaler interface {
    Unmarshal(msg *kgo.Record) (*message.Message, error)
}
```

`DefaultMarshaler` is used when no marshaler is specified.

## Migration from upstream

Replace import path and update config:

```diff
- import "github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
+ import "github.com/nguyenvanduocit/watermill-kafka/v3/pkg/kafka"

  sub, err := kafka.NewSubscriber(kafka.SubscriberConfig{
      Brokers:       brokers,
      ConsumerGroup: group,
-     OverwriteSaramaConfig: saramaCfg,
+     KgoOpts: []kgo.Opt{
+         // your custom options here
+     },
  }, logger)
```

Key differences:
- `OverwriteSaramaConfig` replaced by `KgoOpts []kgo.Opt`
- `InitializeTopicDetails *sarama.TopicDetail` replaced by `InitializeTopicNumPartitions` + `InitializeTopicReplicationFactor`
- `ConsumeFromOldest bool` replaces manual offset configuration
- `SaramaTracer` interface removed — use `kotel` hooks via `KgoOpts`

## Testing

```bash
# Unit tests (in-memory Kafka via kfake, no Docker needed)
go test -race ./pkg/kafka/ -run kfake

# Integration tests (requires Docker)
docker compose up -d
go test -race -v ./pkg/kafka/
docker compose down
```

## License

[MIT License](./LICENSE)
