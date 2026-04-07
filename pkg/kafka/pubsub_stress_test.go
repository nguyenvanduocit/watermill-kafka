//go:build stress
// +build stress

package kafka_test

import (
	"runtime"
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func init() {
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}

func TestPublishSubscribe_stress(t *testing.T) {
	skipIfNoKafka(t)
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:      true,
			ExactlyOnceDelivery: false,
			GuaranteedOrder:     false,
			Persistent:          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_ordered_stress(t *testing.T) {
	skipIfNoKafka(t)
	tests.TestPubSubStressTest(
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
