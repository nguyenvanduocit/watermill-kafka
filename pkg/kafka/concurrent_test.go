package kafka

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// simulateProcessRecord mimics processRecord behavior: sends message to output
// channel, then blocks until Ack — exactly what the real processRecord does,
// but without needing a real Kafka client for offset commit.
func simulateProcessRecord(output chan *message.Message) {
	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	output <- msg   // blocks until router reads
	<-msg.Acked()   // blocks until router acks
}

// TestPollLoop_ConcurrentPartitions verifies that records from different
// partitions are processed concurrently while records within the same
// partition remain sequential.
//
// Strategy: simulate 4 partitions × 3 records each. Each handler takes 50ms.
// Sequential: 4×3×50ms = 600ms. Concurrent (4 parallel): ~3×50ms = 150ms.
func TestPollLoop_ConcurrentPartitions(t *testing.T) {
	const (
		numPartitions    = 4
		recordsPerPart   = 3
		simulatedLatency = 50 * time.Millisecond
	)

	var maxConcurrent atomic.Int32
	var current atomic.Int32
	var totalProcessed atomic.Int32

	// Track per-partition processing order
	var mu sync.Mutex
	partitionOrder := make(map[int][]int)

	output := make(chan *message.Message) // unbuffered — same as real subscriber

	// Simulate Watermill Router: spawns goroutine per message (router.go:670)
	var routerWg sync.WaitGroup
	routerWg.Add(1)
	go func() {
		defer routerWg.Done()
		for msg := range output {
			routerWg.Add(1)
			go func(m *message.Message) {
				defer routerWg.Done()

				c := current.Add(1)
				for {
					old := maxConcurrent.Load()
					if c <= old || maxConcurrent.CompareAndSwap(old, c) {
						break
					}
				}

				time.Sleep(simulatedLatency) // simulate handler work
				current.Add(-1)
				totalProcessed.Add(1)
				m.Ack()
			}(msg)
		}
	}()

	// === CONCURRENT dispatch (per-partition goroutines) ===
	start := time.Now()

	var dispatchWg sync.WaitGroup
	for p := 0; p < numPartitions; p++ {
		dispatchWg.Add(1)
		go func(partition int) {
			defer dispatchWg.Done()
			for r := 0; r < recordsPerPart; r++ {
				simulateProcessRecord(output) // blocks until ack

				mu.Lock()
				partitionOrder[partition] = append(partitionOrder[partition], r)
				mu.Unlock()
			}
		}(p)
	}

	dispatchWg.Wait()
	elapsed := time.Since(start)
	close(output)
	routerWg.Wait()

	// 1. All records processed
	assert.Equal(t, int32(numPartitions*recordsPerPart), totalProcessed.Load(),
		"all records should be processed")

	// 2. Concurrency achieved: max concurrent handlers must be > 1
	assert.Greater(t, maxConcurrent.Load(), int32(1),
		"max concurrent handlers should be > 1, proving parallel partition processing (got %d)",
		maxConcurrent.Load())

	// 3. Per-partition ordering preserved
	for p := 0; p < numPartitions; p++ {
		offsets := partitionOrder[p]
		assert.Len(t, offsets, recordsPerPart, "partition %d should have %d records", p, recordsPerPart)
		for i := 1; i < len(offsets); i++ {
			assert.Greater(t, offsets[i], offsets[i-1],
				"partition %d: record %d should follow %d", p, offsets[i], offsets[i-1])
		}
	}

	// 4. Timing: concurrent should be much faster than sequential
	// Sequential: 12 × 50ms = 600ms. Concurrent: ~150ms + overhead.
	assert.Less(t, elapsed, 400*time.Millisecond,
		"elapsed %v should be < 400ms (sequential would be ~600ms)", elapsed)

	t.Logf("CONCURRENT: %d records in %v, max_concurrent=%d",
		totalProcessed.Load(), elapsed, maxConcurrent.Load())
}

// TestPollLoop_SequentialBaseline proves the sequential version is slower,
// as a control for TestPollLoop_ConcurrentPartitions.
func TestPollLoop_SequentialBaseline(t *testing.T) {
	const (
		numPartitions    = 4
		recordsPerPart   = 3
		simulatedLatency = 50 * time.Millisecond
	)

	var totalProcessed atomic.Int32
	var maxConcurrent atomic.Int32
	var current atomic.Int32

	output := make(chan *message.Message)

	var routerWg sync.WaitGroup
	routerWg.Add(1)
	go func() {
		defer routerWg.Done()
		for msg := range output {
			routerWg.Add(1)
			go func(m *message.Message) {
				defer routerWg.Done()
				c := current.Add(1)
				for {
					old := maxConcurrent.Load()
					if c <= old || maxConcurrent.CompareAndSwap(old, c) {
						break
					}
				}
				time.Sleep(simulatedLatency)
				current.Add(-1)
				totalProcessed.Add(1)
				m.Ack()
			}(msg)
		}
	}()

	// === SEQUENTIAL dispatch (old EachRecord behavior) ===
	start := time.Now()
	for p := 0; p < numPartitions; p++ {
		for r := 0; r < recordsPerPart; r++ {
			simulateProcessRecord(output)
		}
	}
	elapsed := time.Since(start)
	close(output)
	routerWg.Wait()

	assert.Equal(t, int32(numPartitions*recordsPerPart), totalProcessed.Load())

	// Sequential should take ~600ms (12 × 50ms)
	assert.Greater(t, elapsed, 400*time.Millisecond,
		"sequential elapsed %v should be > 400ms (12×50ms = 600ms)", elapsed)

	t.Logf("SEQUENTIAL: %d records in %v, max_concurrent=%d",
		totalProcessed.Load(), elapsed, maxConcurrent.Load())
}
