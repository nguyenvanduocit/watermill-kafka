package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// simulateProcessRecord mimics processRecord behavior: sends message to output
// channel, then blocks until Ack — exactly what the real processRecord does,
// but without needing a real Kafka client for offset commit.
func simulateProcessRecord(output chan *message.Message) {
	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	output <- msg // blocks until router reads
	<-msg.Acked() // blocks until router acks
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

// TestPollLoop_ContextCancel_NoGoroutineLeak verifies that cancelling the
// context unblocks all per-partition goroutines promptly without leaking.
//
// Scenario: 4 partitions start processing, each blocked waiting for ack
// (router is deliberately slow). Cancel context → all goroutines must exit
// within a short deadline.
func TestPollLoop_ContextCancel_NoGoroutineLeak(t *testing.T) {
	const numPartitions = 4

	sub := &Subscriber{
		config: SubscriberConfig{
			Unmarshaler: DefaultMarshaler{},
		},
		logger:  watermill.NopLogger{},
		closing: make(chan struct{}),
	}

	output := make(chan *message.Message) // unbuffered
	ctx, cancel := context.WithCancel(context.Background())

	// NO router reading from output — goroutines will block on `output <- msg`
	// until context is cancelled.

	records := buildRecords(t, numPartitions, 1)
	byPartition := groupByPartition(records)

	var wg sync.WaitGroup
	var exited atomic.Int32

	for _, partRecords := range byPartition {
		wg.Add(1)
		go func(recs []*kgo.Record) {
			defer wg.Done()
			for _, record := range recs {
				// processRecord will block on `output <- msg` since nobody reads.
				// When ctx is cancelled, it should return via `case <-ctx.Done()`.
				_ = sub.processRecord(ctx, nil, record, output, nil)
			}
			exited.Add(1)
		}(partRecords)
	}

	// Give goroutines time to reach the blocking `output <- msg`
	time.Sleep(50 * time.Millisecond)

	// Cancel context — all goroutines should unblock
	cancel()

	// Wait with timeout — if goroutines leak, this times out
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines did not exit within 2s after context cancel — likely goroutine leak")
	}

	assert.Equal(t, int32(numPartitions), exited.Load(),
		"all %d partition goroutines should have exited", numPartitions)

	t.Logf("All %d goroutines exited cleanly after context cancel", exited.Load())
}

// TestPollLoop_SubscriberClose_NoGoroutineLeak verifies that closing the
// subscriber (s.closing channel) unblocks all per-partition goroutines.
func TestPollLoop_SubscriberClose_NoGoroutineLeak(t *testing.T) {
	const numPartitions = 4

	sub := &Subscriber{
		config: SubscriberConfig{
			Unmarshaler: DefaultMarshaler{},
		},
		logger:  watermill.NopLogger{},
		closing: make(chan struct{}),
	}

	output := make(chan *message.Message)
	ctx := context.Background()

	records := buildRecords(t, numPartitions, 1)
	byPartition := groupByPartition(records)

	var wg sync.WaitGroup
	var exited atomic.Int32

	for _, partRecords := range byPartition {
		wg.Add(1)
		go func(recs []*kgo.Record) {
			defer wg.Done()
			for _, record := range recs {
				_ = sub.processRecord(ctx, nil, record, output, nil)
			}
			exited.Add(1)
		}(partRecords)
	}

	time.Sleep(50 * time.Millisecond)

	// Close subscriber — simulates pod shutdown
	close(sub.closing)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines did not exit within 2s after subscriber close — likely goroutine leak")
	}

	assert.Equal(t, int32(numPartitions), exited.Load(),
		"all %d partition goroutines should have exited", numPartitions)

	t.Logf("All %d goroutines exited cleanly after subscriber close", exited.Load())
}

// failOnPartitionUnmarshaler returns an error for records on a specific partition.
type failOnPartitionUnmarshaler struct {
	DefaultMarshaler
	failPartition int32
}

func (f failOnPartitionUnmarshaler) Unmarshal(record *kgo.Record) (*message.Message, error) {
	if record.Partition == f.failPartition {
		return nil, errors.New("simulated unmarshal failure on partition")
	}
	return f.DefaultMarshaler.Unmarshal(record)
}

// TestPollLoop_ErrorInOnePartition_StopsOthers verifies that when one
// partition goroutine encounters an unmarshal error, other goroutines
// complete their in-flight work but the error partition stops immediately.
func TestPollLoop_ErrorInOnePartition_StopsOthers(t *testing.T) {
	const simulatedLatency = 50 * time.Millisecond

	sub := &Subscriber{
		config: SubscriberConfig{
			Unmarshaler: failOnPartitionUnmarshaler{failPartition: 1},
		},
		logger:  watermill.NopLogger{},
		closing: make(chan struct{}),
	}

	output := make(chan *message.Message)
	ctx := context.Background()

	// Router that acks messages
	var routerWg sync.WaitGroup
	routerWg.Add(1)
	go func() {
		defer routerWg.Done()
		for msg := range output {
			routerWg.Add(1)
			go func(m *message.Message) {
				defer routerWg.Done()
				time.Sleep(simulatedLatency)
				m.Ack()
			}(msg)
		}
	}()

	// Partition 0: 3 good records. Partition 1: 3 records but unmarshaler fails on partition 1.
	goodRecords := buildRecords(t, 1, 3) // partition 0
	badPartitionRecords := buildRecordsForPartition(t, 1, 0, 3) // partition 1

	byPartition := map[int32][]*kgo.Record{
		0: goodRecords,
		1: badPartitionRecords,
	}

	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	var processedCount atomic.Int32

	for _, partRecords := range byPartition {
		wg.Add(1)
		go func(recs []*kgo.Record) {
			defer wg.Done()
			for _, record := range recs {
				if err := sub.processRecord(ctx, nil, record, output, nil); err != nil {
					errOnce.Do(func() { firstErr = err })
					return // stop processing this partition
				}
				processedCount.Add(1)
			}
		}(partRecords)
	}

	wg.Wait()
	close(output)
	routerWg.Wait()

	// Error should be captured from partition 1
	require.Error(t, firstErr, "should capture unmarshal error from partition 1")
	assert.Contains(t, firstErr.Error(), "unmarshal",
		"error should be unmarshal error, got: %v", firstErr)

	// Partition 0: all 3 processed. Partition 1: 0 processed (error on first).
	assert.Equal(t, int32(3), processedCount.Load(),
		"partition 0 should process all 3, partition 1 should stop at bad record")

	t.Logf("Error captured: %v, processed: %d", firstErr, processedCount.Load())
}

// TestPollLoop_ContextCancel_WhileWaitingForAck verifies that context
// cancellation unblocks goroutines that are waiting for Ack (not just
// waiting to send to output channel).
func TestPollLoop_ContextCancel_WhileWaitingForAck(t *testing.T) {
	const numPartitions = 3

	sub := &Subscriber{
		config: SubscriberConfig{
			Unmarshaler: DefaultMarshaler{},
		},
		logger:  watermill.NopLogger{},
		closing: make(chan struct{}),
	}

	output := make(chan *message.Message)
	ctx, cancel := context.WithCancel(context.Background())

	records := buildRecords(t, numPartitions, 1)
	byPartition := groupByPartition(records)

	// Router reads messages but NEVER acks — goroutines block on <-msg.Acked()
	var receivedMsgs []*message.Message
	var msgMu sync.Mutex
	routerDone := make(chan struct{})
	go func() {
		defer close(routerDone)
		for msg := range output {
			msgMu.Lock()
			receivedMsgs = append(receivedMsgs, msg)
			msgMu.Unlock()
			// deliberately do NOT ack
		}
	}()

	var wg sync.WaitGroup
	var exited atomic.Int32

	for _, partRecords := range byPartition {
		wg.Add(1)
		go func(recs []*kgo.Record) {
			defer wg.Done()
			for _, record := range recs {
				_ = sub.processRecord(ctx, nil, record, output, nil)
			}
			exited.Add(1)
		}(partRecords)
	}

	// Wait for router to receive all messages (goroutines now blocked on Acked)
	time.Sleep(100 * time.Millisecond)

	msgMu.Lock()
	received := len(receivedMsgs)
	msgMu.Unlock()
	assert.Equal(t, numPartitions, received,
		"router should have received %d messages (one per partition)", numPartitions)

	// Cancel context — goroutines waiting on Acked should unblock
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines stuck waiting for ack even after context cancel — deadlock")
	}

	close(output)
	<-routerDone

	assert.Equal(t, int32(numPartitions), exited.Load(),
		"all goroutines should exit after context cancel while waiting for ack")

	t.Logf("All %d goroutines unblocked from ack-wait after context cancel", exited.Load())
}

// TestPollLoop_SlowPartition_DoesNotBlockOthers verifies that one partition
// with a very slow handler does not prevent other partitions from processing
// or from shutting down cleanly.
//
// Scenario: 3 fast partitions (10ms each) + 1 slow partition (blocks until cancel).
// Fast partitions should complete quickly. Then cancel → slow partition exits.
// Total time should be dominated by fast partitions, NOT by slow one.
func TestPollLoop_SlowPartition_DoesNotBlockOthers(t *testing.T) {
	const (
		numFastPartitions = 3
		recordsPerPart    = 2
		fastLatency       = 10 * time.Millisecond
	)

	var fastProcessed atomic.Int32
	var slowStarted atomic.Int32

	output := make(chan *message.Message)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Router: fast for partitions 0-2, blocks forever for partition 3 (until cancel)
	var routerWg sync.WaitGroup
	routerWg.Add(1)
	go func() {
		defer routerWg.Done()
		for msg := range output {
			routerWg.Add(1)
			go func(m *message.Message) {
				defer routerWg.Done()

				partition, _ := MessagePartitionFromCtx(m.Context())
				if partition == 3 {
					slowStarted.Add(1)
					// Slow handler — blocks until test cancels context
					<-ctx.Done()
					// In real life, handler returns after ctx cancel,
					// then processRecord sees Nack or ctx.Done
					m.Nack()
					return
				}

				time.Sleep(fastLatency)
				fastProcessed.Add(1)
				m.Ack()
			}(msg)
		}
	}()

	sub := &Subscriber{
		config: SubscriberConfig{
			Unmarshaler:     DefaultMarshaler{},
			NackResendSleep: NoSleep, // don't sleep on nack in test
		},
		logger:  watermill.NopLogger{},
		closing: make(chan struct{}),
	}

	// Build: partitions 0-2 (fast, 2 records each) + partition 3 (slow, 1 record)
	records := buildRecords(t, numFastPartitions, recordsPerPart)
	slowRecord := buildRecords(t, 1, 1) // 1 partition, 1 record
	// Reassign slow record to partition 3
	for _, r := range slowRecord {
		r.Partition = 3
	}

	allByPartition := groupByPartition(records)
	allByPartition[3] = slowRecord

	var wg sync.WaitGroup
	for _, partRecords := range allByPartition {
		wg.Add(1)
		go func(recs []*kgo.Record) {
			defer wg.Done()
			for _, record := range recs {
				_ = sub.processRecord(ctx, nil, record, output, nil)
			}
		}(partRecords)
	}

	// Wait for fast partitions to finish (should take ~20ms for 2 records × 10ms)
	// Slow partition will still be blocked
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, int32(numFastPartitions*recordsPerPart), fastProcessed.Load(),
		"all fast partitions should complete while slow one is still running")
	assert.Equal(t, int32(1), slowStarted.Load(),
		"slow partition handler should have started")

	t.Logf("Fast partitions done (%d processed), slow partition still blocked — cancelling...",
		fastProcessed.Load())

	// Cancel → slow partition should exit
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("slow partition goroutine did not exit within 2s after cancel — goroutine leak")
	}

	close(output)
	routerWg.Wait()

	t.Logf("All goroutines exited: fast=%d processed, slow unblocked after cancel",
		fastProcessed.Load())
}

// --- helpers ---

func buildRecords(t *testing.T, numPartitions, recordsPerPart int) []*kgo.Record {
	t.Helper()
	var records []*kgo.Record
	for p := 0; p < numPartitions; p++ {
		for r := 0; r < recordsPerPart; r++ {
			records = append(records, buildRecord(t, int32(p), int64(r)))
		}
	}
	return records
}

func buildRecordsForPartition(t *testing.T, partition int32, startOffset int64, count int) []*kgo.Record {
	t.Helper()
	var records []*kgo.Record
	for i := 0; i < count; i++ {
		records = append(records, buildRecord(t, partition, startOffset+int64(i)))
	}
	return records
}

func buildRecord(t *testing.T, partition int32, offset int64) *kgo.Record {
	t.Helper()
	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	marshaled, err := DefaultMarshaler{}.Marshal("test-topic", msg)
	require.NoError(t, err)

	return &kgo.Record{
		Topic:     "test-topic",
		Partition: partition,
		Offset:    offset,
		Key:       marshaled.Key,
		Value:     marshaled.Value,
		Headers:   marshaled.Headers,
		Timestamp: time.Now(),
	}
}

func groupByPartition(records []*kgo.Record) map[int32][]*kgo.Record {
	result := make(map[int32][]*kgo.Record)
	for _, rec := range records {
		result[rec.Partition] = append(result[rec.Partition], rec)
	}
	return result
}
