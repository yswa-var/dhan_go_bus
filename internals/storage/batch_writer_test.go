package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"yswa-var/dhan_go_bus/internals/ohlc"
)

func TestBatchWriterFlushesAtBatchSizeAndPreservesOrder(t *testing.T) {
	writer := &recordingWriter{}
	queue := make(CandleQueue, 4)
	batchWriter := NewBatchWriterWithConfig(queue, writer, BatchWriterConfig{
		BatchSize:     2,
		FlushInterval: time.Hour,
		RetryMin:      time.Millisecond,
		RetryMax:      time.Millisecond,
	})

	done := make(chan error, 1)
	go func() { done <- batchWriter.Run(context.Background()) }()
	queue <- candle(1)
	queue <- candle(2)
	close(queue)

	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	got := writer.allCandles()
	if len(got) != 2 || got[0].SecurityID != 1 || got[1].SecurityID != 2 {
		t.Fatalf("written candles = %#v", got)
	}
}

func TestBatchWriterFlushesPartialBatchOnTimer(t *testing.T) {
	writer := &recordingWriter{}
	queue := make(CandleQueue, 1)
	batchWriter := NewBatchWriterWithConfig(queue, writer, BatchWriterConfig{
		BatchSize:     100,
		FlushInterval: 10 * time.Millisecond,
		RetryMin:      time.Millisecond,
		RetryMax:      time.Millisecond,
	})

	done := make(chan error, 1)
	go func() { done <- batchWriter.Run(context.Background()) }()
	queue <- candle(1)

	deadline := time.After(time.Second)
	for len(writer.allCandles()) == 0 {
		select {
		case <-deadline:
			t.Fatal("timer did not flush partial batch")
		case <-time.After(time.Millisecond):
		}
	}
	close(queue)
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestBatchWriterRetriesTheSameBatch(t *testing.T) {
	writer := &recordingWriter{failures: 1}
	queue := make(CandleQueue, 1)
	batchWriter := NewBatchWriterWithConfig(queue, writer, BatchWriterConfig{
		BatchSize:     1,
		FlushInterval: time.Hour,
		RetryMin:      time.Millisecond,
		RetryMax:      time.Millisecond,
	})

	done := make(chan error, 1)
	go func() { done <- batchWriter.Run(context.Background()) }()
	queue <- candle(1)
	close(queue)

	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if writer.callCount() != 2 {
		t.Fatalf("WriteBatch() calls = %d, want 2", writer.callCount())
	}
	if got := writer.allCandles(); len(got) != 1 || got[0].SecurityID != 1 {
		t.Fatalf("written candles = %#v", got)
	}
}

func TestEnqueueHonorsCancellationWhenQueueIsFull(t *testing.T) {
	queue := make(CandleQueue, 1)
	queue <- candle(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Enqueue(ctx, queue, candle(2)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Enqueue() error = %v, want context.Canceled", err)
	}
}

func TestDecimalPriceUsesFiveFixedPointPlaces(t *testing.T) {
	if got := decimalPrice(1234567); got != "12.34567" {
		t.Fatalf("decimalPrice() = %q, want 12.34567", got)
	}
	if got := decimalPrice(-5); got != "-0.00005" {
		t.Fatalf("decimalPrice() = %q, want -0.00005", got)
	}
}

func TestParseDecimalPriceUsesFixedPointScale(t *testing.T) {
	tests := map[string]int64{
		"12.34567": 1234567,
		"100":      100 * ohlc.PriceScale,
		"0.00001":  1,
		"-0.00005": -5,
	}
	for input, want := range tests {
		got, err := parseDecimalPrice(input)
		if err != nil || got != want {
			t.Errorf("parseDecimalPrice(%q) = %d, %v; want %d", input, got, err, want)
		}
	}
}

type recordingWriter struct {
	mu       sync.Mutex
	failures int
	calls    int
	candles  []ohlc.Candle
}

func (w *recordingWriter) WriteBatch(_ context.Context, candles []ohlc.Candle) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.failures > 0 {
		w.failures--
		return errors.New("temporary database outage")
	}
	w.candles = append(w.candles, candles...)
	return nil
}

func (w *recordingWriter) allCandles() []ohlc.Candle {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]ohlc.Candle(nil), w.candles...)
}

func (w *recordingWriter) callCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func candle(securityID int64) ohlc.Candle {
	return ohlc.Candle{
		SecurityID:      securityID,
		ExchangeSegment: "NSE_EQ",
		Start:           time.Unix(securityID, 0).UTC(),
		Interval:        time.Minute,
		Open:            100,
		High:            100,
		Low:             100,
		Close:           100,
	}
}
