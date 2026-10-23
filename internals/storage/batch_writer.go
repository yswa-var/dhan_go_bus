package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"yswa-var/dhan_go_bus/internals/ohlc"
)

const (
	QueueCapacity = 4096
	BatchSize     = 100
	FlushInterval = time.Second
	RetryMin      = 100 * time.Millisecond
	RetryMax      = 5 * time.Second
)

// CandleQueue is intentionally bounded so a database outage eventually
// applies backpressure to the upstream candle producer.
type CandleQueue chan ohlc.Candle

// NewCandleQueue creates the fixed-capacity queue used by the pipeline.
func NewCandleQueue() CandleQueue {
	return make(CandleQueue, QueueCapacity)
}

// Enqueue blocks when the database queue is full instead of dropping a candle.
func Enqueue(ctx context.Context, queue CandleQueue, candle ohlc.Candle) error {
	if ctx == nil {
		return errors.New("candle enqueue context is nil")
	}
	select {
	case queue <- candle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CandleWriter is the storage boundary used by BatchWriter and its tests.
type CandleWriter interface {
	WriteBatch(ctx context.Context, candles []ohlc.Candle) error
}

// BatchWriterConfig controls batching and retry timing.
type BatchWriterConfig struct {
	BatchSize     int
	FlushInterval time.Duration
	RetryMin      time.Duration
	RetryMax      time.Duration
}

// DefaultBatchWriterConfig returns the Quest 004 settings.
func DefaultBatchWriterConfig() BatchWriterConfig {
	return BatchWriterConfig{
		BatchSize:     BatchSize,
		FlushInterval: FlushInterval,
		RetryMin:      RetryMin,
		RetryMax:      RetryMax,
	}
}

// BatchWriter reads FIFO candles and retries failed batches without removing
// them from the in-memory batch until storage confirms success.
type BatchWriter struct {
	queue  <-chan ohlc.Candle
	writer CandleWriter
	config BatchWriterConfig
}

// NewBatchWriter creates a writer with the default Quest 004 settings.
func NewBatchWriter(queue <-chan ohlc.Candle, writer CandleWriter) *BatchWriter {
	return NewBatchWriterWithConfig(queue, writer, DefaultBatchWriterConfig())
}

// NewBatchWriterWithConfig creates a writer with explicit settings.
func NewBatchWriterWithConfig(queue <-chan ohlc.Candle, writer CandleWriter, config BatchWriterConfig) *BatchWriter {
	config = normalizeConfig(config)
	return &BatchWriter{queue: queue, writer: writer, config: config}
}

// Run drains the queue until it is closed. Context cancellation is an explicit
// failure: pending candles remain in memory only until this process exits.
func (w *BatchWriter) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("batch writer context is nil")
	}
	if w.writer == nil {
		return errors.New("batch writer requires a candle writer")
	}

	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()

	batch := make([]ohlc.Candle, 0, w.config.BatchSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case candle, ok := <-w.queue:
			if !ok {
				return w.flush(ctx, batch)
			}
			batch = append(batch, candle)
			if len(batch) == w.config.BatchSize {
				if err := w.flush(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := w.flush(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
}

func (w *BatchWriter) flush(ctx context.Context, batch []ohlc.Candle) error {
	if len(batch) == 0 {
		return nil
	}

	delay := w.config.RetryMin
	for {
		if err := w.writer.WriteBatch(ctx, batch); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return fmt.Errorf("write candle batch stopped: %w", ctx.Err())
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("write candle batch stopped: %w", ctx.Err())
		case <-timer.C:
		}
		if delay < w.config.RetryMax {
			delay *= 2
			if delay > w.config.RetryMax {
				delay = w.config.RetryMax
			}
		}
	}
}

func normalizeConfig(config BatchWriterConfig) BatchWriterConfig {
	defaults := DefaultBatchWriterConfig()
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = defaults.FlushInterval
	}
	if config.RetryMin <= 0 {
		config.RetryMin = defaults.RetryMin
	}
	if config.RetryMax < config.RetryMin {
		config.RetryMax = defaults.RetryMax
	}
	return config
}
