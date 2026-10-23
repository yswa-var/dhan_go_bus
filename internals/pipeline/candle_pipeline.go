package pipeline

import (
	"context"
	"errors"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
	"yswa-var/dhan_go_bus/internals/storage"
)

// CandlePipeline turns market events into completed candles and publishes them
// to independent consumers such as storage and indicators.
type CandlePipeline struct {
	events       <-chan marketfeed.MarketEvent
	aggregator   ohlc.Aggregator
	publish      func(context.Context, ohlc.Candle) error
	closePublish func()
}

// NewCandlePipeline creates the aggregation side of the candle pipeline.
func NewCandlePipeline(events <-chan marketfeed.MarketEvent, aggregator ohlc.Aggregator, queue storage.CandleQueue) (*CandlePipeline, error) {
	if queue == nil {
		return nil, errors.New("candle pipeline queue is required")
	}
	return newCandlePipeline(events, aggregator, func(ctx context.Context, candle ohlc.Candle) error {
		return storage.Enqueue(ctx, queue, candle)
	}, func() { close(queue) })
}

// NewCandlePipelineWithPublisher sends each completed candle to a broadcast
// bus or another consumer. The publisher owns its lifecycle and is not closed
// by the pipeline.
func NewCandlePipelineWithPublisher(events <-chan marketfeed.MarketEvent, aggregator ohlc.Aggregator, publish func(context.Context, ohlc.Candle) error) (*CandlePipeline, error) {
	return newCandlePipeline(events, aggregator, publish, nil)
}

func newCandlePipeline(events <-chan marketfeed.MarketEvent, aggregator ohlc.Aggregator, publish func(context.Context, ohlc.Candle) error, closePublish func()) (*CandlePipeline, error) {
	if events == nil {
		return nil, errors.New("candle pipeline event channel is required")
	}
	if aggregator == nil {
		return nil, errors.New("candle pipeline aggregator is required")
	}
	if publish == nil {
		return nil, errors.New("candle pipeline publisher is required")
	}
	return &CandlePipeline{events: events, aggregator: aggregator, publish: publish, closePublish: closePublish}, nil
}

// Run consumes events until the input closes or ctx is cancelled. The queue is
// closed by the pipeline because it is the only producer.
func (p *CandlePipeline) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("candle pipeline context is nil")
	}
	if p.closePublish != nil {
		defer p.closePublish()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-p.events:
			if !ok {
				return p.flushAndPublish(ctx, maxTime())
			}
			if err := p.aggregator.OnEvent(event); err != nil {
				return err
			}
			if timestamp, ok := eventTimestamp(event); ok {
				if err := p.flushAndPublish(ctx, timestamp); err != nil {
					return err
				}
			}
		}
	}
}

func (p *CandlePipeline) flushAndPublish(ctx context.Context, until time.Time) error {
	candles, err := p.aggregator.FlushUntil(until)
	if err != nil {
		return err
	}
	for _, candle := range candles {
		if err := p.publish(ctx, candle); err != nil {
			return err
		}
	}
	return nil
}

func eventTimestamp(event marketfeed.MarketEvent) (time.Time, bool) {
	switch value := event.(type) {
	case marketfeed.Tick:
		return value.Timestamp, true
	case marketfeed.Quote:
		return value.Timestamp, true
	default:
		return time.Time{}, false
	}
}

func maxTime() time.Time {
	return time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
}
