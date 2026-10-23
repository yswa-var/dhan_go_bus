package pipeline

import (
	"context"
	"testing"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
	"yswa-var/dhan_go_bus/internals/storage"
)

func TestPipelineFlushesActiveCandleWhenEventsClose(t *testing.T) {
	events := make(chan marketfeed.MarketEvent, 1)
	start := time.Date(2026, 10, 23, 10, 4, 15, 0, time.UTC)
	events <- marketfeed.Tick{
		SecurityID:      "1",
		ExchangeSegment: "NSE_EQ",
		LTP:             100,
		Timestamp:       start,
	}
	close(events)

	queue := make(storage.CandleQueue, 1)
	p, err := NewCandlePipeline(events, ohlc.NewMinuteAggregator(), queue)
	if err != nil {
		t.Fatalf("NewCandlePipeline() error = %v", err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	candle, ok := <-queue
	if !ok {
		t.Fatal("pipeline closed without emitting the active candle")
	}
	if candle.SecurityID != 1 || candle.Open != 100*ohlc.PriceScale || candle.Close != 100*ohlc.PriceScale {
		t.Fatalf("candle = %#v", candle)
	}
	if _, ok := <-queue; ok {
		t.Fatal("pipeline queue is still open")
	}
}

func TestPipelinePublishesCompletedCandlesBeforePersistence(t *testing.T) {
	events := make(chan marketfeed.MarketEvent, 2)
	start := time.Date(2026, 10, 23, 10, 4, 0, 0, time.UTC)
	events <- marketfeed.Tick{SecurityID: "1", ExchangeSegment: "NSE_EQ", LTP: 100, Timestamp: start}
	events <- marketfeed.Tick{SecurityID: "1", ExchangeSegment: "NSE_EQ", LTP: 101, Timestamp: start.Add(time.Minute)}
	close(events)

	var published []ohlc.Candle
	p, err := NewCandlePipelineWithPublisher(events, ohlc.NewMinuteAggregator(), func(_ context.Context, candle ohlc.Candle) error {
		published = append(published, candle)
		return nil
	})
	if err != nil {
		t.Fatalf("NewCandlePipelineWithPublisher() error = %v", err)
	}
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(published) != 2 || published[0].Close != 100*ohlc.PriceScale || published[1].Close != 101*ohlc.PriceScale {
		t.Fatalf("published candles = %#v", published)
	}
}
