package ohlc

import (
	"testing"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
)

func TestAggregatorBuildsOHLCInOrder(t *testing.T) {
	aggregator := NewMinuteAggregator()
	start := time.Date(2026, 10, 23, 10, 4, 0, 0, time.FixedZone("IST", 5*60*60+30*60))

	for i, price := range []float32{100, 105, 98, 102} {
		if err := aggregator.OnEvent(tick("2885", "NSE_EQ", price, start.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatalf("OnEvent() error = %v", err)
		}
	}

	candles, err := aggregator.FlushUntil(start.Add(time.Minute))
	if err != nil {
		t.Fatalf("FlushUntil() error = %v", err)
	}
	if len(candles) != 1 {
		t.Fatalf("got %d candles, want 1", len(candles))
	}
	got := candles[0]
	wantStart := start.UTC().Truncate(time.Minute)
	if got.Start != wantStart || got.Open != 100*PriceScale || got.High != 105*PriceScale || got.Low != 98*PriceScale || got.Close != 102*PriceScale {
		t.Fatalf("candle = %#v", got)
	}
}

func TestAggregatorKeepsSymbolsIndependent(t *testing.T) {
	aggregator := NewMinuteAggregator()
	start := time.Date(2026, 10, 23, 10, 4, 30, 0, time.UTC)

	for _, event := range []marketfeed.MarketEvent{
		tick("1", "NSE_EQ", 100, start),
		tick("2", "NSE_EQ", 200, start),
	} {
		if err := aggregator.OnEvent(event); err != nil {
			t.Fatalf("OnEvent() error = %v", err)
		}
	}

	candles, err := aggregator.FlushUntil(start.Add(time.Minute))
	if err != nil {
		t.Fatalf("FlushUntil() error = %v", err)
	}
	if len(candles) != 2 || candles[0].SecurityID != 1 || candles[1].SecurityID != 2 {
		t.Fatalf("candles = %#v", candles)
	}
}

func TestAggregatorUsesUTCMinuteBoundaries(t *testing.T) {
	aggregator := NewMinuteAggregator()
	first := time.Date(2026, 10, 23, 10, 4, 59, 0, time.FixedZone("IST", 5*60*60+30*60))
	second := first.Add(time.Second)

	if err := aggregator.OnEvent(tick("1", "NSE_EQ", 100, first)); err != nil {
		t.Fatalf("first OnEvent() error = %v", err)
	}
	if err := aggregator.OnEvent(tick("1", "NSE_EQ", 101, second)); err != nil {
		t.Fatalf("second OnEvent() error = %v", err)
	}

	candles, err := aggregator.FlushUntil(second)
	if err != nil {
		t.Fatalf("FlushUntil() error = %v", err)
	}
	if len(candles) != 1 || !candles[0].Start.Equal(first.UTC().Truncate(time.Minute)) {
		t.Fatalf("candles = %#v", candles)
	}
	if !candles[0].Start.Equal(time.Date(2026, 10, 23, 4, 34, 0, 0, time.UTC)) {
		t.Fatalf("candle start = %v", candles[0].Start)
	}
}

func TestAggregatorUsesCumulativeVolumeDeltas(t *testing.T) {
	aggregator := NewMinuteAggregator()
	start := time.Date(2026, 10, 23, 10, 4, 0, 0, time.UTC)

	for _, volume := range []uint32{100, 105, 105} {
		event := marketfeed.Quote{
			ExchangeSegment: "NSE_EQ",
			SecurityID:      "1",
			LTP:             100,
			Volume:          volume,
			Timestamp:       start,
		}
		if err := aggregator.OnEvent(event); err != nil {
			t.Fatalf("OnEvent() error = %v", err)
		}
	}
	if err := aggregator.OnEvent(marketfeed.Depth{
		ExchangeSegment: "NSE_EQ",
		SecurityID:      "1",
		LTP:             100,
		Levels:          []marketfeed.DepthLevel{{BidPrice: 99, AskPrice: 101}},
	}); err != nil {
		t.Fatalf("depth OnEvent() error = %v", err)
	}

	candles, err := aggregator.FlushUntil(start.Add(time.Minute))
	if err != nil {
		t.Fatalf("FlushUntil() error = %v", err)
	}
	if len(candles) != 1 || candles[0].Volume != 5 {
		t.Fatalf("candles = %#v", candles)
	}
}

func TestAggregatorSkipsEmptyMinutesAndIgnoresLateEvents(t *testing.T) {
	aggregator := NewMinuteAggregator()
	start := time.Date(2026, 10, 23, 10, 4, 0, 0, time.UTC)
	if err := aggregator.OnEvent(tick("1", "NSE_EQ", 100, start)); err != nil {
		t.Fatalf("first OnEvent() error = %v", err)
	}
	if err := aggregator.OnEvent(tick("1", "NSE_EQ", 102, start.Add(2*time.Minute))); err != nil {
		t.Fatalf("second OnEvent() error = %v", err)
	}
	if _, err := aggregator.FlushUntil(start.Add(3 * time.Minute)); err != nil {
		t.Fatalf("FlushUntil() error = %v", err)
	}

	if err := aggregator.OnEvent(tick("1", "NSE_EQ", 99, start.Add(time.Minute))); err != nil {
		t.Fatalf("late OnEvent() error = %v", err)
	}
	candles, err := aggregator.FlushUntil(start.Add(4 * time.Minute))
	if err != nil {
		t.Fatalf("second FlushUntil() error = %v", err)
	}
	if len(candles) != 0 {
		t.Fatalf("late event reopened a candle: %#v", candles)
	}
}

func TestScalePriceRejectsInvalidPrices(t *testing.T) {
	for _, price := range []float32{0, -1} {
		if _, err := ScalePrice(price); err == nil {
			t.Errorf("ScalePrice(%v) succeeded", price)
		}
	}
}

func tick(securityID, segment string, price float32, timestamp time.Time) marketfeed.Tick {
	return marketfeed.Tick{
		SecurityID:      securityID,
		ExchangeSegment: segment,
		LTP:             price,
		Timestamp:       timestamp,
	}
}
