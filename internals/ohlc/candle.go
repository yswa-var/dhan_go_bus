package ohlc

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
)

type MarketEvent = marketfeed.MarketEvent

// PriceScale is the fixed-point scale used for candle prices.
const PriceScale int64 = 100000

// Candle is one completed OHLCV interval. Prices are stored as fixed-point
// integers so database writes do not introduce another floating-point round.
type Candle struct {
	SecurityID      int64
	ExchangeSegment string
	Start           time.Time
	Interval        time.Duration

	Open   int64
	High   int64
	Low    int64
	Close  int64
	Volume int64
}

// Aggregator consumes market events and returns only candles whose buckets are
// known to be complete. Empty time buckets are skipped.
type Aggregator interface {
	OnEvent(event MarketEvent) error
	FlushUntil(t time.Time) ([]Candle, error)
}

// ScalePrice converts a feed price into the fixed-point representation used by
// Candle. Zero and negative prices are rejected because they cannot form a
// valid market candle.
func ScalePrice(price float32) (int64, error) {
	if price <= 0 || math.IsNaN(float64(price)) || math.IsInf(float64(price), 0) {
		return 0, fmt.Errorf("invalid market price %v", price)
	}

	scaled := math.Round(float64(price) * float64(PriceScale))
	if scaled > float64(math.MaxInt64) {
		return 0, fmt.Errorf("market price %v exceeds fixed-point range", price)
	}
	return int64(scaled), nil
}

// Timeframe returns the database label for a candle interval.
func Timeframe(interval time.Duration) string {
	if interval%time.Minute == 0 {
		return strconv.FormatInt(int64(interval/time.Minute), 10) + "m"
	}
	return interval.String()
}
