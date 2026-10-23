package ohlc

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
)

// CandleAggregator keeps one current bucket per instrument. It is intended to
// be called by one pipeline goroutine, which keeps event ordering explicit.
type CandleAggregator struct {
	interval time.Duration
	active   map[instrumentKey]*Candle

	// finalized prevents a late event from reopening a bucket already emitted.
	finalized map[instrumentKey]time.Time
	volumes   map[instrumentKey]uint64
	pending   []Candle
}

type instrumentKey struct {
	securityID      int64
	exchangeSegment string
}

// NewAggregator creates an aggregator for the requested interval.
func NewAggregator(interval time.Duration) (*CandleAggregator, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("candle interval must be positive")
	}
	return &CandleAggregator{
		interval:  interval,
		active:    make(map[instrumentKey]*Candle),
		finalized: make(map[instrumentKey]time.Time),
		volumes:   make(map[instrumentKey]uint64),
	}, nil
}

// NewMinuteAggregator creates the Quest 004 one-minute aggregator.
func NewMinuteAggregator() *CandleAggregator {
	aggregator, err := NewAggregator(time.Minute)
	if err != nil {
		panic(err)
	}
	return aggregator
}

func (a *CandleAggregator) OnEvent(event marketfeed.MarketEvent) error {
	data, handled, err := extractEvent(event, a.interval)
	if err != nil || !handled {
		return err
	}

	key := instrumentKey{securityID: data.securityID, exchangeSegment: data.exchangeSegment}
	if finalized, ok := a.finalized[key]; ok && !data.start.After(finalized) {
		// The feed can deliver late packets. Ignoring them prevents a candle
		// already handed to storage from being emitted a second time.
		return nil
	}

	active := a.active[key]
	switch {
	case active == nil:
		a.active[key] = newCandle(key, data.start, a.interval, data.price)
	case data.start.Equal(active.Start):
		updateCandle(active, data.price)
	case data.start.After(active.Start):
		a.complete(key)
		a.active[key] = newCandle(key, data.start, a.interval, data.price)
	default:
		return nil
	}

	if data.hasVolume {
		a.active[key].Volume += a.volumeDelta(key, data.cumulativeVolume)
	}
	return nil
}

// FlushUntil returns pending candles and finalizes active buckets whose end is
// at or before t. A later event also proves earlier buckets complete, so those
// buckets are returned immediately on the next flush.
func (a *CandleAggregator) FlushUntil(t time.Time) ([]Candle, error) {
	cutoff := t.UTC()
	completed := append([]Candle(nil), a.pending...)
	a.pending = a.pending[:0]

	for key, candle := range a.active {
		if !candle.Start.Add(a.interval).After(cutoff) {
			completed = append(completed, *candle)
			delete(a.active, key)
			a.finalized[key] = candle.Start
		}
	}

	sort.Slice(completed, func(i, j int) bool {
		if completed[i].Start.Equal(completed[j].Start) {
			if completed[i].ExchangeSegment == completed[j].ExchangeSegment {
				return completed[i].SecurityID < completed[j].SecurityID
			}
			return completed[i].ExchangeSegment < completed[j].ExchangeSegment
		}
		return completed[i].Start.Before(completed[j].Start)
	})
	return completed, nil
}

type eventData struct {
	securityID       int64
	exchangeSegment  string
	start            time.Time
	price            int64
	cumulativeVolume uint64
	hasVolume        bool
}

func extractEvent(event marketfeed.MarketEvent, interval time.Duration) (eventData, bool, error) {
	var securityID, exchangeSegment string
	var price float32
	var timestamp time.Time
	var cumulativeVolume uint64
	var hasVolume bool

	switch value := event.(type) {
	case marketfeed.Tick:
		securityID = value.SecurityID
		exchangeSegment = value.ExchangeSegment
		price = value.LTP
		timestamp = value.Timestamp
		if value.Volume > 0 {
			cumulativeVolume = uint64(value.Volume)
			hasVolume = true
		}
	case marketfeed.Quote:
		securityID = value.SecurityID
		exchangeSegment = value.ExchangeSegment
		price = value.LTP
		timestamp = value.Timestamp
		if value.Volume > 0 {
			cumulativeVolume = uint64(value.Volume)
			hasVolume = true
		}
	case marketfeed.Depth:
		// Depth describes the book, not a trade or quote price observation.
		return eventData{}, false, nil
	default:
		return eventData{}, false, fmt.Errorf("unsupported market event %T", event)
	}

	if securityID == "" || exchangeSegment == "" {
		return eventData{}, false, fmt.Errorf("market event is missing instrument identity")
	}
	parsedID, err := strconv.ParseInt(securityID, 10, 64)
	if err != nil || parsedID < 0 {
		return eventData{}, false, fmt.Errorf("invalid market security ID %q", securityID)
	}
	if timestamp.IsZero() {
		return eventData{}, false, fmt.Errorf("market event has no timestamp")
	}
	if interval <= 0 {
		return eventData{}, false, fmt.Errorf("candle interval must be positive")
	}
	priceValue, err := ScalePrice(price)
	if err != nil {
		return eventData{}, false, err
	}

	return eventData{
		securityID:       parsedID,
		exchangeSegment:  exchangeSegment,
		start:            timestamp.UTC().Truncate(interval),
		price:            priceValue,
		cumulativeVolume: cumulativeVolume,
		hasVolume:        hasVolume,
	}, true, nil
}

func newCandle(key instrumentKey, start time.Time, interval time.Duration, price int64) *Candle {
	return &Candle{
		SecurityID:      key.securityID,
		ExchangeSegment: key.exchangeSegment,
		Start:           start,
		Interval:        interval,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
	}
}

func updateCandle(candle *Candle, price int64) {
	if price > candle.High {
		candle.High = price
	}
	if price < candle.Low {
		candle.Low = price
	}
	candle.Close = price
}

func (a *CandleAggregator) complete(key instrumentKey) {
	completed := a.active[key]
	a.pending = append(a.pending, *completed)
	delete(a.active, key)
	a.finalized[key] = completed.Start
}

func (a *CandleAggregator) volumeDelta(key instrumentKey, current uint64) int64 {
	previous, exists := a.volumes[key]
	a.volumes[key] = current
	if !exists || current < previous {
		// The first snapshot includes earlier trading, and a decrease indicates
		// a feed reset. Neither should inflate the current minute.
		return 0
	}
	return int64(current - previous)
}
