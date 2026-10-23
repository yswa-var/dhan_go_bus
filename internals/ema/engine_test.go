package ema

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"yswa-var/dhan_go_bus/internals/ohlc"
)

func TestEMA20InitializesFromSMAAndThenUpdatesIncrementally(t *testing.T) {
	engine := NewEMA20Engine()
	start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

	for index := 0; index < 19; index++ {
		if _, emitted, err := engine.OnCandle(testCandle(1, float64(index+1), start.Add(time.Duration(index)*time.Minute))); err != nil || emitted {
			t.Fatalf("seed candle %d: emitted=%v err=%v", index+1, emitted, err)
		}
	}

	update, emitted, err := engine.OnCandle(testCandle(1, 20, start.Add(19*time.Minute)))
	if err != nil || !emitted {
		t.Fatalf("20th candle: emitted=%v err=%v", emitted, err)
	}
	if !almostEqual(update.Value, 10.5) {
		t.Fatalf("SMA20 = %.12f, want 10.5", update.Value)
	}

	update, emitted, err = engine.OnCandle(testCandle(1, 21, start.Add(20*time.Minute)))
	if err != nil || !emitted {
		t.Fatalf("21st candle: emitted=%v err=%v", emitted, err)
	}
	want := (2.0/21.0)*21 + (19.0/21.0)*10.5
	if !almostEqual(update.Value, want) {
		t.Fatalf("EMA21 = %.12f, want %.12f", update.Value, want)
	}
}

func TestEMAStatesAreIndependentPerInstrument(t *testing.T) {
	engine := NewEMA20Engine()
	start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	for index := 0; index < 20; index++ {
		for securityID, close := range map[int64]float64{1: 10, 2: 100} {
			if _, _, err := engine.OnCandle(testCandle(securityID, close, start.Add(time.Duration(index)*time.Minute))); err != nil {
				t.Fatalf("security %d candle %d: %v", securityID, index, err)
			}
		}
	}

	first, firstOK := engine.State(1)
	second, secondOK := engine.State(2)
	if !firstOK || !secondOK || !almostEqual(first.Value, 10) || !almostEqual(second.Value, 100) {
		t.Fatalf("states = %#v, %#v", first, second)
	}
}

func TestEMARejectsOlderAndIgnoresDuplicateCandles(t *testing.T) {
	engine := NewEMA20Engine()
	start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	for index := 0; index < 20; index++ {
		if _, _, err := engine.OnCandle(testCandle(1, 100, start.Add(time.Duration(index)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}

	before, _ := engine.State(1)
	if _, emitted, err := engine.OnCandle(testCandle(1, 999, before.LastTimestamp)); err != nil || emitted {
		t.Fatalf("duplicate: emitted=%v err=%v", emitted, err)
	}
	after, _ := engine.State(1)
	if after != before {
		t.Fatalf("duplicate changed state from %#v to %#v", before, after)
	}
	if _, _, err := engine.OnCandle(testCandle(1, 100, before.LastTimestamp.Add(-time.Minute))); err == nil || !strings.Contains(err.Error(), ErrOutOfOrder.Error()) {
		t.Fatalf("older candle error = %v", err)
	}
}

func TestEMATracksGapsWithoutResettingObservedCandleSeed(t *testing.T) {
	engine := NewEMA20Engine()
	start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	for index := 0; index < 19; index++ {
		if _, _, err := engine.OnCandle(testCandle(1, 100, start.Add(time.Duration(index)*2*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if _, emitted, err := engine.OnCandle(testCandle(1, 100, start.Add(38*time.Minute))); err != nil || !emitted {
		t.Fatalf("20th observed candle: emitted=%v err=%v", emitted, err)
	}
	state, _ := engine.State(1)
	if state.MissingIntervals != 19 {
		t.Fatalf("missing intervals = %d, want 19", state.MissingIntervals)
	}
}

func TestEMARecoveryReconstructsStateBeforeLiveCandles(t *testing.T) {
	start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	history := make([]ohlc.Candle, 0, 20)
	for index := 0; index < 20; index++ {
		history = append(history, testCandle(7, float64(index+1), start.Add(time.Duration(index)*time.Minute)))
	}
	requestedLimit := -1
	loader := fakeLoader{candles: history, requestedLimit: &requestedLimit}
	engine := NewEMA20Engine()
	if err := engine.Recover(context.Background(), loader, []Instrument{{SecurityID: 7, ExchangeSegment: "NSE_EQ", Interval: time.Minute}}); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if requestedLimit != 0 {
		t.Fatalf("recovery requested limit = %d, want full history (0)", requestedLimit)
	}
	state, ok := engine.State(7)
	if !ok || !state.Initialized || !almostEqual(state.Value, 10.5) || !state.LastTimestamp.Equal(history[len(history)-1].Start) {
		t.Fatalf("recovered state = %#v, exists=%v", state, ok)
	}
	if _, emitted, err := engine.OnCandle(testCandle(7, 21, start.Add(20*time.Minute))); err != nil || !emitted {
		t.Fatalf("live candle after recovery: emitted=%v err=%v", emitted, err)
	}
}

func TestEMARejectsInvalidCandle(t *testing.T) {
	engine := NewEMA20Engine()
	if _, _, err := engine.OnCandle(ohlc.Candle{SecurityID: 1, Close: 0, Start: time.Now()}); err == nil || !strings.Contains(err.Error(), ErrInvalidCandle.Error()) {
		t.Fatalf("invalid candle error = %v", err)
	}
}

type fakeLoader struct {
	candles        []ohlc.Candle
	requestedLimit *int
}

func (f fakeLoader) LoadRecentCandles(_ context.Context, _ int64, _ string, _ time.Duration, limit int) ([]ohlc.Candle, error) {
	if f.requestedLimit != nil {
		*f.requestedLimit = limit
	}
	return append([]ohlc.Candle(nil), f.candles...), nil
}

func testCandle(securityID int64, close float64, start time.Time) ohlc.Candle {
	return ohlc.Candle{
		SecurityID:      securityID,
		ExchangeSegment: "NSE_EQ",
		Start:           start,
		Interval:        time.Minute,
		Close:           int64(math.Round(close * float64(ohlc.PriceScale))),
	}
}

func almostEqual(left, right float64) bool {
	return math.Abs(left-right) < 1e-10
}
