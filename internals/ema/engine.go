// Package ema calculates one incremental EMA per instrument.
package ema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"yswa-var/dhan_go_bus/internals/ohlc"
)

var (
	ErrInvalidCandle = errors.New("invalid candle for EMA")
	ErrOutOfOrder    = errors.New("candle timestamp is older than the last EMA candle")
)

// EMAState is the current indicator state for one instrument.
//
// MissingIntervals is informational only. The EMA uses observed completed
// candles, so an empty minute does not contribute a synthetic close.
type EMAState struct {
	SecurityID       int64
	Period           int
	Value            float64
	LastTimestamp    time.Time
	Initialized      bool
	ObservedCandles  int
	MissingIntervals int64
}

// EMAUpdate is emitted when a candle changes a valid EMA value.
type EMAUpdate struct {
	SecurityID int64
	Timestamp  time.Time
	Period     int
	Value      float64
}

// EMAEngine is the candle-facing indicator contract.
type EMAEngine interface {
	OnCandle(candle ohlc.Candle) (EMAUpdate, bool, error)
}

// CandleLoader supplies completed candles for startup recovery.
type CandleLoader interface {
	LoadRecentCandles(ctx context.Context, securityID int64, exchangeSegment string, interval time.Duration, limit int) ([]ohlc.Candle, error)
}

// Instrument identifies one history stream to recover at startup.
type Instrument struct {
	SecurityID      int64
	ExchangeSegment string
	Interval        time.Duration
}

type state struct {
	EMAState
	seed []float64
}

// Engine calculates EMA values independently for each security ID.
type Engine struct {
	period int
	alpha  float64
	states map[int64]*state
}

// NewEngine creates an incremental EMA calculator for the requested period.
func NewEngine(period int) (*Engine, error) {
	if period <= 0 {
		return nil, fmt.Errorf("EMA period must be positive")
	}
	return &Engine{
		period: period,
		alpha:  2 / float64(period+1),
		states: make(map[int64]*state),
	}, nil
}

// NewEMA20Engine creates the Quest 005 EMA calculator.
func NewEMA20Engine() *Engine {
	engine, err := NewEngine(20)
	if err != nil {
		panic(err)
	}
	return engine
}

// OnCandle accepts one completed candle. Duplicate timestamps are ignored so
// retries or duplicate bus deliveries cannot update an EMA twice.
func (e *Engine) OnCandle(candle ohlc.Candle) (EMAUpdate, bool, error) {
	if e == nil || e.period <= 0 {
		return EMAUpdate{}, false, errors.New("EMA engine is not initialized")
	}
	if candle.SecurityID <= 0 || candle.Close <= 0 || candle.Start.IsZero() {
		return EMAUpdate{}, false, fmt.Errorf("%w: security=%d timestamp=%v close=%d", ErrInvalidCandle, candle.SecurityID, candle.Start, candle.Close)
	}

	current := e.states[candle.SecurityID]
	if current == nil {
		current = &state{
			EMAState: EMAState{SecurityID: candle.SecurityID, Period: e.period},
			seed:     make([]float64, 0, e.period),
		}
		e.states[candle.SecurityID] = current
	}
	if !current.LastTimestamp.IsZero() {
		switch {
		case candle.Start.Equal(current.LastTimestamp):
			return EMAUpdate{}, false, nil
		case candle.Start.Before(current.LastTimestamp):
			return EMAUpdate{}, false, fmt.Errorf("%w: security=%d timestamp=%s last=%s", ErrOutOfOrder, candle.SecurityID, candle.Start.UTC().Format(time.RFC3339Nano), current.LastTimestamp.UTC().Format(time.RFC3339Nano))
		}
		current.MissingIntervals += missingIntervals(current.LastTimestamp, candle.Start, candle.Interval)
	}

	close := float64(candle.Close) / float64(ohlc.PriceScale)
	current.LastTimestamp = candle.Start.UTC()
	current.ObservedCandles++
	if !current.Initialized {
		current.seed = append(current.seed, close)
		if len(current.seed) < e.period {
			return EMAUpdate{}, false, nil
		}
		var total float64
		for _, value := range current.seed {
			total += value
		}
		current.Value = total / float64(e.period)
		current.Initialized = true
		current.seed = nil
	} else {
		current.Value = e.alpha*close + (1-e.alpha)*current.Value
	}

	return EMAUpdate{
		SecurityID: candle.SecurityID,
		Timestamp:  current.LastTimestamp,
		Period:     current.Period,
		Value:      current.Value,
	}, true, nil
}

// State returns a snapshot that is safe for callers to retain.
func (e *Engine) State(securityID int64) (EMAState, bool) {
	if e == nil {
		return EMAState{}, false
	}
	current, ok := e.states[securityID]
	if !ok {
		return EMAState{}, false
	}
	return current.EMAState, true
}

// Recover rebuilds state from all available observed candles before live events
// are consumed. Replaying the full stored sequence preserves the original EMA
// seed and avoids the warm-start error caused by loading only the latest period.
func (e *Engine) Recover(ctx context.Context, loader CandleLoader, instruments []Instrument) error {
	if ctx == nil {
		return errors.New("EMA recovery context is nil")
	}
	if e == nil || e.period <= 0 {
		return errors.New("EMA engine is not initialized")
	}
	if loader == nil {
		return errors.New("EMA recovery candle loader is required")
	}
	for _, instrument := range instruments {
		if instrument.SecurityID <= 0 || instrument.ExchangeSegment == "" || instrument.Interval <= 0 {
			return fmt.Errorf("invalid EMA recovery instrument %#v", instrument)
		}
		candles, err := loader.LoadRecentCandles(ctx, instrument.SecurityID, instrument.ExchangeSegment, instrument.Interval, 0)
		if err != nil {
			return fmt.Errorf("load EMA history for security %d: %w", instrument.SecurityID, err)
		}
		// Sorting here keeps recovery correct even if a loader changes its SQL
		// order. The live path still rejects out-of-order candles.
		sort.SliceStable(candles, func(i, j int) bool { return candles[i].Start.Before(candles[j].Start) })
		for _, candle := range candles {
			if _, _, err := e.OnCandle(candle); err != nil {
				return fmt.Errorf("replay EMA history for security %d: %w", instrument.SecurityID, err)
			}
		}
	}
	return nil
}

func missingIntervals(previous, current time.Time, interval time.Duration) int64 {
	if interval <= 0 || !current.After(previous) {
		return 0
	}
	difference := current.Sub(previous)
	if difference < interval {
		return 0
	}
	return int64(difference/interval) - 1
}

var _ EMAEngine = (*Engine)(nil)
