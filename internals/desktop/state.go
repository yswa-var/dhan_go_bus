package desktop

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"

	"yswa-var/dhan_go_bus/internals/ema"
	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
	"yswa-var/dhan_go_bus/internals/orderbook"
)

const maxChartCandles = 500

type Instrument struct {
	Symbol          string
	SecurityID      string
	ExchangeSegment string
}

type CandlePoint struct {
	Time      string  `json:"time"`
	Timestamp int64   `json:"-"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
	Low       float64 `json:"low"`
	High      float64 `json:"high"`
	EMA       float64 `json:"ema"`
}

type ChartState struct {
	Symbol  string        `json:"symbol"`
	LTP     float64       `json:"ltp"`
	Candles []CandlePoint `json:"candles"`
	Error   string        `json:"error,omitempty"`
}

// State is the concurrency-safe bridge between market goroutines and Fyne.
type State struct {
	mu          sync.RWMutex
	instruments []Instrument
	selected    Instrument
	ltp         map[string]float64
	books       map[string]orderbook.BookSnapshot
	candles     map[string][]CandlePoint
	err         string
}

func NewState(instruments []Instrument, initial Instrument) *State {
	return &State{instruments: append([]Instrument(nil), instruments...), selected: initial,
		ltp: make(map[string]float64), books: make(map[string]orderbook.BookSnapshot), candles: make(map[string][]CandlePoint)}
}

func (s *State) Instruments() []Instrument { return append([]Instrument(nil), s.instruments...) }

func key(instrument Instrument) string {
	return instrument.ExchangeSegment + ":" + instrument.SecurityID
}

func (s *State) Select(instrument Instrument) {
	s.mu.Lock()
	s.selected = instrument
	s.mu.Unlock()
}

func (s *State) Selected() Instrument {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.selected
}

func (s *State) OnMarketEvent(event marketfeed.MarketEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch value := event.(type) {
	case marketfeed.Tick:
		s.ltp[value.ExchangeSegment+":"+value.SecurityID] = float64(value.LTP)
	case marketfeed.Quote:
		s.ltp[value.ExchangeSegment+":"+value.SecurityID] = float64(value.LTP)
	}
}

func (s *State) OnBook(snapshot orderbook.BookSnapshot) {
	s.mu.Lock()
	s.books[bookKey(snapshot.SecurityID)] = snapshot
	s.mu.Unlock()
}

func bookKey(securityID int64) string { return strconv.FormatInt(securityID, 10) }

func (s *State) Book(instrument Instrument) (orderbook.BookSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	book, ok := s.books[instrument.SecurityID]
	return book, ok
}

func (s *State) OnCandle(candle ohlc.Candle, update ema.EMAUpdate, hasEMA bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, instrument := range s.instruments {
		if instrument.SecurityID != strconv.FormatInt(candle.SecurityID, 10) || instrument.ExchangeSegment != candle.ExchangeSegment {
			continue
		}
		points := s.candles[key(instrument)]
		point := CandlePoint{Time: candle.Start.Format("15:04"), Timestamp: candle.Start.UnixMilli(), Open: price(candle.Open), Close: price(candle.Close), Low: price(candle.Low), High: price(candle.High)}
		points = append(points, point)
		if len(points) > maxChartCandles {
			points = append([]CandlePoint(nil), points[len(points)-maxChartCandles:]...)
		}
		_ = update // The shared EMA engine provides the warm-up gate.
		if hasEMA || len(points) >= 20 {
			applyEMA(points)
		}
		s.candles[key(instrument)] = points
		return
	}
}

// LoadCandles replaces a selected instrument's chart history with the candles
// loaded once from storage. The database returns chronological candles.
func (s *State) LoadCandles(instrument Instrument, candles []ohlc.Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	points := make([]CandlePoint, 0, len(candles))
	for _, candle := range candles {
		points = append(points, CandlePoint{Time: candle.Start.Format("15:04"), Timestamp: candle.Start.UnixMilli(), Open: price(candle.Open), Close: price(candle.Close), Low: price(candle.Low), High: price(candle.High)})
	}
	lastHistoryTimestamp := int64(0)
	if len(points) > 0 {
		lastHistoryTimestamp = points[len(points)-1].Timestamp
	}
	for _, live := range s.candles[key(instrument)] {
		if live.Timestamp > lastHistoryTimestamp {
			points = append(points, live)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	if len(points) > maxChartCandles {
		points = points[len(points)-maxChartCandles:]
	}
	applyEMA(points)
	s.candles[key(instrument)] = points
}

func applyEMA(points []CandlePoint) {
	const period = 20
	for i := range points {
		points[i].EMA = 0
	}
	if len(points) < period {
		return
	}
	seed := 0.0
	for i := 0; i < period; i++ {
		seed += points[i].Close
	}
	value := seed / period
	points[period-1].EMA = value
	alpha := 2.0 / float64(period+1)
	for i := period; i < len(points); i++ {
		value = alpha*points[i].Close + (1-alpha)*value
		points[i].EMA = value
	}
}

func (s *State) SetError(err error) {
	s.mu.Lock()
	if err == nil {
		s.err = ""
	} else {
		s.err = err.Error()
	}
	s.mu.Unlock()
}

func (s *State) Snapshot() ChartState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	selected := s.selected
	return ChartState{Symbol: selected.Symbol, LTP: s.ltp[key(selected)], Candles: append([]CandlePoint(nil), s.candles[key(selected)]...), Error: s.err}
}

func (s *State) MarshalSnapshot() ([]byte, error) { return json.Marshal(s.Snapshot()) }

func price(value int64) float64 { return float64(value) / float64(ohlc.PriceScale) }
