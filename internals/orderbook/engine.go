// Package orderbook maintains the latest five-level book for each instrument.
package orderbook

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
)

const LevelCount = 5

const PriceScale = ohlc.PriceScale

var (
	ErrInvalidDepth = errors.New("invalid depth event")
	ErrStaleUpdate  = errors.New("stale order-book update")
	ErrCrossedBook  = errors.New("crossed order book")
)

// BookLevel stores a price in the repository's fixed-point price scale.
type BookLevel struct {
	Price    int64
	Quantity int64
	Orders   uint32
}

// OrderBook is the validated raw state for one instrument.
type OrderBook struct {
	SecurityID int64
	Timestamp  time.Time
	Bids       [LevelCount]BookLevel
	Asks       [LevelCount]BookLevel
	Stale      bool
}

// BookSnapshot is an immutable value returned to callers and subscribers.
// Spread is in fixed-point price units; MidPrice is in normal price units.
type BookSnapshot struct {
	SecurityID int64
	Timestamp  time.Time

	Bids [LevelCount]BookLevel
	Asks [LevelCount]BookLevel

	Spread    int64
	MidPrice  float64
	Imbalance float64
	Stale     bool
}

// BookEngine exposes the live book state and derived metrics.
type BookEngine interface {
	OnDepth(event marketfeed.Depth) (BookSnapshot, error)
	Latest(securityID int64) (BookSnapshot, bool)
}

type subscriber struct {
	channel chan BookSnapshot
}

// SubscriptionID identifies a non-blocking snapshot subscriber.
type SubscriptionID uint64

// Engine keeps one book per security and never waits for a snapshot consumer.
type Engine struct {
	mu          sync.RWMutex
	books       map[int64]OrderBook
	subscribers map[SubscriptionID]*subscriber
	nextID      atomic.Uint64
}

// NewEngine creates an empty in-memory order-book engine.
func NewEngine() *Engine {
	return &Engine{
		books:       make(map[int64]OrderBook),
		subscribers: make(map[SubscriptionID]*subscriber),
	}
}

// NewOrderBookEngine is the descriptive constructor used by application code.
func NewOrderBookEngine() *Engine {
	return NewEngine()
}

// OnDepth validates and stores a newer five-level packet. Empty levels are
// allowed because a feed can temporarily report no displayed quantity.
func (e *Engine) OnDepth(event marketfeed.Depth) (BookSnapshot, error) {
	if e == nil {
		return BookSnapshot{}, errors.New("order-book engine is not initialized")
	}
	securityID, err := parseSecurityID(event.SecurityID)
	if err != nil {
		return BookSnapshot{}, fmt.Errorf("%w: %v", ErrInvalidDepth, err)
	}
	if event.Timestamp.IsZero() {
		return BookSnapshot{}, fmt.Errorf("%w: depth timestamp is required", ErrInvalidDepth)
	}
	if len(event.Levels) != LevelCount {
		return BookSnapshot{}, fmt.Errorf("%w: got %d levels, want %d", ErrInvalidDepth, len(event.Levels), LevelCount)
	}

	bids, asks, err := normalizeLevels(event.Levels)
	if err != nil {
		return BookSnapshot{}, err
	}
	if bestBid, bestAsk := bestPrices(bids, asks); bestBid > 0 && bestAsk > 0 && bestBid > bestAsk {
		return BookSnapshot{}, fmt.Errorf("%w: bid=%d ask=%d", ErrCrossedBook, bestBid, bestAsk)
	}

	timestamp := event.Timestamp.UTC()
	e.mu.Lock()
	if e.books == nil {
		e.books = make(map[int64]OrderBook)
	}
	if e.subscribers == nil {
		e.subscribers = make(map[SubscriptionID]*subscriber)
	}
	current, exists := e.books[securityID]
	if exists && !timestamp.After(current.Timestamp) {
		e.mu.Unlock()
		return BookSnapshot{}, fmt.Errorf("%w: security=%d timestamp=%s last=%s", ErrStaleUpdate, securityID, timestamp.Format(time.RFC3339Nano), current.Timestamp.Format(time.RFC3339Nano))
	}
	book := OrderBook{
		SecurityID: securityID,
		Timestamp:  timestamp,
		Bids:       bids,
		Asks:       asks,
	}
	e.books[securityID] = book
	snapshot := snapshotOf(book)
	e.publishLocked(snapshot)
	e.mu.Unlock()
	return snapshot, nil
}

// Latest returns a copy of the latest snapshot. Arrays and values are copied,
// so callers cannot mutate the engine's state.
func (e *Engine) Latest(securityID int64) (BookSnapshot, bool) {
	if e == nil {
		return BookSnapshot{}, false
	}
	e.mu.RLock()
	book, ok := e.books[securityID]
	e.mu.RUnlock()
	if !ok {
		return BookSnapshot{}, false
	}
	return snapshotOf(book), true
}

// MarkStale marks one existing book stale until a newer packet refreshes it.
func (e *Engine) MarkStale(securityID int64) bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	book, ok := e.books[securityID]
	if !ok || book.Stale {
		return ok
	}
	book.Stale = true
	e.books[securityID] = book
	e.publishLocked(snapshotOf(book))
	return true
}

// MarkAllStale marks every known book stale after a disconnect or reconnect.
func (e *Engine) MarkAllStale() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for securityID, book := range e.books {
		if book.Stale {
			continue
		}
		book.Stale = true
		e.books[securityID] = book
		e.publishLocked(snapshotOf(book))
	}
}

// Subscribe creates a bounded, non-blocking snapshot stream. If a consumer
// falls behind, its oldest pending snapshot is replaced by the newest one.
func (e *Engine) Subscribe(bufferSize int) (SubscriptionID, <-chan BookSnapshot) {
	if bufferSize < 1 {
		panic("order-book subscriber buffer size must be positive")
	}
	sub := &subscriber{channel: make(chan BookSnapshot, bufferSize)}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.books == nil {
		e.books = make(map[int64]OrderBook)
	}
	if e.subscribers == nil {
		e.subscribers = make(map[SubscriptionID]*subscriber)
	}
	id := SubscriptionID(e.nextID.Add(1))
	e.subscribers[id] = sub
	return id, sub.channel
}

// Unsubscribe closes one snapshot stream.
func (e *Engine) Unsubscribe(id SubscriptionID) {
	if e == nil {
		return
	}
	e.mu.Lock()
	sub, ok := e.subscribers[id]
	if ok {
		delete(e.subscribers, id)
		close(sub.channel)
	}
	e.mu.Unlock()
}

// Close closes all snapshot streams and releases subscriber references.
func (e *Engine) Close() {
	if e == nil {
		return
	}
	e.mu.Lock()
	for id, sub := range e.subscribers {
		close(sub.channel)
		delete(e.subscribers, id)
	}
	e.mu.Unlock()
}

func (e *Engine) publishLocked(snapshot BookSnapshot) {
	for _, sub := range e.subscribers {
		select {
		case sub.channel <- snapshot:
		default:
			// Keeping the newest state is more useful to a live UI than a
			// growing backlog of already-obsolete books.
			select {
			case <-sub.channel:
			default:
			}
			select {
			case sub.channel <- snapshot:
			default:
			}
		}
	}
}

func normalizeLevels(levels []marketfeed.DepthLevel) ([LevelCount]BookLevel, [LevelCount]BookLevel, error) {
	var bids, asks [LevelCount]BookLevel
	bidLevels := make([]BookLevel, 0, LevelCount)
	askLevels := make([]BookLevel, 0, LevelCount)
	for index, level := range levels {
		bid, err := convertLevel(level.BidPrice, level.BidQuantity, uint32(level.BidOrders), "bid", index)
		if err != nil {
			return bids, asks, err
		}
		ask, err := convertLevel(level.AskPrice, level.AskQuantity, uint32(level.AskOrders), "ask", index)
		if err != nil {
			return bids, asks, err
		}
		if bid.Price > 0 {
			bidLevels = append(bidLevels, bid)
		}
		if ask.Price > 0 {
			askLevels = append(askLevels, ask)
		}
	}
	sort.SliceStable(bidLevels, func(i, j int) bool { return bidLevels[i].Price > bidLevels[j].Price })
	sort.SliceStable(askLevels, func(i, j int) bool { return askLevels[i].Price < askLevels[j].Price })
	if duplicatePrice(bidLevels) || duplicatePrice(askLevels) {
		return bids, asks, fmt.Errorf("%w: duplicate level price", ErrInvalidDepth)
	}
	copy(bids[:], bidLevels)
	copy(asks[:], askLevels)
	return bids, asks, nil
}

func convertLevel(price float32, quantity uint32, orders uint32, side string, index int) (BookLevel, error) {
	if math.IsNaN(float64(price)) || math.IsInf(float64(price), 0) || price < 0 {
		return BookLevel{}, fmt.Errorf("%w: invalid %s price at level %d", ErrInvalidDepth, side, index)
	}
	if price == 0 {
		if quantity != 0 || orders != 0 {
			return BookLevel{}, fmt.Errorf("%w: %s quantity without price at level %d", ErrInvalidDepth, side, index)
		}
		return BookLevel{}, nil
	}
	scaled, err := ohlc.ScalePrice(price)
	if err != nil {
		return BookLevel{}, fmt.Errorf("%w: %s price at level %d: %v", ErrInvalidDepth, side, index, err)
	}
	return BookLevel{Price: scaled, Quantity: int64(quantity), Orders: orders}, nil
}

func duplicatePrice(levels []BookLevel) bool {
	for index := 1; index < len(levels); index++ {
		if levels[index].Price == levels[index-1].Price {
			return true
		}
	}
	return false
}

func bestPrices(bids, asks [LevelCount]BookLevel) (int64, int64) {
	return bids[0].Price, asks[0].Price
}

func snapshotOf(book OrderBook) BookSnapshot {
	bestBid, bestAsk := bestPrices(book.Bids, book.Asks)
	var spread int64
	var mid float64
	if bestBid > 0 && bestAsk > 0 {
		spread = bestAsk - bestBid
		mid = (float64(bestBid) + float64(bestAsk)) / (2 * float64(ohlc.PriceScale))
	}
	var bidQuantity, askQuantity int64
	for index := 0; index < LevelCount; index++ {
		bidQuantity += book.Bids[index].Quantity
		askQuantity += book.Asks[index].Quantity
	}
	var imbalance float64
	totalQuantity := bidQuantity + askQuantity
	if totalQuantity > 0 {
		imbalance = float64(bidQuantity-askQuantity) / float64(totalQuantity)
	}
	return BookSnapshot{
		SecurityID: book.SecurityID,
		Timestamp:  book.Timestamp,
		Bids:       book.Bids,
		Asks:       book.Asks,
		Spread:     spread,
		MidPrice:   mid,
		Imbalance:  imbalance,
		Stale:      book.Stale,
	}
}

func parseSecurityID(value string) (int64, error) {
	securityID, err := strconv.ParseInt(value, 10, 64)
	if err != nil || securityID <= 0 {
		return 0, fmt.Errorf("invalid security ID %q", value)
	}
	return securityID, nil
}

var _ BookEngine = (*Engine)(nil)
