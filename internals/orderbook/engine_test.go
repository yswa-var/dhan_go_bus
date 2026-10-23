package orderbook

import (
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
)

func TestEngineCreatesSnapshotAndCalculatesMetrics(t *testing.T) {
	engine := NewOrderBookEngine()
	snapshot, err := engine.OnDepth(depthEvent("1", time.Unix(100, 0), 300, 100))
	if err != nil {
		t.Fatalf("OnDepth() error = %v", err)
	}
	if snapshot.Bids[0].Price != 100*ohlc.PriceScale || snapshot.Asks[0].Price != 102*ohlc.PriceScale {
		t.Fatalf("best levels = %#v / %#v", snapshot.Bids[0], snapshot.Asks[0])
	}
	if snapshot.Spread != 2*ohlc.PriceScale || !almostEqual(snapshot.MidPrice, 101) || !almostEqual(snapshot.Imbalance, 0.5) {
		t.Fatalf("metrics = spread %d mid %.4f imbalance %.4f", snapshot.Spread, snapshot.MidPrice, snapshot.Imbalance)
	}
	if snapshot.Stale {
		t.Fatal("new snapshot is stale")
	}
}

func TestEngineKeepsInstrumentsIndependent(t *testing.T) {
	engine := NewEngine()
	first, err := engine.OnDepth(depthEvent("1", time.Unix(100, 0), 300, 100))
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.OnDepth(depthEvent("2", time.Unix(100, 0), 100, 200))
	if err != nil {
		t.Fatal(err)
	}
	if first.SecurityID != 1 || second.SecurityID != 2 || first.Bids[0].Price == second.Bids[0].Price {
		t.Fatalf("snapshots = %#v, %#v", first, second)
	}
	if _, ok := engine.Latest(3); ok {
		t.Fatal("unknown instrument has a book")
	}
}

func TestEngineReturnsDefinedEmptyMetricsForZeroQuantityBook(t *testing.T) {
	engine := NewEngine()
	empty := emptyDepth("1", time.Unix(100, 0))
	snapshot, err := engine.OnDepth(empty)
	if err != nil {
		t.Fatalf("OnDepth() error = %v", err)
	}
	if snapshot.Spread != 0 || snapshot.MidPrice != 0 || snapshot.Imbalance != 0 {
		t.Fatalf("empty metrics = %#v", snapshot)
	}
}

func TestEngineRejectsMalformedCrossedAndStaleUpdates(t *testing.T) {
	engine := NewEngine()
	valid := depthEvent("1", time.Unix(100, 0), 300, 100)
	valid.Levels = valid.Levels[:4]
	if _, err := engine.OnDepth(valid); err == nil || !strings.Contains(err.Error(), ErrInvalidDepth.Error()) {
		t.Fatalf("short packet error = %v", err)
	}

	valid = depthEvent("1", time.Unix(100, 0), 300, 100)
	valid.Levels[0].BidPrice = float32(math.NaN())
	if _, err := engine.OnDepth(valid); err == nil || !strings.Contains(err.Error(), ErrInvalidDepth.Error()) {
		t.Fatalf("NaN packet error = %v", err)
	}

	valid = depthEvent("1", time.Unix(100, 0), 300, 100)
	valid.Levels[0].AskPrice = 99
	if _, err := engine.OnDepth(valid); err == nil || !strings.Contains(err.Error(), ErrCrossedBook.Error()) {
		t.Fatalf("crossed packet error = %v", err)
	}

	if _, err := engine.OnDepth(depthEvent("1", time.Unix(100, 0), 300, 100)); err != nil {
		t.Fatalf("valid packet error = %v", err)
	}
	if _, err := engine.OnDepth(depthEvent("1", time.Unix(99, 0), 999, 99)); err == nil || !strings.Contains(err.Error(), ErrStaleUpdate.Error()) {
		t.Fatalf("stale packet error = %v", err)
	}
	latest, ok := engine.Latest(1)
	if !ok || latest.Bids[0].Quantity != 300 {
		t.Fatalf("stale packet overwrote latest book: %#v, exists=%v", latest, ok)
	}
}

func TestEngineMarksBooksStaleUntilRefreshed(t *testing.T) {
	engine := NewEngine()
	if _, err := engine.OnDepth(depthEvent("1", time.Unix(100, 0), 300, 100)); err != nil {
		t.Fatal(err)
	}
	engine.MarkAllStale()
	stale, ok := engine.Latest(1)
	if !ok || !stale.Stale {
		t.Fatalf("book after reconnect = %#v, exists=%v", stale, ok)
	}
	refreshed, err := engine.OnDepth(depthEvent("1", time.Unix(101, 0), 301, 100))
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Stale {
		t.Fatal("refreshed book is still stale")
	}
}

func TestEnginePublishesLatestSnapshotWithoutBlocking(t *testing.T) {
	engine := NewEngine()
	_, snapshots := engine.Subscribe(1)
	if _, err := engine.OnDepth(depthEvent("1", time.Unix(100, 0), 300, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.OnDepth(depthEvent("1", time.Unix(101, 0), 301, 100)); err != nil {
		t.Fatal(err)
	}
	got := <-snapshots
	if !got.Timestamp.Equal(time.Unix(101, 0).UTC()) || got.Bids[0].Quantity != 301 {
		t.Fatalf("snapshot subscriber got %#v", got)
	}
}

func TestEngineSupportsConcurrentReadersAndWriters(t *testing.T) {
	engine := NewEngine()
	var waitGroup sync.WaitGroup
	for writer := 0; writer < 4; writer++ {
		waitGroup.Add(1)
		go func(offset int) {
			defer waitGroup.Done()
			for index := 0; index < 25; index++ {
				stamp := time.Unix(int64(offset*100+index+1), 0)
				_, _ = engine.OnDepth(depthEvent("1", stamp, uint32(index+1), 100))
			}
		}(writer)
	}
	for reader := 0; reader < 4; reader++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for index := 0; index < 50; index++ {
				_, _ = engine.Latest(1)
			}
		}()
	}
	waitGroup.Wait()
}

func depthEvent(securityID string, timestamp time.Time, bidQuantity, bidPrice uint32) marketfeed.Depth {
	levels := make([]marketfeed.DepthLevel, LevelCount)
	for index := range levels {
		levels[index] = marketfeed.DepthLevel{
			BidQuantity: bidQuantity,
			AskQuantity: 100,
			BidOrders:   1,
			AskOrders:   1,
			BidPrice:    float32(bidPrice) - float32(index),
			AskPrice:    float32(bidPrice) + 2 + float32(index),
		}
	}
	return marketfeed.Depth{
		SecurityID: securityID,
		Timestamp:  timestamp,
		Levels:     levels,
	}
}

func emptyDepth(securityID string, timestamp time.Time) marketfeed.Depth {
	return marketfeed.Depth{SecurityID: securityID, Timestamp: timestamp, Levels: make([]marketfeed.DepthLevel, LevelCount)}
}

func almostEqual(left, right float64) bool {
	return math.Abs(left-right) < 1e-10
}
