package marketfeed

import "time"

// MarketEvent is implemented by all decoded market-feed events.
type MarketEvent interface{ isMarketEvent() }

// Event is kept as an alias for callers of the original marketfeed API.
type Event = MarketEvent

// Tick contains the latest traded price. Full packets also populate Volume
// and OpenInterest so a consumer can log one compact market update.
type Tick struct {
	ExchangeSegment string
	SecurityID      string
	Symbol          string
	LTP             float32
	Volume          uint32
	OpenInterest    uint32
	Timestamp       time.Time
}

func (Tick) isMarketEvent() {}

// Quote contains the trade and OHLC fields carried by a quote or full packet.
type Quote struct {
	ExchangeSegment string
	SecurityID      string
	Symbol          string
	LTP             float32
	LastTradeQty    uint16
	Timestamp       time.Time
	AveragePrice    float32
	Volume          uint32
	TotalSellQty    uint32
	TotalBuyQty     uint32
	Open            float32
	Close           float32
	High            float32
	Low             float32
	OpenInterest    uint32
}

func (Quote) isMarketEvent() {}

// DepthLevel is one bid/ask level from a FULL packet.
type DepthLevel struct {
	BidQuantity uint32
	AskQuantity uint32
	BidOrders   uint16
	AskOrders   uint16
	BidPrice    float32
	AskPrice    float32
}

// Depth contains the five market-depth levels sent by a FULL packet.
type Depth struct {
	ExchangeSegment string
	SecurityID      string
	Symbol          string
	Timestamp       time.Time
	LTP             float32
	Levels          []DepthLevel
}

func (Depth) isMarketEvent() {}
