package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"yswa-var/dhan_go_bus/internals"
	"yswa-var/dhan_go_bus/internals/bus"
	"yswa-var/dhan_go_bus/internals/dhan_ops"
	"yswa-var/dhan_go_bus/internals/ema"
	"yswa-var/dhan_go_bus/internals/marketfeed"
	"yswa-var/dhan_go_bus/internals/ohlc"
	"yswa-var/dhan_go_bus/internals/orderbook"
	"yswa-var/dhan_go_bus/internals/pipeline"
	"yswa-var/dhan_go_bus/internals/storage"
)

func main() {
	contracts, err := dhan_ops.ResolveContracts("contracts.csv")
	if err != nil {
		log.Fatal(err)
	}
	if len(contracts) != 48 {
		log.Fatalf("resolved %d instruments, want 48", len(contracts))
	}

	config, err := internals.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	token, err := config.GetAuthToken()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	postgres, err := storage.NewPostgresWriter(ctx, config.DatabaseURL())
	if err != nil {
		log.Fatal(err)
	}
	defer postgres.Close()
	if err := postgres.EnsureSchema(ctx); err != nil {
		log.Fatal(err)
	}

	eventBus := bus.NewBroadcastBus[marketfeed.MarketEvent]()
	defer eventBus.Close()
	metricsID, metricsEvents := eventBus.Subscribe(256)
	go printMetrics(eventBus, metricsID, metricsEvents)
	_, ohlcEvents := eventBus.Subscribe(256)
	bookEngine := orderbook.NewOrderBookEngine()
	_, depthEvents := eventBus.Subscribe(256)
	candleBus := bus.NewBroadcastBus[ohlc.Candle]()
	defer candleBus.Close()
	_, writerEvents := candleBus.Subscribe(storage.QueueCapacity)
	_, emaEvents := candleBus.Subscribe(256)

	emaEngine := ema.NewEMA20Engine()
	emaInstruments, err := emaInstruments(contracts)
	if err != nil {
		log.Fatal(err)
	}
	if err := emaEngine.Recover(ctx, postgres, emaInstruments); err != nil {
		log.Fatal(err)
	}

	aggregator := ohlc.NewMinuteAggregator()
	candlePipeline, err := pipeline.NewCandlePipelineWithPublisher(ohlcEvents, aggregator, candleBus.Publish)
	if err != nil {
		log.Fatal(err)
	}

	pipelineContext, cancelPipeline := context.WithCancel(context.Background())
	defer cancelPipeline()
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- storage.NewBatchWriter(writerEvents, postgres).Run(pipelineContext)
	}()
	emaDone := make(chan error, 1)
	go func() {
		emaDone <- runEMA(pipelineContext, emaEvents, emaEngine)
	}()
	bookDone := make(chan error, 1)
	go func() {
		bookDone <- runOrderBook(pipelineContext, depthEvents, bookEngine)
	}()
	pipelineDone := make(chan error, 1)
	go func() {
		pipelineDone <- candlePipeline.Run(pipelineContext)
	}()
	client, err := marketfeed.NewClient(marketfeed.ClientConfig{
		ClientID:    config.ClientID(),
		AccessToken: token,
		OnConnect: func() {
			fmt.Println("[CONNECTED] Dhan WebSocket")
		},
		OnSubscribe: func(count int) {
			fmt.Printf("[SUBSCRIBED] %d instruments\n", count)
		},
		OnError: func(err error) {
			bookEngine.MarkAllStale()
			log.Printf("[RECONNECT] %v", err)
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	subscriptions := make([]marketfeed.Subscription, 0, len(contracts))
	for _, contract := range contracts {
		subscriptions = append(subscriptions, marketfeed.Subscription{
			ExchangeSegment: contract.ExchangeSegment,
			SecurityID:      contract.SecurityID,
			Symbol:          contract.Symbol,
		})
	}

	clientErr := client.RunWithBus(ctx, subscriptions, eventBus)
	eventBus.Close()
	if err := <-bookDone; err != nil {
		log.Printf("order-book engine stopped: %v", err)
	}
	shutdownTimer := time.AfterFunc(10*time.Second, cancelPipeline)
	if err := <-pipelineDone; err != nil {
		log.Printf("candle pipeline stopped: %v", err)
	}
	candleBus.Close()
	if err := <-writerDone; err != nil {
		log.Printf("candle writer stopped: %v", err)
	}
	if err := <-emaDone; err != nil {
		log.Printf("EMA engine stopped: %v", err)
	}
	shutdownTimer.Stop()
	if clientErr != nil {
		log.Fatal(clientErr)
	}
	if ctx.Err() != nil {
		return
	}
}

func runOrderBook(ctx context.Context, events <-chan marketfeed.MarketEvent, engine *orderbook.Engine) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return nil
			}
			depth, ok := event.(marketfeed.Depth)
			if !ok {
				continue
			}
			if _, err := engine.OnDepth(depth); err != nil {
				// A malformed or stale packet must not stop updates for the
				// other subscribed instruments.
				log.Printf("[ORDER BOOK] security=%s: %v", depth.SecurityID, err)
			}
		}
	}
}

func emaInstruments(contracts []dhan_ops.Contract) ([]ema.Instrument, error) {
	instruments := make([]ema.Instrument, 0, len(contracts))
	for _, contract := range contracts {
		securityID, err := strconv.ParseInt(contract.SecurityID, 10, 64)
		if err != nil || securityID <= 0 {
			return nil, fmt.Errorf("invalid security ID %q for EMA recovery", contract.SecurityID)
		}
		instruments = append(instruments, ema.Instrument{
			SecurityID:      securityID,
			ExchangeSegment: contract.ExchangeSegment,
			Interval:        time.Minute,
		})
	}
	return instruments, nil
}

func runEMA(ctx context.Context, events <-chan ohlc.Candle, engine *ema.Engine) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case candle, ok := <-events:
			if !ok {
				return nil
			}
			if _, _, err := engine.OnCandle(candle); err != nil {
				return err
			}
		}
	}
}

func printMetrics(eventBus *bus.BroadcastBusImpl[marketfeed.MarketEvent], subscriberID bus.SubscriberID, events <-chan marketfeed.MarketEvent) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	counts := map[string]int{}
	bidLevels, askLevels := 0, 0
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			counts[eventType(event)]++
			if depth, ok := event.(marketfeed.Depth); ok {
				bids, asks := depthCounts(depth.Levels)
				bidLevels += bids
				askLevels += asks
			}
		case <-ticker.C:
			stats, ok := eventBus.Stats(subscriberID)
			if !ok {
				return
			}
			fmt.Printf("[METRICS] events=%v depth_levels=%d/%d queue=%d/%d dropped=%d\n",
				counts, bidLevels, askLevels, stats.QueueDepth, stats.BufferSize, stats.DroppedEvents)
			counts = map[string]int{}
			bidLevels, askLevels = 0, 0
		}
	}
}

func eventType(event marketfeed.MarketEvent) string {
	switch event.(type) {
	case marketfeed.Tick:
		return "tick"
	case marketfeed.Quote:
		return "quote"
	case marketfeed.Depth:
		return "depth"
	}
	return "unknown"
}

func depthCounts(levels []marketfeed.DepthLevel) (bids, asks int) {
	for _, level := range levels {
		if level.BidQuantity > 0 || level.BidPrice > 0 {
			bids++
		}
		if level.AskQuantity > 0 || level.AskPrice > 0 {
			asks++
		}
	}
	return bids, asks
}
