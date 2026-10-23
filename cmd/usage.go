package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"yswa-var/dhan_go_bus/internals"
	"yswa-var/dhan_go_bus/internals/bus"
	"yswa-var/dhan_go_bus/internals/dhan_ops"
	"yswa-var/dhan_go_bus/internals/marketfeed"
)

func main() {
	contracts, err := dhan_ops.ResolveContracts("contracts.csv")
	if err != nil {
		log.Fatal(err)
	}
	if len(contracts) != 48 {
		log.Fatalf("resolved %d instruments, want 48", len(contracts))
	}

	eventBus := bus.NewBroadcastBus[marketfeed.MarketEvent]()
	defer eventBus.Close()
	subscriberID, events := eventBus.Subscribe(256)
	go printMetrics(eventBus, subscriberID, events)

	config, err := internals.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	token, err := config.GetAuthToken()
	if err != nil {
		log.Fatal(err)
	}

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := client.RunWithBus(ctx, subscriptions, eventBus); err != nil {
		log.Fatal(err)
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
