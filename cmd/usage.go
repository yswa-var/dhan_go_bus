package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"yswa-var/dhan_go_bus/internals"
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
	if err := client.Run(ctx, subscriptions, printEvent); err != nil {
		log.Fatal(err)
	}
}

func printEvent(event marketfeed.Event) {
	switch event := event.(type) {
	case marketfeed.Tick:
		fmt.Printf("[TICK] %-20s SecurityID=%s LTP=%.2f Volume=%d OI=%d timestamp=%s\n",
			event.Symbol, event.SecurityID, event.LTP, event.Volume, event.OpenInterest, event.Timestamp.Format(timeFormat))
	case marketfeed.Quote:
		fmt.Printf("[QUOTE] %-20s SecurityID=%s LTP=%.2f Volume=%d OI=%d timestamp=%s\n",
			event.Symbol, event.SecurityID, event.LTP, event.Volume, event.OpenInterest, event.Timestamp.Format(timeFormat))
	case marketfeed.Depth:
		fmt.Printf("[DEPTH] %-20s SecurityID=%s bids=%d asks=%d\n",
			event.Symbol, event.SecurityID, len(event.Levels), len(event.Levels))
	}
}

const timeFormat = "2006-01-02T15:04:05Z07:00"
