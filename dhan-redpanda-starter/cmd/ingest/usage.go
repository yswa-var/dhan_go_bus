package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"yswa-var/dhan_go_bus/dhan-redpanda-starter/internal/broker"
	"yswa-var/dhan_go_bus/dhan-redpanda-starter/internal/config"
	dhan "yswa-var/dhan_go_bus/dhan-redpanda-starter/internal/dhan"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	contractPath := flag.String("contracts", "contracts.csv", "Dhan contract master CSV")
	brokerAddress := flag.String("broker", envOr("BROKER", "localhost:19092"), "Redpanda broker address")
	topic := flag.String("topic", envOr("TOPIC", "dhan.market.raw"), "Redpanda topic")
	flag.Parse()

	credentials, err := config.NewConfig()
	if err != nil {
		return err
	}
	token, err := credentials.GetAuthToken()
	if err != nil {
		return err
	}
	if _, err := os.Stat(*contractPath); errors.Is(err, os.ErrNotExist) {
		if *contractPath != "contracts.csv" {
			return err
		}
		if err := dhan.GetContract(); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	contracts, err := dhan.ResolveContracts(*contractPath)
	if err != nil {
		return err
	}
	if len(contracts) != 48 {
		return errors.New("contract selection must contain four futures and 44 options")
	}
	subscriptions := make([]dhan.Subscription, 0, len(contracts))
	for _, contract := range contracts {
		subscriptions = append(subscriptions, dhan.Subscription{ExchangeSegment: contract.ExchangeSegment, SecurityID: contract.SecurityID})
	}
	reader, err := dhan.NewFeedReader(dhan.FeedConfig{ClientID: credentials.ClientID(), AccessToken: token})
	if err != nil {
		return err
	}
	publisher, err := broker.NewPublisher(*brokerAddress, *topic)
	if err != nil {
		return err
	}
	defer publisher.Close()
	slog.Info("starting Dhan feed", "instruments", len(subscriptions), "broker", *brokerAddress, "topic", *topic)
	return reader.Run(ctx, subscriptions, func(event dhan.FeedEvent) error {
		return publisher.Publish(ctx, broker.Event{
			ExchangeCode: "MCX",
			SecurityID:   event.SecurityID,
			ReceivedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			RawPacket:    hex.EncodeToString(event.RawPacket),
		})
	})
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
