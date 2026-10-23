package main

import (
	"context"
	"log"

	"yswa-var/dhan_go_bus"
	"yswa-var/dhan_go_bus/internals"
	"yswa-var/dhan_go_bus/internals/desktop"
	"yswa-var/dhan_go_bus/internals/dhan_ops"
	"yswa-var/dhan_go_bus/internals/storage"
)

func main() {
	contracts, err := dhan_ops.ResolveContracts("contracts.csv")
	if err != nil {
		log.Fatal(err)
	}
	config, err := internals.NewConfig()
	if err != nil {
		log.Fatal(err)
	}
	var history *storage.PostgresWriter
	if config.DatabaseURL() != "" {
		history, err = storage.NewPostgresWriter(context.Background(), config.DatabaseURL())
		if err != nil {
			log.Printf("historical candles unavailable: %v", err)
			history = nil
		} else if err := history.EnsureSchema(context.Background()); err != nil {
			log.Printf("historical candles unavailable: %v", err)
			history.Close()
			history = nil
		}
	}
	token, err := config.GetAuthToken()
	if err != nil {
		log.Fatal(err)
	}
	instruments := make([]desktop.Instrument, 0, len(contracts))
	for _, contract := range contracts {
		instruments = append(instruments, desktop.Instrument{Symbol: contract.Symbol, SecurityID: contract.SecurityID, ExchangeSegment: contract.ExchangeSegment})
	}
	if err := desktop.Run(dhan_go_bus.ChartAssets, config.ClientID(), token, instruments, history); err != nil {
		if history != nil {
			history.Close()
		}
		log.Fatal(err)
	}
	if history != nil {
		history.Close()
	}
}
