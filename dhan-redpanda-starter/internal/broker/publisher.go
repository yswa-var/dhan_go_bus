package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Event struct {
	ExchangeCode string `json:"exchange_code"`
	SecurityID   string `json:"security_id"`
	ReceivedAt   string `json:"received_at"`
	RawPacket    string `json:"raw_packet"`
}

func Publish(ctx context.Context, brokerAddress, topic string, events []Event) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddress),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, len(events))
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
		records = append(records, &kgo.Record{
			Key:   []byte(event.ExchangeCode + ":" + event.SecurityID),
			Value: value,
		})
	}
	results := client.ProduceSync(ctx, records...)
	for _, result := range results {
		if result.Err != nil {
			return fmt.Errorf("publish event: %w", result.Err)
		}
	}
	return nil
}
