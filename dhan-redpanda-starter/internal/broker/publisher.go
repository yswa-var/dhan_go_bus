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

type Publisher struct {
	client *kgo.Client
	topic  string
}

func NewPublisher(brokerAddress, topic string) (*Publisher, error) {
	if brokerAddress == "" || topic == "" {
		return nil, fmt.Errorf("broker address and topic are required")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddress),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &Publisher{client: client, topic: topic}, nil
}

func (p *Publisher) Close() { p.client.Close() }

func (p *Publisher) Publish(ctx context.Context, event Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	results := p.client.ProduceSync(ctx, &kgo.Record{
		Key:   []byte(event.ExchangeCode + ":" + event.SecurityID),
		Value: value,
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}

// Publish retains the batch helper for callers that do not need a long-lived producer.
func Publish(ctx context.Context, brokerAddress, topic string, events []Event) error {
	p, err := NewPublisher(brokerAddress, topic)
	if err != nil {
		return err
	}
	defer p.Close()

	for _, event := range events {
		if err := p.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}
