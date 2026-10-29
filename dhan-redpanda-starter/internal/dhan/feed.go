package dhan_ops

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultFeedEndpoint  = "wss://api-feed.dhan.co"
	maxFeedInstruments   = 5000
	maxSubscriptionBatch = 100
	fullSubscriptionCode = 21
	disconnectCode       = 12
	defaultReconnectMin  = time.Second
	defaultReconnectMax  = 30 * time.Second
)

// Subscription identifies one instrument in a Dhan market-feed request.
type Subscription struct {
	ExchangeSegment string
	SecurityID      string
}

// FeedConfig controls authentication, endpoint, and reconnect behavior.
type FeedConfig struct {
	ClientID     string
	AccessToken  string
	Endpoint     string
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

// FeedEvent is a decoded Dhan packet. RawPacket is retained for downstream
// publishing, auditing, or decoding fields added by Dhan later.
type FeedEvent struct {
	PacketType      PacketType
	ExchangeSegment string
	SecurityID      string
	ReceivedAt      time.Time
	RawPacket       []byte
	Tick            *Tick
	Quote           *Quote
	Depth           *Depth
}

type PacketType string

const (
	PacketTicker PacketType = "ticker"
	PacketQuote  PacketType = "quote"
	PacketFull   PacketType = "full"
)

// FeedReader owns the connection lifecycle. The wire connection is injected
// through dial so decoder and reconnect behavior remain unit-testable.
type FeedReader struct {
	config FeedConfig
	dial   func(context.Context, string) (feedConnection, error)
}

type handlerError struct{ error }

type feedConnection interface {
	ReadMessage() (int, []byte, error)
	WriteJSON(any) error
	Close() error
}

// NewFeedReader validates config and creates a reader that connects on Run.
func NewFeedReader(config FeedConfig) (*FeedReader, error) {
	if config.ClientID == "" {
		return nil, errors.New("Dhan feed client ID is required")
	}
	if config.AccessToken == "" {
		return nil, errors.New("Dhan feed access token is required")
	}
	if config.Endpoint == "" {
		config.Endpoint = defaultFeedEndpoint
	}
	if config.ReconnectMin <= 0 {
		config.ReconnectMin = defaultReconnectMin
	}
	if config.ReconnectMax < config.ReconnectMin {
		config.ReconnectMax = defaultReconnectMax
	}

	reader := &FeedReader{config: config}
	reader.dial = func(ctx context.Context, endpoint string) (feedConnection, error) {
		connection, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, http.Header{})
		if err != nil {
			return nil, err
		}
		return connection, nil
	}
	return reader, nil
}

// Run reads binary packets until ctx is cancelled. Temporary connection
// failures are retried with exponential backoff up to ReconnectMax.
func (r *FeedReader) Run(ctx context.Context, subscriptions []Subscription, onEvent func(FeedEvent) error) error {
	if onEvent == nil {
		return errors.New("Dhan feed event handler is required")
	}
	if err := validateSubscriptions(subscriptions); err != nil {
		return err
	}

	delay := r.config.ReconnectMin
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		connection, err := r.connect(ctx, subscriptions)
		if err == nil {
			delay = r.config.ReconnectMin
			err = r.read(ctx, connection, subscriptions, onEvent)
			_ = connection.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		var callbackFailure handlerError
		if errors.As(err, &callbackFailure) {
			return callbackFailure.error
		}
		if err == nil {
			err = errors.New("Dhan feed connection closed")
		}
		slog.Warn("Dhan feed reconnecting", "error", err, "delay", delay)
		if !waitForReconnect(ctx, delay) {
			return ctx.Err()
		}
		if delay < r.config.ReconnectMax {
			delay *= 2
			if delay > r.config.ReconnectMax {
				delay = r.config.ReconnectMax
			}
		}
	}
}

func (r *FeedReader) connect(ctx context.Context, subscriptions []Subscription) (feedConnection, error) {
	endpoint, err := r.endpoint()
	if err != nil {
		return nil, err
	}
	connection, err := r.dial(ctx, endpoint)
	if err != nil {
		// A dial error can contain the request URL, including its access token.
		message := strings.ReplaceAll(err.Error(), endpoint, "[Dhan feed URL]")
		message = strings.ReplaceAll(message, r.config.AccessToken, "[redacted]")
		message = strings.ReplaceAll(message, url.QueryEscape(r.config.AccessToken), "[redacted]")
		return nil, fmt.Errorf("connect to Dhan market feed: %s", message)
	}
	for start := 0; start < len(subscriptions); start += maxSubscriptionBatch {
		end := start + maxSubscriptionBatch
		if end > len(subscriptions) {
			end = len(subscriptions)
		}
		request := subscriptionRequest{
			RequestCode:     fullSubscriptionCode,
			InstrumentCount: end - start,
			InstrumentList:  make([]subscriptionInstrument, 0, end-start),
		}
		for _, subscription := range subscriptions[start:end] {
			request.InstrumentList = append(request.InstrumentList, subscriptionInstrument{
				ExchangeSegment: subscription.ExchangeSegment,
				SecurityID:      subscription.SecurityID,
			})
		}
		if err := connection.WriteJSON(request); err != nil {
			_ = connection.Close()
			return nil, fmt.Errorf("subscribe to Dhan market feed: %w", err)
		}
	}
	return connection, nil
}

func (r *FeedReader) endpoint() (string, error) {
	parsed, err := url.Parse(r.config.Endpoint)
	if err != nil {
		return "", fmt.Errorf("parse Dhan feed endpoint: %w", err)
	}
	query := parsed.Query()
	query.Set("version", "2")
	query.Set("token", r.config.AccessToken)
	query.Set("clientId", r.config.ClientID)
	query.Set("authType", "2")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (r *FeedReader) read(ctx context.Context, connection feedConnection, subscriptions []Subscription, onEvent func(FeedEvent) error) error {
	lookup := makeInstrumentLookup(subscriptions)
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.WriteJSON(disconnectRequest{RequestCode: disconnectCode})
			_ = connection.Close()
		case <-closed:
		}
	}()

	for {
		messageType, data, err := connection.ReadMessage()
		if err != nil {
			return err
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		events, err := decodeFeedMessage(data, lookup)
		if err != nil {
			return err
		}
		for _, event := range events {
			if err := onEvent(event); err != nil {
				return handlerError{err}
			}
		}
	}
}

func validateSubscriptions(subscriptions []Subscription) error {
	if len(subscriptions) == 0 {
		return errors.New("at least one Dhan feed subscription is required")
	}
	if len(subscriptions) > maxFeedInstruments {
		return fmt.Errorf("Dhan feed supports at most %d instruments", maxFeedInstruments)
	}
	seen := make(map[string]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		if exchangeCode(subscription.ExchangeSegment) == 255 {
			return fmt.Errorf("unsupported Dhan exchange segment %q", subscription.ExchangeSegment)
		}
		securityID, err := strconv.ParseUint(subscription.SecurityID, 10, 32)
		if err != nil || securityID == 0 {
			return fmt.Errorf("invalid Dhan security ID %q", subscription.SecurityID)
		}
		key := subscription.ExchangeSegment + ":" + subscription.SecurityID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate Dhan subscription %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

type subscriptionRequest struct {
	RequestCode     int                      `json:"RequestCode"`
	InstrumentCount int                      `json:"InstrumentCount"`
	InstrumentList  []subscriptionInstrument `json:"InstrumentList"`
}

type subscriptionInstrument struct {
	ExchangeSegment string `json:"ExchangeSegment"`
	SecurityID      string `json:"SecurityId"`
}

type disconnectRequest struct {
	RequestCode int `json:"RequestCode"`
}

func makeInstrumentLookup(subscriptions []Subscription) map[string]instrument {
	lookup := make(map[string]instrument, len(subscriptions))
	for _, subscription := range subscriptions {
		lookup[fmt.Sprintf("%d:%s", exchangeCode(subscription.ExchangeSegment), subscription.SecurityID)] = instrument{
			ExchangeSegment: subscription.ExchangeSegment,
			SecurityID:      subscription.SecurityID,
		}
	}
	return lookup
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
