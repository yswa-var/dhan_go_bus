package marketfeed

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultEndpoint          = "wss://api-feed.dhan.co"
	fullRequestCode          = 21
	disconnectRequestCode    = 12
	maxInstrumentsPerRequest = 100
	defaultReconnectMin      = time.Second
	defaultReconnectMax      = 30 * time.Second
)

// Subscription identifies one Dhan instrument in a market-feed request.
type Subscription struct {
	ExchangeSegment string
	SecurityID      string
	Symbol          string
}

// ClientConfig contains credentials and connection behavior for Client.
type ClientConfig struct {
	ClientID    string
	AccessToken string
	Endpoint    string

	ReconnectMin time.Duration
	ReconnectMax time.Duration
	OnConnect    func()
	OnSubscribe  func(int)
	OnError      func(error)

	// Dialer is injectable so packet and reconnect behavior can be tested
	// without opening a real Dhan connection.
	Dialer *websocket.Dialer
}

// connection is the small subset of websocket behavior needed by the reader.
// Keeping it private makes the production API simple while allowing focused tests.
type connection interface {
	ReadMessage() (messageType int, data []byte, err error)
	WriteJSON(value any) error
	Close() error
}

// Client maintains one Dhan market-feed connection at a time.
type Client struct {
	config ClientConfig
	dial   func(context.Context, string) (connection, error)
}

// NewClient creates a market-feed client. The client does not connect until Run.
func NewClient(config ClientConfig) (*Client, error) {
	if config.ClientID == "" {
		return nil, errors.New("market feed client ID is required")
	}
	if config.AccessToken == "" {
		return nil, errors.New("market feed access token is required")
	}
	if config.Endpoint == "" {
		config.Endpoint = defaultEndpoint
	}
	if config.ReconnectMin <= 0 {
		config.ReconnectMin = defaultReconnectMin
	}
	if config.ReconnectMax < config.ReconnectMin {
		config.ReconnectMax = defaultReconnectMax
	}
	if config.Dialer == nil {
		config.Dialer = websocket.DefaultDialer
	}

	return &Client{
		config: config,
		dial: func(ctx context.Context, endpoint string) (connection, error) {
			conn, _, err := config.Dialer.DialContext(ctx, endpoint, http.Header{})
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}, nil
}

// Run connects, reads binary packets, and reconnects until ctx is cancelled.
// The callback is invoked by this single reader loop, so event order is stable.
func (c *Client) Run(ctx context.Context, subscriptions []Subscription, onEvent func(Event)) error {
	if err := validateSubscriptions(subscriptions); err != nil {
		return err
	}
	if onEvent == nil {
		return errors.New("market feed event handler is required")
	}

	delay := c.config.ReconnectMin
	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := c.connect(ctx, subscriptions)
		if err == nil {
			delay = c.config.ReconnectMin
			err = readConnection(ctx, conn, subscriptions, onEvent)
			_ = conn.Close()
			if ctx.Err() != nil {
				return nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}

		if c.config.OnError != nil {
			c.config.OnError(err)
		}
		if !wait(ctx, delay) {
			return nil
		}
		if delay < c.config.ReconnectMax {
			delay *= 2
			if delay > c.config.ReconnectMax {
				delay = c.config.ReconnectMax
			}
		}
	}
}

func (c *Client) connect(ctx context.Context, subscriptions []Subscription) (connection, error) {
	endpoint, err := c.endpoint()
	if err != nil {
		return nil, err
	}
	conn, err := c.dial(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("connect to Dhan market feed: %w", err)
	}
	if c.config.OnConnect != nil {
		c.config.OnConnect()
	}

	for start := 0; start < len(subscriptions); start += maxInstrumentsPerRequest {
		end := start + maxInstrumentsPerRequest
		if end > len(subscriptions) {
			end = len(subscriptions)
		}
		batch := subscriptions[start:end]
		message := subscriptionRequest{
			RequestCode:     fullRequestCode,
			InstrumentCount: len(batch),
			InstrumentList:  make([]subscriptionInstrument, 0, len(batch)),
		}
		for _, subscription := range batch {
			message.InstrumentList = append(message.InstrumentList, subscriptionInstrument{
				ExchangeSegment: subscription.ExchangeSegment,
				SecurityID:      subscription.SecurityID,
			})
		}
		if err := conn.WriteJSON(message); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("subscribe to Dhan market feed: %w", err)
		}
	}
	if c.config.OnSubscribe != nil {
		c.config.OnSubscribe(len(subscriptions))
	}
	return conn, nil
}

func (c *Client) endpoint() (string, error) {
	parsed, err := url.Parse(c.config.Endpoint)
	if err != nil {
		return "", fmt.Errorf("parse market feed endpoint: %w", err)
	}
	query := parsed.Query()
	query.Set("version", "2")
	query.Set("token", c.config.AccessToken)
	query.Set("clientId", c.config.ClientID)
	query.Set("authType", "2")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func validateSubscriptions(subscriptions []Subscription) error {
	if len(subscriptions) == 0 {
		return errors.New("at least one market-feed subscription is required")
	}
	if len(subscriptions) > 5000 {
		return fmt.Errorf("Dhan market feed supports at most 5000 instruments, got %d", len(subscriptions))
	}

	seen := make(map[string]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription.ExchangeSegment == "" || subscription.SecurityID == "" {
			return errors.New("market-feed subscriptions need an exchange segment and security ID")
		}
		if exchangeCode(subscription.ExchangeSegment) == 255 {
			return fmt.Errorf("unsupported Dhan exchange segment %q", subscription.ExchangeSegment)
		}
		if _, err := strconv.ParseUint(subscription.SecurityID, 10, 32); err != nil {
			return fmt.Errorf("invalid market-feed security ID %q: %w", subscription.SecurityID, err)
		}
		key := subscription.ExchangeSegment + ":" + subscription.SecurityID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate market-feed subscription %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func readConnection(ctx context.Context, conn connection, subscriptions []Subscription, onEvent func(Event)) error {
	lookup := makeInstrumentLookup(subscriptions)
	closed := make(chan struct{})
	defer close(closed)

	// ReadMessage blocks, so close the socket when the caller cancels its
	// context. Closing the connection is what safely wakes the one reader.
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.WriteJSON(disconnectRequest{RequestCode: disconnectRequestCode})
			_ = conn.Close()
		case <-closed:
		}
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read Dhan market feed: %w", err)
		}
		if messageType != websocket.BinaryMessage {
			continue
		}

		events, err := decode(data, lookup)
		if err != nil {
			return err
		}
		for _, event := range events {
			onEvent(event)
		}
	}
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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

type instrument struct {
	ExchangeSegment string
	SecurityID      string
	Symbol          string
}

func makeInstrumentLookup(subscriptions []Subscription) map[string]instrument {
	lookup := make(map[string]instrument, len(subscriptions))
	for _, subscription := range subscriptions {
		lookup[subscriptionKey(subscription.ExchangeSegment, subscription.SecurityID)] = instrument{
			ExchangeSegment: subscription.ExchangeSegment,
			SecurityID:      subscription.SecurityID,
			Symbol:          subscription.Symbol,
		}
	}
	return lookup
}

func subscriptionKey(exchangeSegment, securityID string) string {
	return fmt.Sprintf("%d:%d", exchangeCode(exchangeSegment), parseSecurityID(securityID))
}
