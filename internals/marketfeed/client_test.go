package marketfeed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestClientSubscribesAndStopsWithContext(t *testing.T) {
	conn := newFakeConnection()
	client, err := NewClient(ClientConfig{
		ClientID:     "client",
		AccessToken:  "token",
		ReconnectMin: time.Millisecond,
		ReconnectMax: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.dial = func(context.Context, string) (connection, error) {
		return conn, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx, []Subscription{{
			ExchangeSegment: "MCX_COMM",
			SecurityID:      "571445",
			Symbol:          "GOLDM NOV FUT",
		}}, func(Event) {})
	}()

	if !conn.waitForWrite(time.Second) {
		t.Fatal("client did not send subscription")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after context cancellation")
	}
	if !conn.hasDisconnectRequest() {
		t.Fatal("client did not send a graceful disconnect request")
	}

	request, ok := conn.firstWrite().(subscriptionRequest)
	if !ok {
		t.Fatalf("first write = %T, want subscriptionRequest", conn.firstWrite())
	}
	if request.RequestCode != fullRequestCode || request.InstrumentCount != 1 || request.InstrumentList[0].ExchangeSegment != "MCX_COMM" {
		t.Fatalf("subscription request = %#v", request)
	}
}

func TestClientReconnectsAndResubscribes(t *testing.T) {
	first := newFakeConnection()
	first.readResult = readResult{err: errors.New("connection lost")}
	second := newFakeConnection()

	client, err := NewClient(ClientConfig{
		ClientID:     "client",
		AccessToken:  "token",
		ReconnectMin: time.Millisecond,
		ReconnectMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	var mu sync.Mutex
	dials := 0
	client.dial = func(context.Context, string) (connection, error) {
		mu.Lock()
		defer mu.Unlock()
		dials++
		if dials == 1 {
			return first, nil
		}
		return second, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx, []Subscription{{ExchangeSegment: "MCX_COMM", SecurityID: "1"}}, func(Event) {})
	}()

	deadline := time.After(time.Second)
	for {
		mu.Lock()
		count := dials
		mu.Unlock()
		if count >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("client did not reconnect")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after reconnect")
	}
	if len(first.writes()) == 0 || len(second.writes()) == 0 {
		t.Fatalf("subscription writes = first %d, second %d", len(first.writes()), len(second.writes()))
	}
}

type readResult struct {
	messageType int
	data        []byte
	err         error
}

type fakeConnection struct {
	mu         sync.Mutex
	writeLog   []any
	readResult readResult
	closed     chan struct{}
	once       sync.Once
}

func newFakeConnection() *fakeConnection {
	return &fakeConnection{closed: make(chan struct{})}
}

func (f *fakeConnection) ReadMessage() (int, []byte, error) {
	if f.readResult.err != nil {
		return f.readResult.messageType, f.readResult.data, f.readResult.err
	}
	<-f.closed
	return 0, nil, errors.New("fake connection closed")
}

func (f *fakeConnection) WriteJSON(value any) error {
	f.mu.Lock()
	f.writeLog = append(f.writeLog, value)
	f.mu.Unlock()
	return nil
}

func (f *fakeConnection) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeConnection) waitForWrite(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(f.writes()) > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func (f *fakeConnection) writes() []any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]any(nil), f.writeLog...)
}

func (f *fakeConnection) firstWrite() any {
	writes := f.writes()
	if len(writes) == 0 {
		return nil
	}
	return writes[0]
}

func (f *fakeConnection) hasDisconnectRequest() bool {
	for _, value := range f.writes() {
		if request, ok := value.(disconnectRequest); ok && request.RequestCode == disconnectRequestCode {
			return true
		}
	}
	return false
}
