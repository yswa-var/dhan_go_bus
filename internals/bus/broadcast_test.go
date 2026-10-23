package bus

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestBroadcastsEveryEventInOrder(t *testing.T) {
	broadcast := NewBroadcastBus[int]()
	defer broadcast.Close()

	channels := make([]<-chan int, 3)
	for i := range channels {
		_, channels[i] = broadcast.Subscribe(4)
	}

	for event := 1; event <= 4; event++ {
		if err := broadcast.Publish(context.Background(), event); err != nil {
			t.Fatalf("Publish() error = %v", err)
		}
	}

	for i, channel := range channels {
		got := make([]int, 0, 4)
		for range 4 {
			got = append(got, <-channel)
		}
		if want := []int{1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
			t.Errorf("subscriber %d events = %v, want %v", i, got, want)
		}
	}
}

func TestConcurrentSubscriptionsAreSafe(t *testing.T) {
	broadcast := NewBroadcastBus[int]()
	defer broadcast.Close()

	const subscriberCount = 32
	var waitGroup sync.WaitGroup
	channels := make([]<-chan int, subscriberCount)
	for i := range channels {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			_, channels[index] = broadcast.Subscribe(1)
		}(i)
	}
	waitGroup.Wait()

	for _, channel := range channels {
		if channel == nil {
			t.Fatal("concurrent subscription returned a nil channel")
		}
	}
}

func TestUnsubscribeStopsAndClosesSubscriber(t *testing.T) {
	broadcast := NewBroadcastBus[int]()
	defer broadcast.Close()

	id, events := broadcast.Subscribe(1)
	if err := broadcast.Publish(context.Background(), 1); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	if got := <-events; got != 1 {
		t.Fatalf("queued event = %d, want 1", got)
	}
	broadcast.Unsubscribe(id)

	if _, ok := <-events; !ok {
		return
	}
	t.Fatal("subscriber received an event after Unsubscribe")
}

func TestSlowSubscriberBlocksUntilSpaceOrCancellation(t *testing.T) {
	broadcast := NewBroadcastBus[int]()
	defer broadcast.Close()

	_, events := broadcast.Subscribe(1)
	if err := broadcast.Publish(context.Background(), 1); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- broadcast.Publish(ctx, 2) }()

	select {
	case err := <-result:
		t.Fatalf("blocked Publish() returned early with %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	stats, ok := broadcast.Stats(1)
	if !ok || stats.QueueDepth != 1 || stats.BufferSize != 1 || stats.DroppedEvents != 0 {
		t.Fatalf("subscriber stats = %#v, exists = %v", stats, ok)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Publish() error = %v, want context.Canceled", err)
	}
	if got := <-events; got != 1 {
		t.Fatalf("queued event = %d, want 1", got)
	}
}

func TestCloseClosesAllSubscriberChannels(t *testing.T) {
	broadcast := NewBroadcastBus[string]()
	_, first := broadcast.Subscribe(1)
	_, second := broadcast.Subscribe(1)

	broadcast.Close()
	broadcast.Close()

	for name, events := range map[string]<-chan string{"first": first, "second": second} {
		if _, ok := <-events; ok {
			t.Errorf("%s subscriber channel is still open", name)
		}
	}
	if err := broadcast.Publish(context.Background(), "event"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish() after Close() error = %v, want ErrClosed", err)
	}
}

func TestCloseWakesBlockedPublish(t *testing.T) {
	broadcast := NewBroadcastBus[int]()
	defer broadcast.Close()

	_, events := broadcast.Subscribe(1)
	if err := broadcast.Publish(context.Background(), 1); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- broadcast.Publish(context.Background(), 2) }()
	select {
	case err := <-result:
		t.Fatalf("blocked Publish() returned early with %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	broadcast.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Publish() during Close() error = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not wake blocked Publish()")
	}

	if got := <-events; got != 1 {
		t.Fatalf("queued event = %d, want 1", got)
	}
}
