// Package bus contains small, reusable concurrency primitives for distributing
// values between independent consumers.
package bus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var ErrClosed = errors.New("broadcast bus is closed")

type SubscriberID uint64

// BroadcastBus sends every published value to every active subscriber.
//
// Publish is lossless: it waits for each subscriber's bounded channel to have
// room, or returns when the context is cancelled. Publish calls are serialized
// so each subscriber observes one consistent event order.
type BroadcastBus[T any] interface {
	Subscribe(bufferSize int) (SubscriberID, <-chan T)
	Publish(ctx context.Context, event T) error
	Unsubscribe(id SubscriberID)
	Close()
}

// SubscriberStats describes the current queue state for one subscriber.
// DroppedEvents is retained as an explicit metric even though this bus never
// drops values; it makes the lossless behavior observable to callers.
type SubscriberStats struct {
	QueueDepth    int
	BufferSize    int
	DroppedEvents uint64
}

// BroadcastBusImpl is the default BroadcastBus implementation.
type BroadcastBusImpl[T any] struct {
	mu          sync.RWMutex
	publishMu   sync.Mutex
	closed      bool
	nextID      atomic.Uint64
	subscribers map[SubscriberID]*subscriber[T]
}

type subscriber[T any] struct {
	channel chan T
	done    chan struct{}
	stopErr error

	// sendMu keeps channel close and sends mutually exclusive. An unsubscribe
	// can therefore wait for an in-flight send before closing the channel.
	sendMu sync.Mutex

	dropped atomic.Uint64
}

// NewBroadcastBus creates an empty broadcast bus.
func NewBroadcastBus[T any]() *BroadcastBusImpl[T] {
	return &BroadcastBusImpl[T]{
		subscribers: make(map[SubscriberID]*subscriber[T]),
	}
}

func (b *BroadcastBusImpl[T]) Subscribe(bufferSize int) (SubscriberID, <-chan T) {
	if bufferSize < 1 {
		panic("broadcast bus subscriber buffer size must be positive")
	}

	sub := &subscriber[T]{
		channel: make(chan T, bufferSize),
		done:    make(chan struct{}),
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		close(sub.done)
		close(sub.channel)
		return 0, sub.channel
	}

	id := SubscriberID(b.nextID.Add(1))
	b.subscribers[id] = sub
	return id, sub.channel
}

func (b *BroadcastBusImpl[T]) Publish(ctx context.Context, event T) error {
	if ctx == nil {
		return errors.New("broadcast bus publish context is nil")
	}

	// A single publisher lock gives all subscribers the same ordering even when
	// several producers call Publish concurrently.
	b.publishMu.Lock()
	defer b.publishMu.Unlock()

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrClosed
	}
	subscribers := make([]*subscriber[T], 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		subscribers = append(subscribers, sub)
	}
	b.mu.RUnlock()

	for _, sub := range subscribers {
		if err := sub.publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (s *subscriber[T]) publish(ctx context.Context, event T) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	select {
	case s.channel <- event:
		return nil
	case <-s.done:
		return s.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *BroadcastBusImpl[T]) Unsubscribe(id SubscriberID) {
	sub := b.remove(id)
	if sub == nil {
		return
	}

	sub.stopErr = nil
	close(sub.done)
	sub.sendMu.Lock()
	close(sub.channel)
	sub.sendMu.Unlock()
}

func (b *BroadcastBusImpl[T]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subscribers := make([]*subscriber[T], 0, len(b.subscribers))
	for id, sub := range b.subscribers {
		subscribers = append(subscribers, sub)
		delete(b.subscribers, id)
	}
	b.mu.Unlock()

	for _, sub := range subscribers {
		sub.stopErr = ErrClosed
		close(sub.done)
		sub.sendMu.Lock()
		close(sub.channel)
		sub.sendMu.Unlock()
	}
}

func (b *BroadcastBusImpl[T]) remove(id SubscriberID) *subscriber[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	sub := b.subscribers[id]
	delete(b.subscribers, id)
	return sub
}

// Stats returns queue depth and loss metrics for an active subscriber.
func (b *BroadcastBusImpl[T]) Stats(id SubscriberID) (SubscriberStats, bool) {
	b.mu.RLock()
	sub, ok := b.subscribers[id]
	b.mu.RUnlock()
	if !ok {
		return SubscriberStats{}, false
	}
	return SubscriberStats{
		QueueDepth:    len(sub.channel),
		BufferSize:    cap(sub.channel),
		DroppedEvents: sub.dropped.Load(),
	}, true
}
