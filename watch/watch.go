package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ErrorHandler[T any] func(event Event[T], err error, value []byte)
type CallbackFunc[T any] func(context.Context, Event[T]) error

type Event[T any] struct {
	Key      string
	Type     EventType
	Current  *T
	Previous *T
}

type watcher[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	errorHandler ErrorHandler[T]

	key string

	etcd *clientv3.Client

	closeOnce sync.Once
	startOnce sync.Once

	mu     sync.RWMutex
	queues map[string]chan Event[T]

	blocking bool
	prefix   bool

	callbacks Callbacks[T]

	started bool
}

// The Callbacks[T] concurrency model is:
// Same key -> one queue, one consumer goroutine, callbacks are serialized
// Different keys -> different queues, different consumer goroutines, callbacks can overlap
// A minimal example:
// 1. Prefix watch gets event for /a
// 2. enqueue("/a") creates A.queue and starts consumer goroutine A.consumer()
// 3. A.consumer() enters s.applyEvent(...) and the callback blocks
// 4. Another event arrives for /b
// 5. enqueue("/b") creates B.queue and starts consumer goroutine B.consumer()
// 6. B.consumer() also enters the same s.applyEvent(...)
//
// As such, try to avoid locking inside the callbacks as it may end up stalling a large number of keys which will eventually hang the watch
type Callbacks[T any] struct {
	Created  CallbackFunc[T]
	Modified CallbackFunc[T]
	Deleted  CallbackFunc[T]
	All      CallbackFunc[T]
}

type Watcher[T any] struct {
	watcher *watcher[T]

	Callbacks[T]
}

func (w *Watcher[T]) Start() error {
	if w.watcher == nil {
		return fmt.Errorf("watcher is not initialized")
	}

	if w.watcher.started {
		return errors.New("start has already been called on this watcher")
	}

	if w.Created == nil && w.Modified == nil && w.Deleted == nil && w.All == nil {
		return fmt.Errorf("at least one callback must be defined")
	}

	w.watcher.startOnce.Do(func() {
		w.watcher.mu.Lock()
		w.watcher.callbacks = w.Callbacks
		w.watcher.mu.Unlock()

		w.watcher.started = true
		w.watcher.start()
	})

	return nil
}

func (w *Watcher[T]) Close() error {
	if w.watcher != nil {
		return w.watcher.close()
	}
	return nil
}

type opFunc[T any] func(*watcher[T])

func WithErrorHandler[T any](errorHandler ErrorHandler[T]) opFunc[T] {
	return func(w *watcher[T]) {
		if errorHandler == nil {
			return
		}

		w.errorHandler = errorHandler
	}
}

func WithContext[T any](ctx context.Context) opFunc[T] {
	return func(w *watcher[T]) {
		if w.ctx == nil {
			return
		}

		w.ctx = ctx
	}
}

func WithPrefix[T any]() opFunc[T] {
	return func(w *watcher[T]) {
		w.prefix = true
	}
}

func WithBlock[T any]() opFunc[T] {
	return func(w *watcher[T]) {
		w.blocking = true
	}
}

// NewWatch allows you to register a typesafe callback that will be fired when a key, or prefix is modified in etcd.
func NewWatch[T any](
	etcd *clientv3.Client,
	key string,
	opFuncs ...opFunc[T],
) *Watcher[T] {

	s := &watcher[T]{
		// this is set as the default context in case no context is supplied
		ctx: context.Background(),

		queues: make(map[string]chan Event[T]),

		etcd: etcd,
		key:  key,

		// do nothing as the default error handler
		errorHandler: func(event Event[T], err error, value []byte) {},
	}

	for _, op := range opFuncs {
		op(s)
	}

	s.ctx, s.cancel = context.WithCancel(s.ctx)

	return &Watcher[T]{watcher: s}
}

func (s *watcher[T]) start() {
	ops := []clientv3.OpOption{clientv3.WithPrevKV()}
	if s.prefix {
		ops = append(ops, clientv3.WithPrefix())
	}

	wc := s.etcd.Watch(s.ctx, s.key, ops...)

	if s.blocking {
		s.producer(wc)
		return
	}

	go s.producer(wc)

}

func (s *watcher[T]) consume(key string, q chan Event[T]) {

	go func() {
		const idleTimeout = 10 * time.Second

		for {
			select {
			case <-s.ctx.Done():
				// close() sets the context as done and also clears queues so gc will clean up for us
				return
			case event, ok := <-q:
				if !ok {
					return
				}

				func() {
					defer func() {
						if err := recover(); err != nil {
							s.errorHandler(event, fmt.Errorf("panic for key %q recovered: %v", key, err), nil)
						}
					}()

					s.applyEvent(s.ctx, event)
				}()

				// if we get no events for 10 seconds on this queue, shut down the consumer go routine
			case <-time.After(idleTimeout):

				s.mu.Lock()
				if len(q) > 0 {
					s.mu.Unlock()
					continue
				}
				defer s.mu.Unlock()

				delete(s.queues, key)
				return
			}
		}

	}()

}

func (s *watcher[T]) producer(wc clientv3.WatchChan) {
	for watchEvent := range wc {
		if s.ctx.Err() != nil {
			return
		}

		if err := watchEvent.Err(); err != nil {
			s.errorHandler(Event[T]{}, fmt.Errorf("got watch error key %q: %w", s.key, err), nil)
			return
		}

		for _, rawEvent := range watchEvent.Events {
			event, err := parseEvent[T](rawEvent)
			if err != nil {
				b, _ := json.MarshalIndent(event.Current, "", "    ")
				s.errorHandler(event, fmt.Errorf("failed to parse event: %w", err), b)
				continue
			}

			s.enqueue(event)
		}
	}
}

func (s *watcher[T]) enqueue(event Event[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ctx.Err() != nil {
		return
	}

	q, ok := s.queues[event.Key]
	if !ok {
		q = make(chan Event[T], 128)
		s.queues[event.Key] = q

		q <- event
		s.consume(event.Key, q)
		return
	}

	select {
	case q <- event:
	default:
		// make sure that the error handler cannot block
		go s.errorHandler(event, fmt.Errorf("queue for key %q is full", event.Key), nil)
	}
}

func (s *watcher[T]) applyEvent(ctx context.Context, event Event[T]) {
	apply := func(callbackType EventType, cb CallbackFunc[T], e Event[T]) {
		if cb != nil {
			if err := cb(ctx, e); err != nil {
				value, _ := json.Marshal(e.Current)
				s.errorHandler(e, fmt.Errorf("callback %q event failed: %w", callbackType.String(), err), value)
			}
		}
	}

	switch event.Type {
	case CREATED:
		apply(CREATED, s.callbacks.Created, event)
	case MODIFIED:
		apply(MODIFIED, s.callbacks.Modified, event)
	case DELETED:
		apply(DELETED, s.callbacks.Deleted, event)
	}

	apply(ALL, s.callbacks.All, event)
}

func parseEvent[T any](event *clientv3.Event) (p Event[T], err error) {
	p.Key = string(event.Kv.Key)

	switch event.Type {
	case mvccpb.DELETE:
		p.Type = DELETED
		if event.PrevKv == nil {
			return p, fmt.Errorf("previous key value is nil for key %q deleted event", p.Key)
		}

		err = json.Unmarshal(event.PrevKv.Value, &p.Previous)
		if err != nil {
			return p, fmt.Errorf("failed to unmarshal previous entry for key %q deleted event: %w", p.Key, err)
		}
	case mvccpb.PUT:
		p.Type = CREATED
		err = json.Unmarshal(event.Kv.Value, &p.Current)
		if err != nil {
			return p, fmt.Errorf("failed to unmarshal current key %q event: %w", p.Key, err)
		}
		if event.IsModify() {
			p.Type = MODIFIED
			if event.PrevKv == nil {
				return p, fmt.Errorf("previous key value is nil for key %q modified event", p.Key)
			}
			err = json.Unmarshal(event.PrevKv.Value, &p.Previous)
			if err != nil {
				return p, fmt.Errorf("failed to unmarshal previous key %q previous data: %q err: %w", p.Key, event.PrevKv.Value, err)
			}
		}
	default:
		return p, fmt.Errorf("invalid mvccpb type: %q, this is a bug", event.Type)
	}

	return
}

func (s *watcher[T]) close() error {

	s.closeOnce.Do(func() {

		s.mu.Lock()
		defer s.mu.Unlock()

		s.cancel()
		clear(s.queues)
	})
	return nil
}
