package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ErrorHandler[T any] func(event Event[T], err error, value []byte)
type CallbackFunc[T any] func(context.Context, Event[T]) error
type EventParser[T any] func(event *clientv3.Event, codec codecs.Codec[T]) (p Event[T], skip bool, err error)

type Event[T any] struct {
	Key      string
	Type     EventType
	Current  T
	Previous T

	empty struct {
		current  bool
		previous bool
	}
}

func (e Event[T]) Empty() bool {
	return e.empty.current && e.empty.previous
}

func (e Event[T]) HasCurrent() bool {
	return !e.empty.current
}

func (e Event[T]) HasPrevious() bool {
	return !e.empty.previous
}

type watcher[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	errorHandler ErrorHandler[T]

	codec codecs.Codec[T]

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

	prefixTrimFunc func(string) string
	eventParser    EventParser[T]
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
}

type CallbackOption[T any] func(*Callbacks[T])

func Created[T any](fn CallbackFunc[T]) CallbackOption[T] {
	return func(c *Callbacks[T]) { c.Created = fn }
}

func Modified[T any](fn CallbackFunc[T]) CallbackOption[T] {
	return func(c *Callbacks[T]) { c.Modified = fn }
}

func Deleted[T any](fn CallbackFunc[T]) CallbackOption[T] {
	return func(c *Callbacks[T]) { c.Deleted = fn }
}

func All[T any](fn CallbackFunc[T]) CallbackOption[T] {
	return func(c *Callbacks[T]) { c.All = fn }
}

// Start takes a list of callbacks
// e.g .Start(Created(fn), Modified(fn), Deleted(fn), All(fn))
// it may return an error if no callbacks are supplied
// it may block if WithBlock() has been supplied during the creation of the Watcher
// If multiple of the same type of callback is specified the last one will be used.
// e.g .Start(Created(fn1), Created(fn2)) will use fn2
func (w *Watcher[T]) Start(opts ...CallbackOption[T]) error {
	if w == nil {
		return errors.New("watcher has not been created yet")
	}

	if w.watcher == nil {
		return fmt.Errorf("watcher is not initialized")
	}

	w.watcher.mu.RLock()
	if w.watcher.started {
		w.watcher.mu.RUnlock()
		return errors.New("start has already been called on this watcher")
	}
	w.watcher.mu.RUnlock()

	var err error
	w.watcher.startOnce.Do(func() {

		w.watcher.mu.Lock()
		for _, opt := range opts {
			opt(&w.watcher.callbacks)
		}
		w.watcher.started = true

		w.watcher.mu.Unlock()

		if w.watcher.callbacks.Created == nil &&
			w.watcher.callbacks.Modified == nil &&
			w.watcher.callbacks.Deleted == nil &&
			w.watcher.callbacks.All == nil {
			err = fmt.Errorf("at least one callback must be defined")
			return
		}

		w.watcher.start()
	})

	return err
}

func (w *Watcher[T]) Close() error {
	if w == nil {
		return nil
	}

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
		if ctx == nil {
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

func WithPrefixTrimFunc[T any](trimFunc func(string) string) opFunc[T] {
	return func(w *watcher[T]) {
		w.prefixTrimFunc = trimFunc
	}
}

func WithEventParser[T any](parser EventParser[T]) opFunc[T] {
	return func(w *watcher[T]) {
		if parser == nil {
			return
		}

		w.eventParser = parser
	}
}

// NewWatch allows you to register a typesafe callback that will be fired when a key, or prefix is modified in etcd.
func NewWatch[T any](
	etcd *clientv3.Client,
	key string,
	codec codecs.Codec[T],
	opFuncs ...opFunc[T],
) *Watcher[T] {

	s := &watcher[T]{
		// this is set as the default context in case no context is supplied
		ctx: context.Background(),

		queues: make(map[string]chan Event[T]),

		codec: codec,
		etcd:  etcd,
		key:   key,

		// do nothing as the default error handler
		errorHandler: func(event Event[T], err error, value []byte) {},
	}

	s.eventParser = s.defaultEventParser

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
			event, skip, err := s.eventParser(rawEvent, s.codec)
			if err != nil {
				b, _ := json.MarshalIndent(event.Current, "", "    ")
				s.errorHandler(event, fmt.Errorf("failed to parse event: %w", err), b)
				continue
			}

			if skip {
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

// the default parser never skips parsing events
func (s *watcher[T]) defaultEventParser(event *clientv3.Event, codec codecs.Codec[T]) (p Event[T], skip bool, err error) {
	p.Key = string(event.Kv.Key)
	if s.prefixTrimFunc != nil {
		p.Key = s.prefixTrimFunc(string(event.Kv.Key))
	}

	switch event.Type {
	case mvccpb.DELETE:
		p.Type = DELETED

		p.empty.current = true

		if event.PrevKv == nil {
			return p, false, fmt.Errorf("previous key value is nil for key %q deleted event", p.Key)
		}

		p.Previous, err = codec.Decode(event.PrevKv.Value)
		if err != nil {
			return p, false, fmt.Errorf("failed to unmarshal previous entry for key %q deleted event: %w", p.Key, err)
		}

	case mvccpb.PUT:
		p.Type = CREATED

		p.Current, err = codec.Decode(event.Kv.Value)
		if err != nil {
			return p, false, fmt.Errorf("failed to unmarshal current key %q event: %w", p.Key, err)
		}
		p.empty.previous = true

		if event.IsModify() {
			p.Type = MODIFIED
			p.empty.previous = false

			if event.PrevKv == nil {
				return p, false, fmt.Errorf("previous key value is nil for key %q modified event", p.Key)
			}
			p.Previous, err = codec.Decode(event.PrevKv.Value)
			if err != nil {
				return p, false, fmt.Errorf("failed to unmarshal previous key %q previous data: %q err: %w", p.Key, event.PrevKv.Value, err)
			}

		}

	default:
		return p, false, fmt.Errorf("invalid mvccpb type: %q, this is a bug", event.Type)
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
