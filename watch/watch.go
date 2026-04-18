package watch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ErrorHandler[T any] func(event Event[T], err error, value []byte)

type Watcher[T any] struct {
	cancel       context.CancelFunc
	callbacks    WatcherCallbacks[T]
	errorHandler ErrorHandler[T]

	close sync.Once

	mu      sync.Mutex
	queues  map[string]chan Event[T]
	workers int

	work chan chan Event[T]
}

type Event[T any] struct {
	Key      string
	Type     EventType
	Current  *T
	Previous *T
}

type CallbackFunc[T any] func(context.Context, Event[T]) error

type WatcherCallbacks[T any] struct {
	Created  []CallbackFunc[T]
	Modified []CallbackFunc[T]
	Deleted  []CallbackFunc[T]
	All      CallbackFunc[T]
}

func defaultErrorHandler[T any](event Event[T], err error, value []byte) {
	fmt.Printf("watch error: %v, value: %s\n", err, value)
}

// Watch allows you to register a typesafe callback that will be fired when a key, or prefix is modified in etcd.
// workerPoolSize controls the number of concurrent workers processing per-key queues.
// onError is called when errors occur; if nil a default handler is used.
func Watch[T any](
	ctx context.Context,
	etcd *clientv3.Client,
	key string,
	prefix bool,
	workerPoolSize int,
	onError ErrorHandler[T],
	callbacks WatcherCallbacks[T]) (*Watcher[T], error) {

	if workerPoolSize < 1 {
		workerPoolSize = 4
	}

	if onError == nil {
		onError = defaultErrorHandler
	}

	ctx, cancel := context.WithCancel(ctx)

	s := &Watcher[T]{
		cancel:       cancel,
		callbacks:    callbacks,
		errorHandler: onError,
		queues:       make(map[string]chan Event[T]),
		workers:      workerPoolSize,
		// Work channel distributes per-key queues to the worker pool

		work: make(chan chan Event[T], 64),
	}

	// Start fixed worker pool
	for i := 0; i < workerPoolSize; i++ {
		go func() {
			for queue := range s.work {
				// Drain all events in this key's queue before picking up next work item
				for {
					select {
					case event, ok := <-queue:
						if !ok {
							goto done
						}
						s.applyEvent(ctx, event)
					default:
						goto done
					}
				}
			done:
			}
		}()
	}

	ops := []clientv3.OpOption{clientv3.WithPrevKV()}
	if prefix {
		ops = append(ops, clientv3.WithPrefix())
	}

	wc := etcd.Watch(ctx, key, ops...)

	go func() {

		for watchEvent := range wc {
			if ctx.Err() != nil {
				return
			}

			if err := watchEvent.Err(); err != nil {
				s.errorHandler(Event[T]{}, fmt.Errorf("got watch error key %q: %w", key, err), nil)
				return
			}

			for _, event := range watchEvent.Events {
				p, err := parseEvent[T](event)
				if err != nil {
					b, _ := json.MarshalIndent(p.Current, "", "    ")
					s.errorHandler(p, fmt.Errorf("failed to parse event: %w", err), b)
					continue
				}

				q := s.getOrCreateQueue(p.Key)
				select {
				case q <- p:
				case <-ctx.Done():
					return
				}

				// Schedule this key's queue for processing
				select {
				case s.work <- q:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return s, nil
}

func (s *Watcher[T]) getOrCreateQueue(key string) chan Event[T] {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[key]
	if !ok {
		q = make(chan Event[T], 256)
		s.queues[key] = q
	}
	return q
}

func (s *Watcher[T]) applyEvent(ctx context.Context, event Event[T]) {
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
		for _, f := range s.callbacks.Created {
			apply(CREATED, f, event)
		}
	case MODIFIED:
		for _, f := range s.callbacks.Modified {
			apply(MODIFIED, f, event)
		}
	case DELETED:
		for _, f := range s.callbacks.Deleted {
			apply(DELETED, f, event)
		}
	}

	if s.callbacks.All != nil {
		apply(ALL, s.callbacks.All, event)
	}
}

func parseEvent[T any](event *clientv3.Event) (p Event[T], err error) {
	p.Key = string(event.Kv.Key)

	switch event.Type {
	case mvccpb.DELETE:
		p.Type = DELETED
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

func (s *Watcher[T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.close.Do(func() {
		s.cancel()
		close(s.work)

		for _, q := range s.queues {
			close(q)
		}
		clear(s.queues)
	})
	return nil
}
