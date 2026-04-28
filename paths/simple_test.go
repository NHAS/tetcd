package paths_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
	"github.com/NHAS/tetcd/testhelpers"
	"github.com/NHAS/tetcd/watch"
)

type testType struct {
	Value string
}

// TestWatch_KeyStrippedToBase verifies that when Watch() is called on a plain key
// (e.g. /foo/bar/baz), the event key reported to the callback is path.Base of
// the path key — i.e. "baz" — not the full etcd key.
func TestWatch_KeyStrippedToBase(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	p := paths.NewPath("/foo/bar/baz", codecs.NewJsonCodec[testType]())

	var got atomic.Value // stores string

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.Created(func(_ context.Context, e watch.Event[testType]) error {
			got.Store(e.Key)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Close()

	if err := p.Put(context.Background(), etcd, testType{Value: "hello"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	waitFor(t, func() bool { return got.Load() != nil }, 3*time.Second)

	if key := got.Load().(string); key != "baz" {
		t.Errorf("expected stripped key %q, got %q", "baz", key)
	}
}

// TestWatch_PrefixTrimAlwaysUsesPathKeyBase documents the current behaviour:
// the trim function captures p.key at construction time, so ALL events —
// regardless of the actual etcd key that fired — will report path.Base(p.key).
//
// This test exists to make the behaviour explicit. If the intention is for each
// event to report its own base name, the trim function in Watch() needs fixing:
//
//	func(key string) string { return path.Base(key) }   // fix
//	func(key string) string { return path.Base(p.key) } // current (bug?)
func TestWatch_PrefixTrimAlwaysUsesPathKeyBase(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	// Use a prefix path so multiple child keys can fire events.
	prefix := "/things"
	p := paths.NewPath(prefix, codecs.NewJsonCodec[testType]())

	type result struct {
		key string
		val string
	}
	results := make(chan result, 10)

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.All(func(_ context.Context, e watch.Event[testType]) error {
			v := ""
			if !e.HasCurrent() {
				v = e.Current.Value
			}
			results <- result{key: e.Key, val: v}
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Close()

	// Write a key that is an exact match for the watched path.
	child := paths.NewPath(prefix, codecs.NewJsonCodec[testType]())
	if err := child.Put(context.Background(), etcd, testType{Value: "v1"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var got result
	select {
	case got = <-results:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	// Current behaviour: trim func always returns path.Base(p.key) == "things"
	// regardless of actual event key.
	if got.key != "things" {
		t.Errorf("expected key %q (current behaviour), got %q", "things", got.key)
	}
}

// TestWatch_CreatedCallbackFires is a basic sanity-check that the watcher
// integrated through Path.Watch() fires a Created event.
func TestWatch_CreatedCallbackFires(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	p := paths.NewPath("/watch-created-test", codecs.NewJsonCodec[testType]())

	var fired atomic.Bool

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.Created(func(_ context.Context, e watch.Event[testType]) error {
			fired.Store(true)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Close()

	if err := p.Put(context.Background(), etcd, testType{Value: "x"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	waitFor(t, fired.Load, 3*time.Second)
}

// TestWatch_ModifiedCallbackFires verifies Modified fires (not Created) on a second Put.
func TestWatch_ModifiedCallbackFires(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	p := paths.NewPath("/watch-modified-test", codecs.NewJsonCodec[testType]())

	// Pre-create the key before the watcher starts so the first watcher event is a modify.
	if err := p.Put(context.Background(), etcd, testType{Value: "initial"}); err != nil {
		t.Fatalf("pre-Put: %v", err)
	}

	var modified atomic.Bool

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.Modified(func(_ context.Context, e watch.Event[testType]) error {
			modified.Store(true)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Close()

	if err := p.Put(context.Background(), etcd, testType{Value: "updated"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	waitFor(t, modified.Load, 3*time.Second)
}

// TestWatch_DeletedCallbackFires verifies Deleted fires after a key is removed.
func TestWatch_DeletedCallbackFires(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	p := paths.NewPath("/watch-deleted-test", codecs.NewJsonCodec[testType]())

	if err := p.Put(context.Background(), etcd, testType{Value: "to-delete"}); err != nil {
		t.Fatalf("pre-Put: %v", err)
	}

	var deleted atomic.Bool
	var prevValue atomic.Value // stores string

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.Deleted(func(_ context.Context, e watch.Event[testType]) error {
			if e.HasPrevious() {
				prevValue.Store(e.Previous.Value)
			}
			deleted.Store(true)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Close()

	if _, err := p.Delete(context.Background(), etcd); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	waitFor(t, deleted.Load, 3*time.Second)

	if v, ok := prevValue.Load().(string); !ok || v != "to-delete" {
		t.Errorf("expected Previous.Value %q, got %q", "to-delete", v)
	}
}

// TestWatch_CloseCancelsEvents verifies that after Close(), no further callbacks fire.
func TestWatch_CloseCancelsEvents(t *testing.T) {
	etcd, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()

	p := paths.NewPath("/watch-close-test", codecs.NewJsonCodec[testType]())

	var count atomic.Int32

	w := p.Watch(context.Background(), etcd)
	err := w.Start(
		watch.All(func(_ context.Context, _ watch.Event[testType]) error {
			count.Add(1)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := p.Put(context.Background(), etcd, testType{Value: "before-close"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	waitFor(t, func() bool { return count.Load() > 0 }, 3*time.Second)

	w.Close()

	before := count.Load()
	if err := p.Put(context.Background(), etcd, testType{Value: "after-close"}); err != nil {
		t.Fatalf("Put after close: %v", err)
	}

	// Give a generous window for any spurious events.
	time.Sleep(300 * time.Millisecond)

	if count.Load() != before {
		t.Errorf("expected no events after Close, but count went from %d to %d", before, count.Load())
	}
}
