package paths_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"
	"github.com/NHAS/tetcd/testhelpers"
	"github.com/NHAS/tetcd/watch"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func seedMap[T any](t *testing.T, ctx context.Context, cli *clientv3.Client, m paths.MapPath[T], data map[string]T) {
	t.Helper()
	for k, v := range data {

		if err := m.Key(k).Put(ctx, cli, v); err != nil {
			t.Fatalf("failed to seed key %q: %v", k, err)
		}
	}
}

func TestMapPath_Prefix(t *testing.T) {
	m := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[string](), false)
	if got := m.Prefix(); got != "wag/Acls/Groups/" {
		t.Errorf("Prefix() = %q, want %q", got, "wag/Acls/Groups/")
	}
}

func TestMapPath_Key(t *testing.T) {
	m := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[string](), false)
	if got := m.Key("admins").Key(); got != "wag/Acls/Groups/admins" {
		t.Errorf("Key() = %q, want %q", got, "wag/Acls/Groups/admins")
	}
}

func TestMapPath_Keys(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[string](), false)
	seedMap(t, ctx, cli, m, map[string]string{
		"admins":  "alice,bob",
		"viewers": "carol",
	})

	keys, err := m.Keys(ctx, cli)
	if err != nil {
		t.Fatalf("Keys() error = %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("Keys() returned %d keys, want 2", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range []string{"admins", "viewers"} {
		if !keySet[expected] {
			t.Errorf("Keys() missing %q", expected)
		}
	}
}

func TestMapPath_Keys_Empty(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/Empty", codecs.NewJsonCodec[string](), false)

	keys, err := m.Keys(ctx, cli)
	if err != nil {
		t.Fatalf("Keys() error = %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("Keys() = %v, want empty", keys)
	}
}

func TestMapPath_List(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/List", codecs.NewJsonCodec[string](), false)
	want := map[string]string{
		"admins":  "alice,bob",
		"viewers": "carol",
	}
	seedMap(t, ctx, cli, m, want)

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(result.Values) != len(want) {
		t.Fatalf("List() returned %d entries, want %d", len(result.Values), len(want))
	}
	for k, v := range want {
		if result.Values[k] != v {
			t.Errorf("List()[%q] = %q, want %q", k, result.Values[k], v)
		}
	}
}

func TestMapPath_List_Empty(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/ListEmpty", codecs.NewJsonCodec[string](), false)

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(result.Values) != 0 {
		t.Errorf("List() = %v, want empty map", result.Values)
	}
}

func TestMapPath_Put(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	type TestObject struct {
		Something string
		Number    int

		Nested struct {
			Abc bool
		}
	}

	m := paths.NewMapPath("wag/Acls/Settings", codecs.NewJsonCodec[TestObject](), false)

	adminObject := TestObject{Something: "alice", Number: 1, Nested: struct{ Abc bool }{Abc: true}}
	viewiersObject := TestObject{Something: "carol", Number: 2, Nested: struct{ Abc bool }{Abc: false}}

	seedMap(t, ctx, cli, m, map[string]TestObject{
		"admins":  adminObject,
		"viewers": viewiersObject,
	})

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after Put() error = %v", err)
	}
	if result.Values["admins"] != adminObject {
		t.Error("Put() did not correctly set key 'admins'")
	}
	if result.Values["viewers"] != viewiersObject {
		t.Error("Put() did not correctly set key 'viewers'")
	}
}

func TestMapPath_DeleteAll(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/DeleteAll", codecs.NewJsonCodec[string](), false)
	seedMap(t, ctx, cli, m, map[string]string{
		"admins":  "alice",
		"viewers": "carol",
		"editors": "dave",
	})

	deleted, err := m.DeleteAll(ctx, cli)
	if err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}
	if deleted.Count != 3 {
		t.Errorf("DeleteAll() deleted %d, want 3", deleted.Count)
	}

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after DeleteAll() error = %v", err)
	}
	if len(result.Values) != 0 {
		t.Errorf("List() after DeleteAll() = %v, want empty", result.Values)
	}
}

func TestMapPath_DeleteAll_Empty(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/DeleteAllEmpty", codecs.NewJsonCodec[string](), false)

	deleted, err := m.DeleteAll(ctx, cli)
	if err != nil {
		t.Fatalf("DeleteAll() error = %v", err)
	}
	if deleted.Count != 0 {
		t.Errorf("DeleteAll() on empty prefix deleted %d, want 0", deleted.Count)
	}
}

func TestMapPath_Watch_Put(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := paths.NewMapPath("wag/Acls/WatchPut", codecs.NewJsonCodec[string](), false)

	var mu sync.Mutex
	var received []watch.Event[string]

	w := m.Watch(ctx, cli)
	if err := w.Start(watch.Created(func(ctx context.Context, e watch.Event[string]) error {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := m.Key("alpha").Put(ctx, cli, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) > 0
	}, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(received) == 0 {
		t.Fatal("expected created event, got none")
	}
	if received[0].Key != "alpha" {
		t.Errorf("Watch put event Key = %q, want %q", received[0].Key, "alpha")
	}
	if !received[0].HasCurrent() || received[0].Current != "value1" {
		t.Errorf("Watch put event Current = %v, want %q", received[0].Current, "value1")
	}
	if received[0].Type != watch.CREATED {
		t.Errorf("Watch put event Type = %v, want CREATED", received[0].Type)
	}
}

func TestMapPath_Watch_Put_Object(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type TestObject struct {
		Something string
		Number    int
		Fronk     struct {
			Nested string
		}
	}

	m := paths.NewMapPath("wag/Acls/WatchPutObject", codecs.NewJsonCodec[TestObject](), false)

	var mu sync.Mutex
	var received []watch.Event[TestObject]

	w := m.Watch(ctx, cli)
	if err := w.Start(watch.Created(func(ctx context.Context, e watch.Event[TestObject]) error {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	testObject := TestObject{
		Something: "test",
		Number:    42,
		Fronk: struct {
			Nested string
		}{
			Nested: "nested",
		},
	}

	if err := m.Key("alpha").Put(ctx, cli, testObject); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) > 0
	}, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(received) == 0 {
		t.Fatal("expected created event, got none")
	}
	if received[0].Key != "alpha" {
		t.Errorf("Watch put event Key = %q, want %q", received[0].Key, "alpha")
	}
	if !received[0].HasCurrent() || received[0].Current != testObject {
		t.Errorf("Watch put event Current = %v, want %v", received[0].Current, testObject)
	}
	if received[0].Type != watch.CREATED {
		t.Errorf("Watch put event Type = %v, want CREATED", received[0].Type)
	}
}
func TestMapPath_Watch_Delete(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := paths.NewMapPath("wag/Acls/WatchDel", codecs.NewJsonCodec[string](), false)

	if err := m.Key("alpha").Put(ctx, cli, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	var mu sync.Mutex
	var received []watch.Event[string]

	w := m.Watch(ctx, cli)
	if err := w.Start(watch.Deleted(func(ctx context.Context, e watch.Event[string]) error {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	deleted, err := m.Key("alpha").Delete(ctx, cli)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if deleted.Count != 1 {
		t.Fatal("removed a different number of keys than expected: ", deleted.Count)
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) > 0
	}, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(received) == 0 {
		t.Fatal("expected deleted event, got none")
	}
	if received[0].Key != "alpha" {
		t.Errorf("Watch delete event Key = %q, want %q", received[0].Key, "alpha")
	}
	if received[0].Type != watch.DELETED {
		t.Errorf("Watch delete event Type = %v, want DELETED", received[0].Type)
	}
}

func TestMapPath_Watch_ContextCancel(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())

	m := paths.NewMapPath("wag/Acls/WatchCancel", codecs.NewJsonCodec[string](), false)

	w := m.Watch(ctx, cli)
	if err := w.Start(watch.Created(func(ctx context.Context, e watch.Event[string]) error {
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	cancel()

	// Give the watcher time to observe context cancellation without panic or deadlock
	time.Sleep(200 * time.Millisecond)
	w.Close()
}

func TestMapPath_Watch_DoesNotReceiveOtherPrefixes(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := t.Context()

	m := paths.NewMapPath("wag/Acls/Isolated", codecs.NewJsonCodec[string](), false)

	var mu sync.Mutex
	var received []watch.Event[string]

	w := m.Watch(ctx, cli)
	if err := w.Start(watch.Created(func(ctx context.Context, e watch.Event[string]) error {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := cli.Put(ctx, "wag/Acls/Other/key", "shouldnotappear"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := m.Key("correct").Put(ctx, cli, "yes"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) > 0
	}, 3*time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(received) == 0 {
		t.Fatal("expected created event, got none")
	}
	for _, ev := range received {
		if ev.Key != "correct" {
			t.Errorf("Watch received unexpected key %q, want %q", ev.Key, "correct")
		}
	}
}

func TestMapPath_Apply(t *testing.T) {
	cli, cleanup := testhelpers.SetupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	type TestValue struct {
		Name  string `json:"name,omitempty"`
		Level int    `json:"level,omitempty"`
	}

	m := paths.NewMapPath("wag/Acls/Apply", codecs.NewJsonCodec[TestValue](), false)

	seedMap(t, ctx, cli, m, map[string]TestValue{
		"admins":  {Name: "alice", Level: 1},
		"viewers": {Name: "carol", Level: 2},
	})

	ops, err := m.Apply(ctx, cli, json.RawMessage(`{
        "admins": {"level": 3},
        "guests": {"name": "dave", "level": 4},
        "viewers": null
    }`))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if len(ops) != 3 {
		t.Fatalf("Apply() returned %d ops, want 3", len(ops))
	}

	if _, err := cli.Txn(ctx).Then(ops...).Commit(); err != nil {
		t.Fatalf("committing Apply() ops: %v", err)
	}

	result, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after Apply() error = %v", err)
	}

	want := map[string]TestValue{
		"admins": {Name: "alice", Level: 3},
		"guests": {Name: "dave", Level: 4},
	}

	if len(result.Values) != len(want) {
		t.Fatalf("List() returned %d entries, want %d", len(result.Values), len(want))
	}

	for k, v := range want {
		if result.Values[k] != v {
			t.Errorf("List()[%q] = %+v, want %+v", k, result.Values[k], v)
		}
	}

	if _, ok := result.Values["viewers"]; ok {
		t.Errorf("List() still contains deleted key %q", "viewers")
	}

	ops, err = m.Apply(ctx, cli, json.RawMessage("null"))
	if err != nil {
		t.Fatalf("Apply(null) error = %v", err)
	}

	if len(ops) != 1 {
		t.Fatalf("Apply(null) returned %d ops, want 1", len(ops))
	}

	if _, err := cli.Txn(ctx).Then(ops...).Commit(); err != nil {
		t.Fatalf("committing Apply(null) ops: %v", err)
	}

	result, err = m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after Apply(null) error = %v", err)
	}

	if len(result.Values) != 0 {
		t.Fatalf("List() after Apply(null) = %v, want empty", result.Values)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
