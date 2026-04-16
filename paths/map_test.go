package paths_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/paths"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func setupEtcdContainer(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/coreos/etcd:v3.6.7",
		ExposedPorts: []string{"2379/tcp"},
		Env: map[string]string{
			"ETCD_LISTEN_CLIENT_URLS":    "http://0.0.0.0:2379",
			"ETCD_ADVERTISE_CLIENT_URLS": "http://0.0.0.0:2379",
		},
		WaitingFor: wait.ForLog("ready to serve client requests").
			WithStartupTimeout(10 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start etcd container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "2379")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://%s:%s", host, port.Port())},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}

	return client, func() {
		client.Close()
		container.Terminate(ctx)
	}
}

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
	if got := m.Prefix(); got != "wag/Acls/Groups" {
		t.Errorf("Prefix() = %q, want %q", got, "wag/Acls/Groups")
	}
}

func TestMapPath_Key(t *testing.T) {
	m := paths.NewMapPath("wag/Acls/Groups", codecs.NewJsonCodec[string](), false)
	if got := m.Key("admins").Key(); got != "wag/Acls/Groups/admins" {
		t.Errorf("Key() = %q, want %q", got, "wag/Acls/Groups/admins")
	}
}

func TestMapPath_Keys(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
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
	cli, cleanup := setupEtcdContainer(t)
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
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/List", codecs.NewJsonCodec[string](), false)
	want := map[string]string{
		"admins":  "alice,bob",
		"viewers": "carol",
	}
	seedMap(t, ctx, cli, m, want)

	_, got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("List() returned %d entries, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("List()[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestMapPath_List_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/ListEmpty", codecs.NewJsonCodec[string](), false)

	_, got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List() = %v, want empty map", got)
	}
}

func TestMapPath_Delete(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := context.Background()

	m := paths.NewMapPath("wag/Acls/Delete", codecs.NewJsonCodec[string](), false)
	seedMap(t, ctx, cli, m, map[string]string{
		"admins":  "alice",
		"viewers": "carol",
	})

	if err := m.Delete(ctx, cli, "admins"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after Delete() error = %v", err)
	}
	if _, exists := got["admins"]; exists {
		t.Error("Delete() did not remove key 'admins'")
	}
	if _, exists := got["viewers"]; !exists {
		t.Error("Delete() incorrectly removed key 'viewers'")
	}
}

func TestMapPath_Put(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
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

	_, got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after Put() error = %v", err)
	}
	if got["admins"] != adminObject {
		t.Error("Put() did not correctly set key 'admins'")
	}
	if got["viewers"] != viewiersObject {
		t.Error("Put() did not correctly set key 'viewers'")
	}
}

func TestMapPath_DeleteAll(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
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

	_, got, err := m.List(ctx, cli)
	if err != nil {
		t.Fatalf("List() after DeleteAll() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List() after DeleteAll() = %v, want empty", got)
	}
}

func TestMapPath_DeleteAll_Empty(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
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
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := paths.NewMapPath("wag/Acls/WatchPut", codecs.NewJsonCodec[string](), false)
	ch := m.Watch(ctx, cli)

	if err := m.Key("alpha").Put(ctx, cli, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	ev := <-ch
	if ev.Key != "alpha" {
		t.Errorf("Watch put event Key = %q, want %q", ev.Key, "alpha")
	}
	if ev.Value != "value1" {
		t.Errorf("Watch put event Value = %q, want %q", ev.Value, "value1")
	}
	if ev.Deleted {
		t.Error("Watch put event Deleted = true, want false")
	}
}

func TestMapPath_Watch_Put_Object(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type TestObject struct {
		Something string

		Number int
		Fronk  struct {
			Nested string
		}
	}

	m := paths.NewMapPath("wag/Acls/WatchPut", codecs.NewJsonCodec[TestObject](), false)
	ch := m.Watch(ctx, cli)

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

	ev := <-ch
	if ev.Key != "alpha" {
		t.Errorf("Watch put event Key = %q, want %q", ev.Key, "alpha")
	}
	if ev.Value != testObject {
		t.Errorf("Watch put event Value = %v, want %v", ev.Value, testObject)
	}
	if ev.Deleted {
		t.Error("Watch put event Deleted = true, want false")
	}
}

func TestMapPath_Watch_Delete(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := paths.NewMapPath("wag/Acls/WatchDel", codecs.NewJsonCodec[string](), false)

	// Seed before watching so the put event doesn't interfere
	if err := m.Key("alpha").Put(ctx, cli, "value1"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	ch := m.Watch(ctx, cli)

	deleted, err := m.Key("alpha").Delete(ctx, cli)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if deleted.Count != 1 {
		t.Fatal("removed a different number of keys than expected: ", deleted.Count)
	}

	ev := <-ch
	if ev.Key != "alpha" {
		t.Errorf("Watch delete event Key = %q, want %q", ev.Key, "alpha")
	}
	if !ev.Deleted {
		t.Error("Watch delete event Deleted = false, want true")
	}
}

func TestMapPath_Watch_ContextCancel(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())

	m := paths.NewMapPath("wag/Acls/WatchCancel", codecs.NewJsonCodec[string](), false)
	ch := m.Watch(ctx, cli)

	cancel()

	// Drain channel; it should close after context cancellation
	for range ch {
	}
}

func TestMapPath_Watch_DoesNotReceiveOtherPrefixes(t *testing.T) {
	cli, cleanup := setupEtcdContainer(t)
	defer cleanup()
	ctx := t.Context()

	m := paths.NewMapPath("wag/Acls/Isolated", codecs.NewJsonCodec[string](), false)
	ch := m.Watch(ctx, cli)

	// Write to a different prefix — should not appear on ch
	if _, err := cli.Put(ctx, "wag/Acls/Other/key", "shouldnotappear"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Write to the watched prefix
	if err := m.Key("correct").Put(ctx, cli, "yes"); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	ev := <-ch
	if ev.Key != "correct" {
		t.Errorf("Watch received unexpected key %q, want %q", ev.Key, "correct")
	}
}
