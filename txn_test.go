package tetcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	clientv3util "go.etcd.io/etcd/client/v3/clientv3util"

	"github.com/NHAS/tetcd/paths"

	"github.com/NHAS/tetcd/codecs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type autoTypeTestPaths struct{}

func (autoTypeTestPaths) SingleKey(key string) paths.Path[string] {
	return paths.NewPath("txn/"+key, codecs.NewJsonCodec[string]())
}

func (autoTypeTestPaths) MapPrefix(prefix string) paths.MapPath[string] {
	return paths.NewMapPath("txn/"+prefix+"/", codecs.NewJsonCodec[string]())
}

var TestPaths = autoTypeTestPaths{}

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// PutTx / GetTx – unconditional
// ---------------------------------------------------------------------------

func TestTxn_UnconditionalPutAndGet(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("put-get")

	put := NewTxn(ctx, client)
	if err := PutTx(put.Then(), p, "hello"); err != nil {
		t.Fatalf("PutTx failed: %v", err)
	}
	if err := put.Commit(); err != nil {
		t.Fatalf("Commit (put) failed: %v", err)
	}

	get := NewTxn(ctx, client)
	h := GetTx(get.Then(), p)
	if err := get.Commit(); err != nil {
		t.Fatalf("Commit (get) failed: %v", err)
	}

	val, err := h.Value()
	if err != nil {
		t.Fatalf("Value() failed: %v", err)
	}
	if val != "hello" {
		t.Errorf("expected 'hello', got %q", val)
	}
}

// ---------------------------------------------------------------------------
// DeleteTx – unconditional
// ---------------------------------------------------------------------------

func TestTxn_UnconditionalDelete(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete")

	if _, err := client.Put(ctx, p.Key(), "to-be-deleted"); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	del := NewTxn(ctx, client)
	DeleteTx(del.Then(), p)
	if err := del.Commit(); err != nil {
		t.Fatalf("Commit (delete) failed: %v", err)
	}

	resp, err := client.Get(ctx, p.Key())
	if err != nil {
		t.Fatalf("raw get failed: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected key to be deleted, still present")
	}
}

// ---------------------------------------------------------------------------
// Conditional – then branch taken (key exists)
// ---------------------------------------------------------------------------

func TestTxn_ConditionalThenBranch(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("cond-then")

	if _, err := client.Put(ctx, p.Key(), "original"); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, elseCond := txn.Conditional(clientv3util.KeyExists(p.Key()))

	if err := PutTx(thenCond, p, "updated"); err != nil {
		t.Fatalf("PutTx (then) failed: %v", err)
	}
	if err := PutTx(elseCond, p, "created"); err != nil {
		t.Fatalf("PutTx (else) failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !txn.SubTxn.succeeded {
		t.Fatal("expected then branch to be taken")
	}

	resp, err := client.Get(ctx, p.Key())
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(resp.Kvs[0].Value) != `"updated"` {
		t.Errorf("expected '\"updated\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// Conditional – else branch taken (key missing)
// ---------------------------------------------------------------------------

func TestTxn_ConditionalElseBranch(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("cond-else")

	txn := NewTxn(ctx, client)
	thenCond, elseCond := txn.Conditional(clientv3util.KeyExists(p.Key()))

	if err := PutTx(thenCond, p, "should-not-appear"); err != nil {
		t.Fatalf("PutTx (then) failed: %v", err)
	}
	if err := PutTx(elseCond, p, "else-value"); err != nil {
		t.Fatalf("PutTx (else) failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if txn.SubTxn.succeeded {
		t.Fatal("expected else branch to be taken")
	}

	resp, err := client.Get(ctx, p.Key())
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(resp.Kvs[0].Value) != `"else-value"` {
		t.Errorf("expected '\"else-value\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// GetHandle – ErrNotFound when key is absent
// ---------------------------------------------------------------------------

func TestTxn_GetHandle_NotFound(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()

	txn := NewTxn(ctx, client)
	h := GetTx(txn.Then(), TestPaths.SingleKey("does-not-exist"))
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	_, err := h.Value()
	if err == nil {
		t.Fatal("expected ErrNotDone, got nil")
	}
	if err != ErrNotDone {
		t.Errorf("expected ErrNotDone, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// ListTx – values and keys-only
// ---------------------------------------------------------------------------

func TestTxn_ListTx_Entries(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	mp := TestPaths.MapPrefix("list")

	want := map[string]string{
		"alice": "alice-data",
		"bob":   "bob-data",
	}
	for k, v := range want {
		encoded := fmt.Sprintf("%q", v)
		if _, err := client.Put(ctx, mp.Prefix()+k, encoded); err != nil {
			t.Fatalf("seed put failed: %v", err)
		}
	}

	txn := NewTxn(ctx, client)
	h := ListTx(txn.Then(), mp)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	entries, err := h.Entries()
	if err != nil {
		t.Fatalf("Entries() failed: %v", err)
	}
	if len(entries) != len(want) {
		t.Errorf("expected %d entries, got %d", len(want), len(entries))
	}
	for k, v := range want {
		if entries[k] != v {
			t.Errorf("key %q: expected %q, got %q", k, v, entries[k])
		}
	}
}

func TestTxn_ListTx_KeysOnly(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	mp := TestPaths.MapPrefix("keys-only")

	names := []string{"carol", "dave", "eve"}
	for _, n := range names {
		if _, err := client.Put(ctx, mp.Prefix()+n, `"value"`); err != nil {
			t.Fatalf("seed put failed: %v", err)
		}
	}

	txn := NewTxn(ctx, client)
	h := ListTx(txn.Then(), mp, clientv3.WithKeysOnly())
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	keys, err := h.Keys()
	if err != nil {
		t.Fatalf("Keys() failed: %v", err)
	}
	if len(keys) != len(names) {
		t.Errorf("expected %d keys, got %d", len(names), len(keys))
	}
}

func TestTxn_ListTx_EmptyPrefix(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()

	txn := NewTxn(ctx, client)
	h := ListTx(txn.Then(), TestPaths.MapPrefix("empty-prefix"))
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	_, err := h.Entries()
	if err != paths.ErrNotFound {
		t.Fatalf("Entries() should have failed on empty prefix: %v", err)
	}

	_, err = h.Keys()
	if err != paths.ErrNotFound {
		t.Fatalf("Keys() should have failed on empty prefix: %v", err)
	}
}

// ---------------------------------------------------------------------------
// SubTx – nested transaction
// ---------------------------------------------------------------------------

func TestTxn_SubTx_InnerThenBranch(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	outerPath := TestPaths.SingleKey("subtx-outer")
	innerPath := TestPaths.SingleKey("subtx-inner")

	if _, err := client.Put(ctx, outerPath.Key(), `"exists"`); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	outerThen, _ := txn.Conditional(clientv3util.KeyExists(outerPath.Key()))

	sub := SubTx(outerThen)
	innerThen, innerElse := sub.Conditional(clientv3util.KeyMissing(innerPath.Key()))

	if err := PutTx(innerThen, innerPath, "created-by-inner-then"); err != nil {
		t.Fatalf("PutTx inner then failed: %v", err)
	}
	if err := PutTx(innerElse, innerPath, "created-by-inner-else"); err != nil {
		t.Fatalf("PutTx inner else failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	resp, err := client.Get(ctx, innerPath.Key())
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("expected inner key to be set")
	}
	if string(resp.Kvs[0].Value) != `"created-by-inner-then"` {
		t.Errorf("expected '\"created-by-inner-then\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// Multiple ops in one transaction
// ---------------------------------------------------------------------------

func TestTxn_MultipleOpsInThen(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()

	txn := NewTxn(ctx, client)
	then := txn.Then()

	subKeys := []string{"a", "b", "c"}
	for _, k := range subKeys {
		p := TestPaths.SingleKey("multi/" + k)
		if err := PutTx(then, p, k+"-value"); err != nil {
			t.Fatalf("PutTx(%s) failed: %v", k, err)
		}
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	for _, k := range subKeys {
		p := TestPaths.SingleKey("multi/" + k)
		value, err := p.Get(ctx, client)
		if err != nil {
			t.Fatalf("get %s failed: %v", k, err)
		}
		if value != k+"-value" {
			t.Errorf("%s: expected %s, got %q", k, k+"-value", value)
		}
	}
}

// ---------------------------------------------------------------------------
// Conditional – then branch: multiple mixed ops (put + get + delete)
// ---------------------------------------------------------------------------

func TestTxn_ConditionalThen_MixedOps(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	existingPath := TestPaths.SingleKey("mixed-existing")
	deletePath := TestPaths.SingleKey("mixed-delete")
	newPath := TestPaths.SingleKey("mixed-new")

	// seed
	if _, err := client.Put(ctx, existingPath.Key(), `"seed"`); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}
	if _, err := client.Put(ctx, deletePath.Key(), `"tobedeleted"`); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, _ := txn.Conditional(clientv3util.KeyExists(existingPath.Key()))

	getH := GetTx(thenCond, existingPath)
	DeleteTx(thenCond, deletePath)
	if err := PutTx(thenCond, newPath, "brand-new"); err != nil {
		t.Fatalf("PutTx failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !txn.SubTxn.succeeded {
		t.Fatal("expected then branch")
	}

	// existing key was read
	val, err := getH.Value()
	if err != nil {
		t.Fatalf("GetHandle.Value() failed: %v", err)
	}
	if val != "seed" {
		t.Errorf("expected 'seed', got %q", val)
	}

	// delete key is gone
	resp, err := client.Get(ctx, deletePath.Key())
	if err != nil {
		t.Fatalf("get deleted key failed: %v", err)
	}
	if resp.Count != 0 {
		t.Error("expected deletePath to be gone")
	}

	// new key exists
	resp, err = client.Get(ctx, newPath.Key())
	if err != nil {
		t.Fatalf("get new key failed: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("expected newPath to exist")
	}
	if string(resp.Kvs[0].Value) != `"brand-new"` {
		t.Errorf("expected '\"brand-new\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// Conditional – get in then, get in else — only the taken branch handle works
// ---------------------------------------------------------------------------

func TestTxn_ConditionalGet_OnlyTakenBranchResolves(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	triggerPath := TestPaths.SingleKey("branch-trigger")
	thenReadPath := TestPaths.SingleKey("branch-then-read")
	elseReadPath := TestPaths.SingleKey("branch-else-read")

	// Only the then-read key exists; trigger also exists so then branch fires.
	if _, err := client.Put(ctx, triggerPath.Key(), `"1"`); err != nil {
		t.Fatalf("seed failed: %v", err)
	}
	if _, err := client.Put(ctx, thenReadPath.Key(), `"then-value"`); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, elseCond := txn.Conditional(clientv3util.KeyExists(triggerPath.Key()))

	thenH := GetTx(thenCond, thenReadPath)
	elseH := GetTx(elseCond, elseReadPath)

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// then branch taken — thenH should resolve
	val, err := thenH.Value()
	if err != nil {
		t.Fatalf("thenH.Value() failed: %v", err)
	}
	if val != "then-value" {
		t.Errorf("expected 'then-value', got %q", val)
	}

	// else branch not taken — elseH should be ErrNotDone
	_, err = elseH.Value()
	if err != ErrNotDone {
		t.Errorf("expected ErrNotDone for else handle, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// PutTx overwrite — second put replaces first within same txn
// ---------------------------------------------------------------------------

func TestTxn_PutOverwrite(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("overwrite")

	// first write
	txn := NewTxn(ctx, client)
	if err := PutTx(txn.Then(), p, "v1"); err != nil {
		t.Fatalf("first PutTx failed: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("first Commit failed: %v", err)
	}

	// overwrite in a second txn
	txn2 := NewTxn(ctx, client)
	if err := PutTx(txn2.Then(), p, "v2"); err != nil {
		t.Fatalf("second PutTx failed: %v", err)
	}
	if err := txn2.Commit(); err != nil {
		t.Fatalf("second Commit failed: %v", err)
	}

	resp, err := client.Get(ctx, p.Key())
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(resp.Kvs[0].Value) != `"v2"` {
		t.Errorf("expected '\"v2\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// SubTx – outer else → inner conditional (key missing → created by inner else)
// ---------------------------------------------------------------------------

func TestTxn_SubTx_OuterElse_InnerElseBranch(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	outerPath := TestPaths.SingleKey("subtx2-outer") // does NOT exist → outer else fires
	innerPath := TestPaths.SingleKey("subtx2-inner") // DOES exist → inner then fires
	resultPath := TestPaths.SingleKey("subtx2-result")

	// seed inner so KeyMissing is false → inner else fires
	if _, err := client.Put(ctx, innerPath.Key(), `"present"`); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	_, outerElse := txn.Conditional(clientv3util.KeyExists(outerPath.Key()))

	sub := SubTx(outerElse)
	innerThen, innerElse := sub.Conditional(clientv3util.KeyMissing(innerPath.Key()))

	if err := PutTx(innerThen, resultPath, "from-inner-then"); err != nil {
		t.Fatalf("PutTx innerThen failed: %v", err)
	}
	if err := PutTx(innerElse, resultPath, "from-inner-else"); err != nil {
		t.Fatalf("PutTx innerElse failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	resp, err := client.Get(ctx, resultPath.Key())
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("expected resultPath to be set")
	}
	if string(resp.Kvs[0].Value) != `"from-inner-else"` {
		t.Errorf("expected '\"from-inner-else\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// ListTx – written via PutTx then read back via ListTx in same sequence
// ---------------------------------------------------------------------------

func TestTxn_ListTx_AfterPutTx(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	mp := TestPaths.MapPrefix("list-after-put")

	// write entries via individual PutTx transactions
	entries := map[string]string{
		"x": "xval",
		"y": "yval",
		"z": "zval",
	}
	for k, v := range entries {
		p := paths.NewPath(mp.Prefix()+k, codecs.NewJsonCodec[string]())
		txn := NewTxn(ctx, client)
		if err := PutTx(txn.Then(), p, v); err != nil {
			t.Fatalf("PutTx failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// list them back
	txn := NewTxn(ctx, client)
	h := ListTx(txn.Then(), mp)
	if err := txn.Commit(); err != nil {
		t.Fatalf("ListTx Commit failed: %v", err)
	}

	got, err := h.Entries()
	if err != nil {
		t.Fatalf("Entries() failed: %v", err)
	}
	if len(got) != len(entries) {
		t.Errorf("expected %d entries, got %d", len(entries), len(got))
	}
	for k, v := range entries {
		if got[k] != v {
			t.Errorf("key %q: expected %q, got %q", k, v, got[k])
		}
	}
}

// ---------------------------------------------------------------------------
// ListTx – Keys() returns stripped suffixes (prefix trimmed)
// ---------------------------------------------------------------------------

func TestTxn_ListTx_KeysStrippedOfPrefix(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	mp := TestPaths.MapPrefix("strip-prefix")

	names := []string{"alpha", "beta", "gamma"}
	for _, n := range names {
		if _, err := client.Put(ctx, mp.Prefix()+n, `"v"`); err != nil {
			t.Fatalf("seed put failed: %v", err)
		}
	}

	txn := NewTxn(ctx, client)
	h := ListTx(txn.Then(), mp, clientv3.WithKeysOnly())
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	keys, err := h.Keys()
	if err != nil {
		t.Fatalf("Keys() failed: %v", err)
	}

	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	for _, name := range names {
		if _, ok := keySet[name]; !ok {
			t.Errorf("expected key %q in result, got %v", name, keys)
		}
	}
}

// ---------------------------------------------------------------------------
// Conditional – multiple conditions (AND semantics)
// ---------------------------------------------------------------------------

func TestTxn_ConditionalMultipleConditions(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p1 := TestPaths.SingleKey("multi-cond-1")
	p2 := TestPaths.SingleKey("multi-cond-2")
	result := TestPaths.SingleKey("multi-cond-result")

	// both keys exist → then fires
	if _, err := client.Put(ctx, p1.Key(), `"1"`); err != nil {
		t.Fatalf("seed p1 failed: %v", err)
	}
	if _, err := client.Put(ctx, p2.Key(), `"2"`); err != nil {
		t.Fatalf("seed p2 failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, elseCond := txn.Conditional(
		clientv3util.KeyExists(p1.Key()),
		clientv3util.KeyExists(p2.Key()),
	)

	if err := PutTx(thenCond, result, "both-exist"); err != nil {
		t.Fatalf("PutTx then failed: %v", err)
	}
	if err := PutTx(elseCond, result, "not-both"); err != nil {
		t.Fatalf("PutTx else failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !txn.SubTxn.succeeded {
		t.Fatal("expected then branch when both keys exist")
	}

	resp, err := client.Get(ctx, result.Key())
	if err != nil {
		t.Fatalf("get result failed: %v", err)
	}
	if string(resp.Kvs[0].Value) != `"both-exist"` {
		t.Errorf("expected '\"both-exist\"', got %q", string(resp.Kvs[0].Value))
	}
}

// one of the two conditions fails → else fires
func TestTxn_ConditionalMultipleConditions_OneFails(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p1 := TestPaths.SingleKey("multi-cond-one-fail-1")
	p2 := TestPaths.SingleKey("multi-cond-one-fail-2") // does NOT exist
	result := TestPaths.SingleKey("multi-cond-one-fail-result")

	if _, err := client.Put(ctx, p1.Key(), `"1"`); err != nil {
		t.Fatalf("seed p1 failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, elseCond := txn.Conditional(
		clientv3util.KeyExists(p1.Key()),
		clientv3util.KeyExists(p2.Key()),
	)

	if err := PutTx(thenCond, result, "both-exist"); err != nil {
		t.Fatalf("PutTx then failed: %v", err)
	}
	if err := PutTx(elseCond, result, "not-both"); err != nil {
		t.Fatalf("PutTx else failed: %v", err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if txn.SubTxn.succeeded {
		t.Fatal("expected else branch when one key is missing")
	}

	resp, err := client.Get(ctx, result.Key())
	if err != nil {
		t.Fatalf("get result failed: %v", err)
	}
	if string(resp.Kvs[0].Value) != `"not-both"` {
		t.Errorf("expected '\"not-both\"', got %q", string(resp.Kvs[0].Value))
	}
}

// ---------------------------------------------------------------------------
// GetHandle – Key() returns the correct key string
// ---------------------------------------------------------------------------

func TestTxn_GetHandle_KeyReturnsPath(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("handle-key-check")

	txn := NewTxn(ctx, client)
	h := GetTx(txn.Then(), p)
	// no need to commit — key is set at construction time
	_ = txn.Commit()

	if h.Key() != p.Key() {
		t.Errorf("expected handle key %q, got %q", p.Key(), h.Key())
	}
}

// ...existing code...

// ---------------------------------------------------------------------------
// Nested transactions – deep if/then/else/subtx combinations
// ---------------------------------------------------------------------------

// Tests a 3-level deep nesting:
//
//	root: if A exists
//	  then: if B exists
//	    then: put result = "A-and-B"
//	    else: put result = "A-not-B"
//	  else: if B exists
//	    then: put result = "B-not-A"
//	    else: put result = "neither"
func TestTxn_DeepNesting_AllFourPaths(t *testing.T) {
	cases := []struct {
		name       string
		seedA      bool
		seedB      bool
		wantResult string
	}{
		{"A-and-B", true, true, "A-and-B"},
		{"A-not-B", true, false, "A-not-B"},
		{"B-not-A", false, true, "B-not-A"},
		{"neither", false, false, "neither"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			client, cleanup := setupEtcdContainer(t)
			defer cleanup()

			ctx := context.Background()

			pathA := TestPaths.SingleKey("deep/" + tc.name + "/a")
			pathB := TestPaths.SingleKey("deep/" + tc.name + "/b")
			resultPath := TestPaths.SingleKey("deep/" + tc.name + "/result")

			if tc.seedA {
				if _, err := client.Put(ctx, pathA.Key(), `"exists"`); err != nil {
					t.Fatalf("seed A failed: %v", err)
				}
			}
			if tc.seedB {
				if _, err := client.Put(ctx, pathB.Key(), `"exists"`); err != nil {
					t.Fatalf("seed B failed: %v", err)
				}
			}

			txn := NewTxn(ctx, client)
			rootThen, rootElse := txn.Conditional(clientv3util.KeyExists(pathA.Key()))

			// then-subtx: A exists
			thenSub := SubTx(rootThen)
			thenSubThen, thenSubElse := thenSub.Conditional(clientv3util.KeyExists(pathB.Key()))
			if err := PutTx(thenSubThen, resultPath, "A-and-B"); err != nil {
				t.Fatalf("PutTx A-and-B failed: %v", err)
			}
			if err := PutTx(thenSubElse, resultPath, "A-not-B"); err != nil {
				t.Fatalf("PutTx A-not-B failed: %v", err)
			}

			// else-subtx: A missing
			elseSub := SubTx(rootElse)
			elseSubThen, elseSubElse := elseSub.Conditional(clientv3util.KeyExists(pathB.Key()))
			if err := PutTx(elseSubThen, resultPath, "B-not-A"); err != nil {
				t.Fatalf("PutTx B-not-A failed: %v", err)
			}
			if err := PutTx(elseSubElse, resultPath, "neither"); err != nil {
				t.Fatalf("PutTx neither failed: %v", err)
			}

			if err := txn.Commit(); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			resp, err := client.Get(ctx, resultPath.Key())
			if err != nil {
				t.Fatalf("get result failed: %v", err)
			}
			if len(resp.Kvs) == 0 {
				t.Fatal("expected result key to be set")
			}

			want := fmt.Sprintf("%q", tc.wantResult)
			got := string(resp.Kvs[0].Value)
			if got != want {
				t.Errorf("expected %s, got %s", want, got)
			}
		})
	}
}

// ...existing code...

// ---------------------------------------------------------------------------
// DeleteHandle – deleted count
// ---------------------------------------------------------------------------

func TestTxn_DeleteHandle_DeletedCount(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-count")

	if _, err := client.Put(ctx, p.Key(), `"value"`); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	count, err := h.Deleted()
	if err != nil {
		t.Fatalf("Deleted() failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected deleted count 1, got %d", count)
	}
}

func TestTxn_DeleteHandle_DeletedCount_KeyMissing(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-count-missing")

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p)
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	count, err := h.Deleted()
	if err != nil {
		t.Fatalf("Deleted() failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected deleted count 0 for missing key, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// DeleteHandle – PrevValue with WithPrevKV
// ---------------------------------------------------------------------------

func TestTxn_DeleteHandle_PrevValue(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-prevkv")

	if err := PutTx(NewTxn(ctx, client).Then(), p, "original"); err != nil {
		t.Fatalf("seed PutTx failed: %v", err)
	}
	if err := NewTxn(ctx, client).Commit(); err != nil {
		t.Fatalf("seed Commit failed: %v", err)
	}

	// Use a fresh txn to actually write the seed value
	seedTxn := NewTxn(ctx, client)
	if err := PutTx(seedTxn.Then(), p, "original"); err != nil {
		t.Fatalf("seed PutTx failed: %v", err)
	}
	if err := seedTxn.Commit(); err != nil {
		t.Fatalf("seed Commit failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p, clientv3.WithPrevKV())
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	prev, err := h.PrevValue()
	if err != nil {
		t.Fatalf("PrevValue() failed: %v", err)
	}
	if prev != "original" {
		t.Errorf("expected prev value 'original', got %q", prev)
	}

	count, err := h.Deleted()
	if err != nil {
		t.Fatalf("Deleted() failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected deleted count 1, got %d", count)
	}
}

func TestTxn_DeleteHandle_PrevValue_NotFound_WhenNoPrevKVOption(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-no-prevkv")

	seedTxn := NewTxn(ctx, client)
	if err := PutTx(seedTxn.Then(), p, "data"); err != nil {
		t.Fatalf("seed PutTx failed: %v", err)
	}
	if err := seedTxn.Commit(); err != nil {
		t.Fatalf("seed Commit failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p) // no WithPrevKV
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	_, err := h.PrevValue()
	if err != paths.ErrNotFound {
		t.Errorf("expected ErrNotFound when WithPrevKV not set, got %v", err)
	}
}

func TestTxn_DeleteHandle_PrevValue_KeyMissing(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-prevkv-missing")

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p, clientv3.WithPrevKV())
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	_, err := h.PrevValue()
	if err != paths.ErrNotFound {
		t.Errorf("expected ErrNotFound for missing key, got %v", err)
	}

	count, err := h.Deleted()
	if err != nil {
		t.Fatalf("Deleted() failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected deleted count 0, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// DeleteHandle – ErrNotDone before Commit
// ---------------------------------------------------------------------------

func TestTxn_DeleteHandle_ErrNotDone(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-notdone")

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p)
	// deliberately do NOT commit

	if _, err := h.Deleted(); err != ErrNotDone {
		t.Errorf("Deleted(): expected ErrNotDone before Commit, got %v", err)
	}
	if _, err := h.PrevValue(); err != ErrNotDone {
		t.Errorf("PrevValue(): expected ErrNotDone before Commit, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// DeleteHandle – Key() returns correct path
// ---------------------------------------------------------------------------

func TestTxn_DeleteHandle_KeyReturnsPath(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	p := TestPaths.SingleKey("delete-handle-key")

	txn := NewTxn(ctx, client)
	h := DeleteTx(txn.Then(), p)
	_ = txn.Commit()

	if h.Key() != p.Key() {
		t.Errorf("expected handle key %q, got %q", p.Key(), h.Key())
	}
}

// ---------------------------------------------------------------------------
// DeleteHandle – inside conditional branch
// ---------------------------------------------------------------------------

func TestTxn_DeleteHandle_InConditionalThenBranch(t *testing.T) {
	client, cleanup := setupEtcdContainer(t)
	defer cleanup()

	ctx := context.Background()
	triggerPath := TestPaths.SingleKey("delete-cond-trigger")
	deletePath := TestPaths.SingleKey("delete-cond-target")

	seedTxn := NewTxn(ctx, client)
	if err := PutTx(seedTxn.Then(), triggerPath, "trigger"); err != nil {
		t.Fatalf("seed trigger failed: %v", err)
	}
	if err := PutTx(seedTxn.Then(), deletePath, "to-delete"); err != nil {
		t.Fatalf("seed target failed: %v", err)
	}
	if err := seedTxn.Commit(); err != nil {
		t.Fatalf("seed Commit failed: %v", err)
	}

	txn := NewTxn(ctx, client)
	thenCond, _ := txn.Conditional(clientv3util.KeyExists(triggerPath.Key()))
	h := DeleteTx(thenCond, deletePath, clientv3.WithPrevKV())

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !txn.SubTxn.succeeded {
		t.Fatal("expected then branch to be taken")
	}

	prev, err := h.PrevValue()
	if err != nil {
		t.Fatalf("PrevValue() failed: %v", err)
	}
	if prev != "to-delete" {
		t.Errorf("expected prev 'to-delete', got %q", prev)
	}

	count, err := h.Deleted()
	if err != nil {
		t.Fatalf("Deleted() failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected deleted count 1, got %d", count)
	}

	resp, err := client.Get(ctx, deletePath.Key())
	if err != nil {
		t.Fatalf("raw get failed: %v", err)
	}
	if resp.Count != 0 {
		t.Error("expected key to be deleted")
	}
}
