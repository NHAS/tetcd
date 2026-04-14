package tetcd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/NHAS/tetcd/paths"

	"github.com/NHAS/tetcd/codecs"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Txn builder with if-then-else support
type Txn struct {
	ctx context.Context
	cli *clientv3.Client

	rev int64

	SubTxn
}

func (t *Txn) Rev() int64 {
	return t.rev
}

type collection struct {
	handles []handle
	ops     []clientv3.Op
	txns    []*SubTxn
}

type SubTxn struct {
	conditions []clientv3.Cmp

	thens collection
	elses collection

	succeeded bool
}

type txnMode int

const (
	modeThen txnMode = iota
	modeElse
)

type handle interface {
	parse(*etcdserverpb.ResponseOp) error
	fail(err error)
}

func NewTxn(ctx context.Context, cli *clientv3.Client) *Txn {
	return &Txn{
		ctx: ctx,
		cli: cli,
	}
}

type TxnConditional struct {
	txn  *SubTxn
	mode txnMode
}

func (tt *SubTxn) Then() *TxnConditional {
	return &TxnConditional{
		txn:  tt,
		mode: modeThen,
	}
}

func (tt *SubTxn) Else() *TxnConditional {
	return &TxnConditional{
		txn:  tt,
		mode: modeElse,
	}
}

// Conditional sets conditions on the transaction. If they evaluate to true, the
// then branch runs; otherwise the else branch runs. Both branches can contain
// further sub-transactions, enabling the nested pattern seen in WebhookRecordLastRequest.
func (tt *SubTxn) Conditional(conditions ...clientv3.Cmp) (thenCond *TxnConditional, elseCond *TxnConditional) {
	tt.conditions = conditions

	thenCond = &TxnConditional{txn: tt, mode: modeThen}
	elseCond = &TxnConditional{txn: tt, mode: modeElse}

	return thenCond, elseCond
}

type modFunc func(initialOps []clientv3.Op, initialHandles []handle) (newOps []clientv3.Op, newHandles []handle)

// modify updates the correct then/else collection of a SubTxn.
func modify(mode txnMode, txn *SubTxn, modifer modFunc) *SubTxn {
	collection := txn.thens
	if mode == modeElse {
		collection = txn.elses
	}

	collection.ops, collection.handles = modifer(collection.ops, collection.handles)

	if mode == modeThen {
		txn.thens = collection
		return txn
	}
	txn.elses = collection
	return txn
}

// ---------------------------------------------------------------------------
// Simple operations
// ---------------------------------------------------------------------------

// GetTx queues a typed GET for a single-key Path in the transaction.
// The returned *GetHandle[T] can be used after Commit() to retrieve the value.
func GetTx[T any](t *TxnConditional, path paths.Path[T], opts ...clientv3.OpOption) *GetHandle[T] {
	result := &GetHandle[T]{
		codec: path.Codec(),
		key:   path.Key(),
	}

	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpGet(path.Key(), opts...))
		handles = append(handles, result)
		return ops, handles
	})

	return result
}

// DynamicCollectionTx queues a prefix GET for a MapSlicePath and returns a *ListSliceHandle[T].
// After Commit(), call Entries() to get the full map[string]map[string]V result.
func DynamicCollectionTx[T any](t *TxnConditional, path paths.MapSlicePath[T], opts ...clientv3.OpOption) *DynamicHandle[T] {
	result := &DynamicHandle[T]{
		codec:        path.Codec(),
		prefix:       path.Prefix(),
		presenceOnly: path.PresenceOnly(),
	}

	options := append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)

	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpGet(path.Prefix(), options...))
		handles = append(handles, result)
		return ops, handles
	})

	return result
}

// ListTx queues a prefix GET for a MapPath and returns a *ListHandle[T].
// This covers the "get all keys under a prefix" pattern used in GetEffectiveAcl
// for reading group membership, DNS entries, and ACL policies by prefix.
//
// Example:
//
//	memberHandle := ListTx(thenCond, Paths.GroupMembership.Key(username), clientv3.WithKeysOnly())
func ListTx[T any](t *TxnConditional, path paths.MapPath[T], opts ...clientv3.OpOption) *ListHandle[T] {
	result := &ListHandle[T]{
		codec:  path.Codec(),
		prefix: path.Prefix(),
	}

	options := append([]clientv3.OpOption{clientv3.WithPrefix()}, opts...)

	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpGet(path.Prefix(), options...))
		handles = append(handles, result)
		return ops, handles
	})

	return result
}

// PutTx queues a typed PUT in the transaction.
func PutTx[T any](t *TxnConditional, path paths.Path[T], value T, opts ...clientv3.OpOption) error {
	data, err := path.Codec().Encode(value)
	if err != nil {
		return fmt.Errorf("failed to encode: %q, %w", path.Key(), err)
	}

	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpPut(path.Key(), string(data), opts...))
		handles = append(handles, nil) // etcd returns a response for every op; nil = no handle needed
		return ops, handles
	})

	return nil
}

// DeleteTx queues a DELETE in the transaction.
func DeleteTx[T any](t *TxnConditional, path paths.Path[T], opts ...clientv3.OpOption) *DeleteHandle[T] {
	result := &DeleteHandle[T]{
		codec: path.Codec(),
		key:   path.Key(),
	}

	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpDelete(path.Key(), opts...))
		handles = append(handles, result)
		return ops, handles
	})

	return result
}

// SubTx creates a nested sub-transaction within the given branch.
// The sub-transaction's conditions/then/else are built on the returned *SubTxn
// before Commit() is called on the root Txn.
//
// This matches the pattern in WebhookRecordLastRequest where an Else branch
// contains a further If/Then/Else:
//
//	elseCond, _ := SubTx(elseBranch)
//	innerThen, innerElse := elseCond.Conditional(clientv3util.KeyExists(...))
//	PutTx(innerThen, ...)
func SubTx(t *TxnConditional) *SubTxn {
	r := &SubTxn{}

	// Track the sub-txn so resolve() can walk into it after commit.
	if t.mode == modeThen {
		t.txn.thens.txns = append(t.txn.thens.txns, r)
	} else {
		t.txn.elses.txns = append(t.txn.elses.txns, r)
	}

	// Reserve a slot in ops/handles; build() will replace it with the real
	// OpTxn just before we hand everything to etcd.
	modify(t.mode, t.txn, func(ops []clientv3.Op, handles []handle) ([]clientv3.Op, []handle) {
		ops = append(ops, clientv3.OpTxn(nil, nil, nil)) // placeholder – replaced by build()
		handles = append(handles, nil)
		return ops, handles
	})

	return r
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

func (t *Txn) Commit() error {
	if len(t.conditions) == 0 && len(t.elses.ops) != 0 {
		return fmt.Errorf("else operations defined on an unconditional transaction")
	}

	// Materialise all sub-transactions into real OpTxn values before sending.
	if err := buildCollection(&t.SubTxn.thens); err != nil {
		return err
	}
	if err := buildCollection(&t.SubTxn.elses); err != nil {
		return err
	}

	txn := t.cli.Txn(t.ctx)

	var (
		resp *clientv3.TxnResponse
		err  error
	)

	if len(t.conditions) > 0 {
		resp, err = txn.
			If(t.conditions...).
			Then(t.thens.ops...).
			Else(t.elses.ops...).
			Commit()
	} else {
		resp, err = txn.Then(t.thens.ops...).Commit()
	}

	if err != nil {
		return err
	}

	t.SubTxn.succeeded = resp.Succeeded
	t.rev = resp.Header.Revision

	return t.SubTxn.resolve(resp)
}

// ---------------------------------------------------------------------------
// build – replaces placeholder OpTxn slots with real ones
// ---------------------------------------------------------------------------

// buildCollection walks a collection and replaces each placeholder OpTxn
// (inserted by SubTx) with a fully materialised OpTxn built from the
// corresponding *SubTxn's conditions/thens/elses.
func buildCollection(c *collection) error {
	subIdx := 0
	for i := range c.ops {
		if !c.ops[i].IsTxn() {
			continue
		}
		if subIdx >= len(c.txns) {
			return fmt.Errorf("more OpTxn placeholders than recorded sub-transactions at index %d", i)
		}

		sub := c.txns[subIdx]

		// Recurse so deeply-nested sub-transactions are also built.
		if err := buildCollection(&sub.thens); err != nil {
			return err
		}
		if err := buildCollection(&sub.elses); err != nil {
			return err
		}

		c.ops[i] = clientv3.OpTxn(sub.conditions, sub.thens.ops, sub.elses.ops)
		subIdx++
	}
	return nil
}

// ---------------------------------------------------------------------------
// resolve – walks the etcd response tree and dispatches to handles
// ---------------------------------------------------------------------------

func (t *SubTxn) resolve(resp *clientv3.TxnResponse) error {
	txnIdx := 0

	handles := t.thens.handles
	txns := t.thens.txns
	if !t.succeeded {
		handles = t.elses.handles
		txns = t.elses.txns
	}

	for i, c := range resp.Responses {

		if get := c.GetResponseRange(); get != nil {
			if handles[i] == nil {
				key := "unknown key"
				if len(get.Kvs) > 0 {
					key = string(get.Kvs[0].Key)
				}
				return fmt.Errorf("get response at index %d has no handle (key: %s)", i, key)
			}

			if len(get.Kvs) == 0 {
				handles[i].fail(paths.ErrNotFound)
				continue
			}

			if err := handles[i].parse(c); err != nil {
				return err
			}
			continue
		}

		if put := c.GetResponsePut(); put != nil {
			if handles[i] != nil {
				return fmt.Errorf("put response at index %d unexpectedly has a handle", i)
			}
			continue
		}

		if del := c.GetResponseDeleteRange(); del != nil {
			if handles[i] == nil {
				return fmt.Errorf("delete response at index %d has no handle", i)
			}

			if err := handles[i].parse(c); err != nil {
				return err
			}

			continue
		}

		if inner := c.GetResponseTxn(); inner != nil {
			// Bug fix: was `>` which allows one past the end; must be `>=`.
			if txnIdx >= len(txns) {
				return fmt.Errorf("more sub-transaction responses than recorded sub-transactions")
			}

			sub := txns[txnIdx]
			sub.succeeded = inner.Succeeded

			if err := sub.resolve((*clientv3.TxnResponse)(inner)); err != nil {
				return err
			}

			txnIdx++
			continue
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// GetHandle – single-value typed result
// ---------------------------------------------------------------------------

type GetHandle[T any] struct {
	err   error
	value T
	key   string
	codec codecs.Codec[T]
	wrote bool
}

func (h *GetHandle[T]) parse(resp *etcdserverpb.ResponseOp) error {
	h.wrote = true

	rangeResp := resp.GetResponseRange()
	if rangeResp == nil {
		return fmt.Errorf("expected range response, got something else – this should never happen")
	}

	if len(rangeResp.Kvs) == 0 {
		h.err = paths.ErrNotFound
		return nil
	}

	h.value, h.err = h.codec.Decode(rangeResp.Kvs[0].Value)
	return nil
}

func (h *GetHandle[T]) Key() string {
	return h.key
}

func (h *GetHandle[T]) fail(err error) { h.err = err }

// Value returns the decoded value, or an error (including paths.ErrNotFound).
func (h *GetHandle[T]) Value() (value T, err error) {
	if !h.wrote {
		return value, ErrNotDone
	}

	if h.err != nil {
		return value, h.err
	}
	return h.value, nil
}

// ---------------------------------------------------------------------------
// ListHandle – prefix-scan typed result
// ---------------------------------------------------------------------------

// ListHandle holds the results of a prefix GET queued with ListTx.
// After Commit() call Entries() to retrieve the decoded map, or Keys() for a
// keys-only result (when ListTx was called with clientv3.WithKeysOnly()).
type ListHandle[T any] struct {
	err    error
	items  map[string]T // populated when values are present
	keys   []string     // populated for keys-only scans
	codec  codecs.Codec[T]
	prefix string
}

func (h *ListHandle[T]) parse(resp *etcdserverpb.ResponseOp) error {
	rangeResp := resp.GetResponseRange()
	if rangeResp == nil {
		return fmt.Errorf("expected range response for list, got something else – this should never happen")
	}

	if len(rangeResp.Kvs) == 0 {
		// Not an error – just an empty prefix.
		h.items = map[string]T{}
		h.keys = []string{}
		return nil
	}

	// Detect keys-only by checking whether the first KV has an empty value.
	// etcd sets Value to nil for keys-only responses.
	keysOnly := rangeResp.Kvs[0].Value == nil

	if keysOnly {
		h.keys = make([]string, 0, len(rangeResp.Kvs))
		for _, kv := range rangeResp.Kvs {
			h.keys = append(h.keys, strings.TrimPrefix(string(kv.Key), h.prefix))
		}
		return nil
	}

	h.items = make(map[string]T, len(rangeResp.Kvs))
	for _, kv := range rangeResp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), h.prefix)
		val, err := h.codec.Decode(kv.Value)
		if err != nil {
			return fmt.Errorf("decoding %q: %w", string(kv.Key), err)
		}
		h.items[key] = val
	}

	return nil
}

func (h *ListHandle[T]) fail(err error) { h.err = err }

// Entries returns the decoded key→value map for a non-keys-only scan.
// Returns paths.ErrNotFound when no keys were present under the prefix.
func (h *ListHandle[T]) Entries() (map[string]T, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.items == nil && len(h.keys) != 0 {
		return nil, ErrKeysOnly
	}

	if h.items == nil {
		return nil, paths.ErrNotFound
	}
	return h.items, nil
}

// Keys returns the list of sub-keys for a keys-only scan (WithKeysOnly option).
// Returns paths.ErrNotFound when no keys were present under the prefix.
func (h *ListHandle[T]) Keys() ([]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.keys == nil {
		return nil, paths.ErrNotFound
	}
	return h.keys, nil
}

// ---------------------------------------------------------------------------
// DeleteHandle – delete result with optional prev-KV and deleted count
// ---------------------------------------------------------------------------

// DeleteHandle holds the result of a DELETE queued with DeleteTx.
// If DeleteTx was called with clientv3.WithPrevKV(), PrevValue() returns the
// value that existed before deletion. Deleted() always returns the count of
// keys removed.
type DeleteHandle[T any] struct {
	err     error
	prevVal T
	hasPrev bool
	deleted int64
	key     string
	codec   codecs.Codec[T]
	wrote   bool
}

func (h *DeleteHandle[T]) parse(resp *etcdserverpb.ResponseOp) error {
	h.wrote = true

	delResp := resp.GetResponseDeleteRange()
	if delResp == nil {
		return fmt.Errorf("expected delete response, got something else – this should never happen")
	}

	h.deleted = delResp.Deleted

	if len(delResp.PrevKvs) > 0 {
		val, err := h.codec.Decode(delResp.PrevKvs[0].Value)
		if err != nil {
			return fmt.Errorf("decoding prev kv for %q: %w", h.key, err)
		}
		h.prevVal = val
		h.hasPrev = true
	}

	return nil
}

func (h *DeleteHandle[T]) fail(err error) { h.err = err }

func (h *DeleteHandle[T]) Key() string { return h.key }

// Deleted returns the number of keys removed by the operation.
func (h *DeleteHandle[T]) Deleted() (int64, error) {
	if !h.wrote {
		return 0, ErrNotDone
	}
	if h.err != nil {
		return 0, h.err
	}
	return h.deleted, nil
}

// PrevValue returns the value that existed before deletion.
// Returns paths.ErrNotFound if the key did not exist or DeleteTx was not
// called with clientv3.WithPrevKV().
func (h *DeleteHandle[T]) PrevValue() (T, error) {
	var zero T
	if !h.wrote {
		return zero, ErrNotDone
	}
	if h.err != nil {
		return zero, h.err
	}
	if !h.hasPrev {
		return zero, paths.ErrNotFound
	}
	return h.prevVal, nil
}

// DynamicHandle holds the results of a prefix GET for a MapSlicePath.
type DynamicHandle[T any] struct {
	err          error
	items        map[string]map[string]T
	codec        codecs.Codec[T]
	prefix       string
	presenceOnly bool
}

func (h *DynamicHandle[T]) parse(resp *etcdserverpb.ResponseOp) error {
	rangeResp := resp.GetResponseRange()
	if rangeResp == nil {
		return fmt.Errorf("expected range response for list slice, got something else")
	}

	h.items = make(map[string]map[string]T)

	for _, kv := range rangeResp.Kvs {
		rel := strings.TrimPrefix(string(kv.Key), h.prefix+"/")
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) != 2 {
			continue
		}
		outerKey, innerKey := parts[0], parts[1]

		var (
			val T
			err error
		)

		if !h.presenceOnly {
			val, err = h.codec.Decode(kv.Value)
			if err != nil {
				return fmt.Errorf("decoding %q: %w", string(kv.Key), err)
			}
		}

		if h.items[outerKey] == nil {
			h.items[outerKey] = make(map[string]T)
		}
		h.items[outerKey][innerKey] = val
	}

	return nil
}

func (h *DynamicHandle[T]) fail(err error) { h.err = err }

// Entries returns the decoded two-level map after Commit().
func (h *DynamicHandle[T]) Entries() (map[string]map[string]T, error) {
	if h.err != nil {
		return nil, h.err
	}

	if h.presenceOnly {
		return nil, fmt.Errorf("Entries() called on a presence-only ListSliceHandle, use Keys()")
	}

	if h.items == nil {
		return nil, paths.ErrNotFound
	}
	return h.items, nil
}

func (h *DynamicHandle[T]) Keys() (map[string][]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	if h.items == nil {
		return nil, paths.ErrNotFound
	}
	if !h.presenceOnly {
		return nil, fmt.Errorf("Keys() called on a non-presence-only ListSliceHandle, use Entries()")
	}
	result := make(map[string][]string, len(h.items))
	for outerKey, inner := range h.items {
		keys := make([]string, 0, len(inner))
		for k := range inner {
			keys = append(keys, k)
		}
		result[outerKey] = keys
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var (
	ErrNoHandle = errors.New("no handle for operation")
	ErrKeysOnly = errors.New("Keys only operation")
	ErrNotDone  = errors.New("operation not done")
)
