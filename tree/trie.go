package tree

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/NHAS/tetcd"
	"github.com/wI2L/jsondiff"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Kind int

const (
	KindIntermediate Kind = iota // just a path segment, no value
	KindSimple                   // terminal key=value
	KindMap                      // prefix, owns all children
)

type treeNode struct {
	kind     Kind
	applier  Applier // non-nil for KindSimple and KindMap
	children map[string]*treeNode
}

func (t *treeNode) insert(key string, applier Applier) {
	segments := strings.Split(strings.Trim(key, "/"), "/")
	cur := t
	for _, seg := range segments {
		if cur.children == nil {
			cur.children = make(map[string]*treeNode, 1)
		}
		next, ok := cur.children[seg]
		if !ok {
			next = &treeNode{
				kind: KindIntermediate,
			}
			cur.children[seg] = next
		}
		cur = next
	}
	cur.kind = applier.Kind()
	cur.applier = applier
}

func (t *treeNode) apply(txn *tetcd.TxnConditional, op jsondiff.Operation) error {
	if t.children == nil {
		return fmt.Errorf("no children in root")
	}

	segments := strings.Split(strings.Trim(op.Path, "/"), "/")

	eval := func(cur, prev *treeNode) *treeNode {
		if cur.applier != nil {
			switch cur.applier.Kind() {
			case KindSimple:
				return cur
			case KindMap:
				return cur
			case KindIntermediate:
				if prev != nil && prev.kind == KindSimple {
					return nil
				}
			}
		}
		return prev
	}

	var target *treeNode
	cur := t
	for _, seg := range segments {

		target = eval(cur, target)

		next, ok := cur.children[seg]
		if !ok {
			return fmt.Errorf("child was not found for %q of path %q", seg, op.Path)
		}

		cur = next
	}

	// evaluate the final node
	target = eval(cur, target)

	if target == nil {
		return fmt.Errorf("no target")
	}

	if target.applier == nil {
		return fmt.Errorf("no applier found for path %q", op.Path)
	}
	if target.kind == KindIntermediate {
		return fmt.Errorf("applier was of invalid type (Intermediate)")
	}

	return target.applier.Apply(txn, op)
}

type Tree struct {
	mu   sync.RWMutex
	root *treeNode
}

func NewTree() *Tree {
	return &Tree{
		root: &treeNode{},
	}
}

type Applier interface {
	Apply(txn *tetcd.TxnConditional, op jsondiff.Operation) error
	Kind() Kind
}

func (t *Tree) Register(key string, p Applier) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.root.insert(key, p)
}

func (t *Tree) ApplySingle(ctx context.Context, cli *clientv3.Client, op jsondiff.Operation) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	txn := tetcd.NewTxn(ctx, cli)
	then := txn.Then()

	err := t.root.apply(then, op)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (t *Tree) applyWithTxn(txn *tetcd.TxnConditional, ops jsondiff.Patch) error {
	for _, op := range ops {

		err := t.root.apply(txn, op)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) ApplyWithTxn(txn *tetcd.TxnConditional, ops jsondiff.Patch) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(txn, ops)
}

func (t *Tree) Apply(ctx context.Context, cli *clientv3.Client, ops jsondiff.Patch) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	txn := tetcd.NewTxn(ctx, cli)
	then := txn.Then()

	if err := t.applyWithTxn(then, ops); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}
