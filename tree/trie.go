package tree

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/NHAS/tetcd/tree/kind"
	"github.com/wI2L/jsondiff"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type treeNode struct {
	kind     kind.Kind
	applier  Applier // non-nil for KindSimple and KindMap
	children map[string]*treeNode
}

func (t *treeNode) insert(applier Applier) {
	key, _ := applier.Details()
	segments := strings.Split(strings.Trim(key, "/"), "/")
	cur := t
	for _, seg := range segments {
		if cur.children == nil {
			cur.children = make(map[string]*treeNode, 1)
		}
		next, ok := cur.children[seg]
		if !ok {
			next = &treeNode{
				kind: kind.KindIntermediate,
			}
			cur.children[seg] = next
		}
		cur = next
	}
	_, cur.kind = applier.Details()
	cur.applier = applier
}

func (t *treeNode) apply(op jsondiff.Operation) ([]clientv3.Op, error) {
	if t.children == nil {
		return nil, fmt.Errorf("no children in root")
	}

	segments := strings.Split(strings.Trim(op.Path, "/"), "/")

	eval := func(cur, prev *treeNode) *treeNode {
		if cur.applier != nil {

			_, pathType := cur.applier.Details()
			switch pathType {
			case kind.KindSimple:
				return cur
			case kind.KindMap:
				return cur
			case kind.KindIntermediate:
				if prev != nil && prev.kind == kind.KindSimple {
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
			return nil, fmt.Errorf("child was not found for %q of path %q", seg, op.Path)
		}

		cur = next
	}

	// evaluate the final node
	target = eval(cur, target)

	if target == nil {
		return nil, fmt.Errorf("no target")
	}

	if target.applier == nil {
		return nil, fmt.Errorf("no applier found for path %q", op.Path)
	}
	if target.kind == kind.KindIntermediate {
		return nil, fmt.Errorf("applier was of invalid type (Intermediate)")
	}

	return target.applier.Apply(op)
}

type Tree struct {
	mu   sync.RWMutex
	root *treeNode
}

func NewTree() *Tree {
	return &Tree{
		root: &treeNode{
			kind: kind.KindIntermediate,
		},
	}
}

type Applier interface {
	Apply(op jsondiff.Operation) ([]clientv3.Op, error)
	Details() (path string, pathType kind.Kind)
}

func (t *Tree) Register(p Applier) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.root.insert(p)
}

func (t *Tree) ApplySingle(ctx context.Context, cli *clientv3.Client, op jsondiff.Operation) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	etcdOps, err := t.root.apply(op)
	if err != nil {
		return err
	}

	_, err = cli.Txn(ctx).Then(etcdOps...).Commit()
	return err
}

func (t *Tree) applyWithTxn(ctx context.Context, cli *clientv3.Client, ops jsondiff.Patch) error {
	var etcdOps []clientv3.Op
	for _, op := range ops {
		result, err := t.root.apply(op)
		if err != nil {
			return err
		}
		etcdOps = append(etcdOps, result...)
	}

	_, err := cli.Txn(ctx).Then(etcdOps...).Commit()
	return err
}

func (t *Tree) ApplyWithTxn(ctx context.Context, cli *clientv3.Client, ops jsondiff.Patch) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, ops)
}

func (t *Tree) Apply(ctx context.Context, cli *clientv3.Client, ops jsondiff.Patch) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, ops)
}
