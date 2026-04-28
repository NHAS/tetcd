package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/NHAS/tetcd/tree/kind"
	jsonpatch "github.com/evanphx/json-patch"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type treeNode struct {
	kind     kind.Kind
	applier  Applier // non-nil for KindSimple and KindMap
	children map[string]*treeNode
}

func (t *treeNode) insert(applier Applier) {
	key, applierType := applier.Details()
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

	cur.applier, cur.kind = applier, applierType
}

func (t *treeNode) find(path string) Applier {
	segments := strings.Split(strings.Trim(path, "/"), "/")

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
			if target != nil && target.kind == kind.KindMap {
				break
			}
			return nil
		}

		cur = next
	}

	// evaluate the final node
	target = eval(cur, target)
	if target == nil {
		return nil
	}

	if target.kind == kind.KindIntermediate {
		return nil
	}

	return target.applier
}

type Tree[T any] struct {
	mu     sync.RWMutex
	root   *treeNode
	prefix string
}

func NewTree[T any]() *Tree[T] {
	return NewTreeWithPrefix[T]("")
}

func NewTreeWithPrefix[T any](prefix string) *Tree[T] {
	return &Tree[T]{
		root: &treeNode{
			kind: kind.KindIntermediate,
		},
		prefix: prefix,
	}
}

type Applier interface {
	Apply(ctx context.Context, cli *clientv3.Client, change json.RawMessage) ([]clientv3.Op, error)
	Details() (path string, pathType kind.Kind)
}

func (t *Tree[T]) Register(p Applier) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if p == nil {
		return
	}

	t.root.insert(p)
}

func (t *Tree[T]) applyWithTxn(ctx context.Context, cli *clientv3.Client, plan Plan) error {

	var etcdOps []clientv3.Op
	for _, op := range plan.ops {
		ops, err := op.action(ctx, cli)
		if err != nil {
			return err
		}

		etcdOps = append(etcdOps, ops...)
	}

	_, err := cli.Txn(ctx).Then(etcdOps...).Commit()
	return err
}

func (t *Tree[T]) Apply(ctx context.Context, cli *clientv3.Client, plan Plan) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, plan)
}

type opsFunc func(ctx context.Context, cli *clientv3.Client) ([]clientv3.Op, error)

type Plan struct {
	ops []Op
}

func (p *Plan) KeysChanged() []string {
	result := []string{}

	for _, op := range p.ops {
		result = append(result, op.key)
	}
	return result
}

type Op struct {
	action opsFunc

	ChangeItem
}

type ChangeItem struct {
	key    string
	change json.RawMessage
}

func (t *Tree[T]) Plan(ctx context.Context, originalJSON, modifiedJSON []byte) (Plan, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(bytes.TrimSpace(originalJSON)) == 0 {
		originalJSON = []byte("{}")
	}

	var validate T

	dec := json.NewDecoder(bytes.NewBuffer(modifiedJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&validate); err != nil {
		return Plan{}, fmt.Errorf("failed to decode new config: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	if err != nil {
		return Plan{}, fmt.Errorf("failed to create merge patch: %w", err)
	}

	// the top level elements that have been changed
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(patchBytes, &patch); err != nil {
		return Plan{}, fmt.Errorf("failed to unmarshal merge patch: %w", err)
	}

	queue := make([]ChangeItem, 0, len(patch))
	for k, v := range patch {
		queue = append(queue, ChangeItem{key: k, change: v})
	}

	var plan Plan
	for len(queue) > 0 {

		if err := ctx.Err(); err != nil {
			return Plan{}, fmt.Errorf("cancelled by context: %w", err)
		}

		current := queue[0]
		queue = queue[1:]

		fullKey := current.key
		if t.prefix != "" {
			fullKey = path.Join(t.prefix, current.key)
		}

		applier := t.root.find(fullKey)
		if applier != nil {
			// if we've found a match, add the applier and skip trying to decode
			// this means we collect map types but ignore structs
			plan.ops = append(plan.ops, Op{
				action: func(ctx context.Context, cli *clientv3.Client) ([]clientv3.Op, error) {
					return applier.Apply(ctx, cli, current.change)
				},
				ChangeItem: ChangeItem{
					key:    fullKey,
					change: current.change,
				},
			})
			continue
		}

		// Try to decode as nested object
		// map types should be caught by the t.root.find above
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(current.change, &nested); err == nil {

			// json merge can result in items being nil/null when removed
			if nested == nil {
				return Plan{}, fmt.Errorf("path %q was marked as deleted, however no matcher applies", current.key)
			}

			// if we're a nested item, and not matching a map then continue adding items to the queue
			for k, v := range nested {
				queue = append(queue,
					ChangeItem{
						key:    path.Join(current.key, k),
						change: v,
					},
				)
			}

			continue
		}

		return Plan{}, fmt.Errorf("key: %q has no matching applier", fullKey)
	}

	return plan, nil
}
