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

func (t *Tree[T]) applyWithTxn(ctx context.Context, cli *clientv3.Client, originalJSON, modifiedJSON []byte) error {

	var validate T

	dec := json.NewDecoder(bytes.NewBuffer(modifiedJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&validate); err != nil {
		return fmt.Errorf("failed to decode new config: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	if err != nil {
		return fmt.Errorf("failed to create merge patch: %w", err)
	}

	// the top level elements that have been changed
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(patchBytes, &patch); err != nil {
		return fmt.Errorf("failed to unmarshal merge patch: %w", err)
	}

	type actionsFunc func() ([]clientv3.Op, error)

	type item struct {
		key  string
		data json.RawMessage
	}

	queue := make([]item, 0, len(patch))
	for k, v := range patch {
		queue = append(queue, item{key: k, data: v})
	}

	var actions []actionsFunc
	for len(queue) > 0 {

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
			actions = append(actions, func() ([]clientv3.Op, error) {
				return applier.Apply(ctx, cli, current.data)
			})
			continue
		}

		// Try to decode as nested object
		// map types should be caught by the t.root.find above
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(current.data, &nested); err == nil {

			for k, v := range nested {
				queue = append(queue, item{key: path.Join(fullKey, k), data: v})
			}

			continue
		}
	}

	var etcdOps []clientv3.Op
	for _, action := range actions {
		ops, err := action()
		if err != nil {
			return err
		}

		etcdOps = append(etcdOps, ops...)
	}

	_, err = cli.Txn(ctx).Then(etcdOps...).Commit()
	return err
}

func (t *Tree[T]) Apply(ctx context.Context, cli *clientv3.Client, originalJSON, modifiedJSON []byte) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, originalJSON, modifiedJSON)
}
