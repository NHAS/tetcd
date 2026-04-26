package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
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

func (t *treeNode) apply(ctx context.Context, cli *clientv3.Client, fullPath string, newValue json.RawMessage) ([]clientv3.Op, error) {
	if t.children == nil {
		return nil, fmt.Errorf("no children in root")
	}

	segments := strings.Split(strings.Trim(fullPath, "/"), "/")

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
			return nil, fmt.Errorf("child was not found for %q of path %q", seg, fullPath)
		}

		cur = next
	}

	// evaluate the final node
	target = eval(cur, target)

	if target == nil {
		return nil, fmt.Errorf("no target")
	}

	if target.applier == nil {
		return nil, fmt.Errorf("no applier found for path %q", fullPath)
	}
	if target.kind == kind.KindIntermediate {
		return nil, fmt.Errorf("applier was of invalid type (Intermediate)")
	}

	return target.applier.Apply(ctx, cli, newValue)
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

	t.root.insert(p)
}

func (t *Tree[T]) applyWithTxn(ctx context.Context, cli *clientv3.Client, originalJSON, modifiedJSON []byte) error {
	var etcdOps []clientv3.Op
	ops, err := extract[T](originalJSON, modifiedJSON)
	if err != nil {
		return err
	}
	for path, newValue := range ops {
		result, err := t.root.apply(ctx, cli, path, newValue)
		if err != nil {
			return err
		}
		etcdOps = append(etcdOps, result...)
	}

	_, err = cli.Txn(ctx).Then(etcdOps...).Commit()
	return err
}

func (t *Tree[T]) ApplyWithTxn(ctx context.Context, cli *clientv3.Client, originalJSON, modifiedJSON []byte) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, originalJSON, modifiedJSON)
}

func (t *Tree[T]) Apply(ctx context.Context, cli *clientv3.Client, originalJSON, modifiedJSON []byte) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.applyWithTxn(ctx, cli, originalJSON, modifiedJSON)
}

func extract[T any](originalJSON, modifiedJSON []byte) (map[string]json.RawMessage, error) {

	var validate T

	dec := json.NewDecoder(bytes.NewBuffer(modifiedJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&validate); err != nil {
		return nil, fmt.Errorf("failed to decode new config: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to create merge patch: %w", err)
	}

	var patch map[string]json.RawMessage
	if err := json.Unmarshal(patchBytes, &patch); err != nil {
		return nil, fmt.Errorf("failed to unmarshal merge patch: %w", err)
	}

	return extractPatchPaths(patch, ""), nil
}

// this gets every single change down to the path, we should probably make sure it doesnt unwrap some types like maps
func extractPatchPaths(obj map[string]json.RawMessage, prefix string) map[string]json.RawMessage {
	paths := make(map[string]json.RawMessage)
	for k, v := range obj {
		fullKey := k
		if prefix != "" {
			fullKey = filepath.Join(prefix, k)
		}

		// Try to decode as nested object
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(v, &nested); err == nil {
			maps.Copy(paths, extractPatchPaths(nested, fullKey))
			continue
		}

		paths[fullKey] = v
	}
	return paths
}
