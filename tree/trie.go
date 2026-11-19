package tree

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"path"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/NHAS/tetcd/paths"
	"github.com/NHAS/tetcd/tree/kind"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type treeNode struct {
	kind     kind.Kind
	applier  Applier // non-nil for KindSimple, KindMap and KindMapSlice
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
				kind: kind.Intermediate,
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
			case kind.Simple, kind.Map, kind.MapSlice, kind.Ignored:
				return cur
			case kind.Intermediate:
				if prev != nil && prev.kind == kind.Simple {
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
			if target != nil && (target.kind == kind.Map || target.kind == kind.MapSlice) {
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

	if target.kind == kind.Intermediate {
		return nil
	}

	return target.applier
}

type Tree[T any] struct {
	mu   sync.RWMutex
	root *treeNode

	// the prefix we're storing keys in the db, so we can append them to the patch
	prefix string

	// the top level struct name, which is present in the generated code
	// but not within json diffs
	topLevelStruct string
}

func NewTree[T any](topLevelStruct string) *Tree[T] {
	return NewTreeWithPrefix[T]("", topLevelStruct)
}

func NewTreeWithPrefix[T any](prefix, topLevelStruct string) *Tree[T] {
	return &Tree[T]{
		root: &treeNode{
			kind: kind.Intermediate,
		},
		prefix:         prefix,
		topLevelStruct: topLevelStruct,
	}
}

// Applier turns the planned changes under its path into etcd operations.
type Applier interface {
	Apply(ctx context.Context, cli *clientv3.Client, changes []paths.Change) ([]clientv3.Op, error)
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

func (t *Tree[T]) Ignore(ignorePaths ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, path := range ignorePaths {
		t.root.insert(newIgnoredPath(paths.Join(t.prefix, path)))
	}
}

func (t *Tree[T]) Print() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	printLayer("", t.root)
}

func printLayer(before string, item *treeNode) {
	for name, i := range item.children {
		next := before + "/" + name
		log.Println(next)
		printLayer(next, i)
	}
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

func (p *Plan) Changed() bool {
	return len(p.ops) > 0
}

// Changes lists every key that will be added or removed, along with its value.
func (p *Plan) Changes() []paths.Change {
	var result []paths.Change
	for _, op := range p.ops {
		result = append(result, op.changes...)
	}
	return result
}

func (p *Plan) KeysChanged() []string {
	result := []string{}

	for _, change := range p.Changes() {
		result = append(result, change.Key)
	}
	return result
}

// Op is everything one applier has to do.
type Op struct {
	action  opsFunc
	changes []paths.Change
}

// how many levels of keys sit below an applier's path before a value is stored whole
var storedDepth = map[kind.Kind]int{
	kind.Simple:   0,
	kind.Map:      1,
	kind.MapSlice: 2,
}

func (t *Tree[T]) Plan(ctx context.Context, originalJSON, modifiedJSON []byte) (Plan, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return Plan{}, fmt.Errorf("cancelled by context: %w", err)
	}

	if len(bytes.TrimSpace(originalJSON)) == 0 {
		originalJSON = []byte("{}")
	}

	var validate T
	if err := json.NewDecoder(bytes.NewReader(modifiedJSON)).Decode(&validate); err != nil {
		return Plan{}, fmt.Errorf("failed to decode new config: %w", err)
	}

	before, err := decode(originalJSON)
	if err != nil {
		return Plan{}, fmt.Errorf("failed to decode original config: %w", err)
	}

	after, err := decode(modifiedJSON)
	if err != nil {
		return Plan{}, fmt.Errorf("failed to decode new config: %w", err)
	}

	ops, err := t.diff(path.Join(t.prefix, t.topLevelStruct), before, after)
	return Plan{ops: ops}, err
}

// diff returns the ops that take the config at key from before to after. Structs are gone through
// until an applier is reached, which owns everything below it.
func (t *Tree[T]) diff(key string, before, after any) ([]Op, error) {
	if before == nil && after == nil {
		return nil, nil
	}

	if applier := t.root.find(key); applier != nil {
		_, applierKind := applier.Details()
		if applierKind == kind.Ignored {
			return nil, nil
		}

		depth := storedDepth[applierKind]
		changes, err := compare(entries(key, before, depth), entries(key, after, depth))
		if err != nil || len(changes) == 0 {
			return nil, err
		}

		return []Op{{
			changes: changes,
			action: func(ctx context.Context, cli *clientv3.Client) ([]clientv3.Op, error) {
				return applier.Apply(ctx, cli, changes)
			},
		}}, nil
	}

	beforeFields, beforeOk := before.(map[string]any)
	afterFields, afterOk := after.(map[string]any)
	if (before != nil && !beforeOk) || (after != nil && !afterOk) {
		return nil, fmt.Errorf("key: %q has no matching applier", key)
	}

	var ops []Op
	for _, name := range union(beforeFields, afterFields) {
		fieldOps, err := t.diff(path.Join(key, name), beforeFields[name], afterFields[name])
		if err != nil {
			return nil, err
		}

		ops = append(ops, fieldOps...)
	}

	return ops, nil
}

// compare detemine what additions or removals have taken place within a map
func compare(before, after map[string]any) ([]paths.Change, error) {
	var result []paths.Change
	for _, key := range union(before, after) {
		previous, existed := before[key]
		current, exists := after[key]

		// if the key & value is unchanged just skip it
		if existed && exists && reflect.DeepEqual(previous, current) {
			continue
		}

		change := paths.Change{
			Type: paths.Remove,
			Key:  key,
		}

		//if the key is new we add it
		if exists {
			change.Type = paths.Add

			var err error
			if change.Value, err = json.Marshal(current); err != nil {
				return nil, fmt.Errorf("failed to encode value of %q when creating addition op in compare: %w", key, err)
			}
		}

		result = append(result, change)
	}

	return result, nil
}

// entries lists the values stored under key. Maps and arrays are split depth times, an array being a
// set whose elements are keys, and anything below that is stored whole.
func entries(key string, value any, depth int) map[string]any {
	result := map[string]any{}

	switch v := value.(type) {
	case nil:
	case map[string]any:
		if depth > 0 {
			for k, child := range v {
				maps.Copy(result, entries(path.Join(key, k), child, depth-1))
			}
			return result
		}
		result[key] = v
	case []any:
		if depth > 0 {
			for _, element := range v {
				result[path.Join(key, fmt.Sprint(element))] = element
			}
			return result
		}
		result[key] = v
	default:
		result[key] = v
	}

	return result
}

// Take a union of the keys for both maps
func union[V any](a, b map[string]V) []string {
	all := map[string]struct{}{}
	for k := range a {
		all[k] = struct{}{}
	}
	for k := range b {
		all[k] = struct{}{}
	}

	return slices.Collect(maps.Keys(all))
}

// decode reads a config, keeping numbers as written
func decode(config []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()

	var value any
	err := dec.Decode(&value)
	return value, err
}
