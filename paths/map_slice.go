package paths

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/tree/kind"
	"github.com/NHAS/tetcd/watch"
	"github.com/wI2L/jsondiff"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MapSlicePath represents a two-level expanded structure:
// prefix/{mapKey}/{index} -> V
// e.g.
// Thing/bloop
//
//	             -> "noot1":{data: 1}
//		         -> "noot2"
type MapSlicePath[V any] struct {
	prefix       string
	codec        codecs.Codec[V]
	presenceOnly bool
}

func NewMapSlicePath[V any](prefix string, codec codecs.Codec[V], presenceOnly bool) MapSlicePath[V] {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return MapSlicePath[V]{
		prefix:       prefix,
		codec:        codec,
		presenceOnly: presenceOnly,
	}
}

func (m MapSlicePath[V]) Codec() codecs.Codec[V] {
	return m.codec
}

func (m MapSlicePath[V]) Prefix() string {
	return m.prefix
}

func (m MapSlicePath[T]) Details() (string, kind.Kind) {
	return m.prefix, kind.KindMap
}

func (m MapSlicePath[V]) PresenceOnly() bool { return m.presenceOnly }

// Key drops down to an MapPath for a specific entry
func (m MapSlicePath[V]) Key(k string) MapPath[V] {
	return MapPath[V]{
		prefix: filepath.Join(m.prefix, k),
		codec:  m.codec,
	}
}

// List reads the entire two-level structure
func (m MapSlicePath[V]) List(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (ListResult[map[string]V], error) {

	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return ListResult[map[string]V]{}, err
	}

	result := make(map[string]map[string]V, len(resp.Kvs))
	order := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		rel := strings.TrimPrefix(string(kv.Key), m.prefix+"/")
		parts := strings.SplitN(rel, "/", 2)
		if len(parts) != 2 {
			continue
		}
		outerKey := parts[0]
		innerKey := parts[1]

		v, err := m.codec.Decode(kv.Value)
		if err != nil {
			return ListResult[map[string]V]{}, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
		}

		if result[outerKey] == nil {
			result[outerKey] = make(map[string]V)
		}
		result[outerKey][innerKey] = v
		order = append(order, outerKey)
	}
	return ListResult[map[string]V]{
		Order:  order,
		Values: result,
		Rev:    resp.Header.Revision}, nil
}

func (m MapSlicePath[V]) Entries(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) ([]V, error) {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return nil, err
	}
	result := make([]V, 0, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		v, err := m.codec.Decode(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
		}
		result = append(result, v)
	}

	return result, nil
}

func (m MapSlicePath[V]) DeleteAll(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (DeleteResult[V], error) {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Delete(ctx, m.prefix, options...)
	if err != nil {
		return DeleteResult[V]{}, err
	}

	result := DeleteResult[V]{
		Count: resp.Deleted,
	}

	if len(resp.PrevKvs) > 0 {
		result.PrevValues = make([]V, 0, len(resp.PrevKvs))
		result.PrevKeys = make([]string, 0, len(resp.PrevKvs))
		for _, kv := range resp.PrevKvs {

			// if we were issued with keys only
			if len(kv.Value) != 0 {
				v, err := m.codec.Decode(kv.Value)
				if err != nil {
					return DeleteResult[V]{}, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
				}
				result.PrevValues = append(result.PrevValues, v)
			}

			result.PrevKeys = append(result.PrevKeys, string(kv.Key))
		}
	}

	return result, nil
}

func (m MapSlicePath[V]) Watch(ctx context.Context, cli *clientv3.Client) *watch.Watcher[V] {

	return watch.NewWatch(cli,
		m.prefix,
		m.codec,
		watch.WithPrefix[V](),
		watch.WithPrefixTrimFunc[V](func(key string) string {
			return strings.TrimPrefix(key, m.prefix+"/")
		}))
}

func (m MapSlicePath[V]) Apply(op jsondiff.Operation) ([]clientv3.Op, error) {
	return nil, nil
}
