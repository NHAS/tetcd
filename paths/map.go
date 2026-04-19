package paths

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/watch"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MapPath represents an etcd prefix where each key is a map entry.
// e.g. wag/Acls/Groups/{k} -> V
type MapPath[Inner any] struct {
	prefix       string
	codec        codecs.Codec[Inner]
	presenceOnly bool
}

func NewMapPath[Inner any](prefix string, codec codecs.Codec[Inner], presenceOnly bool) MapPath[Inner] {
	return MapPath[Inner]{
		prefix:       prefix,
		codec:        codec,
		presenceOnly: presenceOnly,
	}
}

func (m MapPath[V]) Codec() codecs.Codec[V] {
	return m.codec
}

func (m MapPath[V]) Prefix() string {
	return m.prefix
}

func (m MapPath[V]) PresenceOnly() bool {
	return m.presenceOnly
}

func (m MapPath[V]) Key(k string) Path[V] {
	return Path[V]{
		key:   filepath.Join(m.prefix, k),
		codec: m.codec,
	}
}

func (m MapPath[V]) All() Path[V] {

	prefix := m.prefix
	if !strings.HasSuffix(m.prefix, "/") {
		prefix += "/"
	}

	return Path[V]{
		key:   prefix,
		codec: m.codec,
	}
}

func (m MapPath[V]) Count(ctx context.Context, cli *clientv3.Client) (int64, error) {
	resp, err := cli.Get(ctx, m.prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// Get all sub keys stripped of their parent prefix
func (m MapPath[V]) Keys(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) ([]string, error) {
	options := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(resp.Kvs))
	for _, r := range resp.Kvs {
		result = append(result, strings.TrimPrefix(string(r.Key), m.prefix+"/"))
	}

	return result, nil
}

func (m MapPath[V]) Entries(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) ([]V, error) {
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

// List returns the keys of the value in the order returned by etcd
// and then a map of those keys to values
func (m MapPath[V]) List(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) ([]string, map[string]V, error) {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return nil, nil, err
	}

	result := make(map[string]V, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		// Strip prefix + trailing slash to get the map key
		k := strings.TrimPrefix(string(kv.Key), m.prefix+"/")
		v, err := m.codec.Decode(kv.Value)
		if err != nil {
			return nil, nil, err
		}
		result[k] = v
		keys = append(keys, k)
	}

	return keys, result, nil
}

func (m MapPath[V]) Delete(ctx context.Context, cli *clientv3.Client, k string) error {
	resp, err := cli.Delete(ctx, filepath.Join(m.prefix, k))
	if err != nil {
		return err
	}

	if resp.Deleted == 0 {
		return ErrNotFound
	}

	return err
}

func (m MapPath[V]) DeleteAll(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (DeleteResult[V], error) {

	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	res, err := cli.Delete(ctx, m.prefix, options...)
	if err != nil {
		return DeleteResult[V]{}, err
	}

	result := DeleteResult[V]{
		Count: res.Deleted,
	}

	if len(res.PrevKvs) > 0 {
		result.PrevValues = make([]V, 0, len(res.PrevKvs))
		result.PrevKeys = make([]string, 0, len(res.PrevKvs))
		for _, kv := range res.PrevKvs {

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

func (m MapPath[V]) Watch(ctx context.Context, cli *clientv3.Client) *watch.Watcher[V] {

	return watch.NewWatch(cli,
		m.prefix,
		m.codec,
		watch.WithPrefix[V](),
		watch.WithPrefixTrimFunc[V](func(key string) string {
			return strings.TrimPrefix(key, m.prefix+"/")
		}))
}
