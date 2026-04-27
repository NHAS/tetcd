package paths

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/tree/kind"
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
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

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

func (p MapPath[T]) Details() (string, kind.Kind) {
	return p.prefix, kind.KindMap
}

func (m MapPath[V]) PresenceOnly() bool {
	return m.presenceOnly
}

func (m MapPath[V]) Key(k string) Path[V] {
	return Path[V]{
		key:   path.Join(m.prefix, k),
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

type ListResult[V any] struct {
	Order  []string
	Values map[string]V
	Rev    int64
}

// List returns the keys of the value in the order returned by etcd
// and then a map of those keys to values
func (m MapPath[V]) List(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (ListResult[V], error) {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return ListResult[V]{}, err
	}

	result := make(map[string]V, len(resp.Kvs))
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		// Strip prefix + trailing slash to get the map key
		k := strings.TrimPrefix(string(kv.Key), m.prefix+"/")
		v, err := m.codec.Decode(kv.Value)
		if err != nil {
			return ListResult[V]{}, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
		}
		result[k] = v
		keys = append(keys, k)
	}

	return ListResult[V]{
		Order:  keys,
		Values: result,
		Rev:    resp.Header.Revision,
	}, nil
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

func (m MapPath[V]) Apply(ctx context.Context, cli *clientv3.Client, change json.RawMessage) ([]clientv3.Op, error) {

	if string(change) == "null" {
		return []clientv3.Op{
			clientv3.OpDelete(m.prefix, clientv3.WithPrefix()),
		}, nil
	}

	var entries map[string]json.RawMessage
	if err := json.Unmarshal(change, &entries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal map patch for prefix %q: %w", m.prefix, err)
	}

	ops := make([]clientv3.Op, 0, len(entries))

	mergeGetOps := make([]clientv3.Op, 0, len(entries))

	type merge struct {
		key  string
		data json.RawMessage
	}

	mergeData := make([]merge, 0, len(entries))
	for k, v := range entries {
		etcdKey := path.Join(m.prefix, k)

		// null value means delete this key
		if string(v) == "null" {
			ops = append(ops, clientv3.OpDelete(etcdKey))
			continue
		}

		if m.presenceOnly {
			ops = append(ops, clientv3.OpPut(etcdKey, ""))
			continue
		}

		mergeGetOps = append(mergeGetOps, clientv3.OpGet(etcdKey))
		mergeData = append(mergeData, merge{key: etcdKey, data: v})
	}

	if len(mergeGetOps) == 0 {
		return ops, nil
	}

	resp, err := cli.Txn(ctx).Then(mergeGetOps...).Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to get previous values to merge: %w", err)
	}

	for i, response := range resp.Responses {

		if response.GetResponseRange() == nil {
			return nil, fmt.Errorf("failed to get the same number of responses for merges than issued")
		}

		var value V

		// this is a new key, just apply the merge directly
		if len(response.GetResponseRange().Kvs) > 0 {
			kv := response.GetResponseRange().Kvs[0]

			err := m.codec.DecodeToPointer(kv.Value, &value)
			if err != nil {
				return nil, fmt.Errorf("invalid initial type for key %q: %w", mergeData[i].key, err)
			}
		}

		// this is only valid if the codec supports partial updates like the json standard library
		// this is a limitation of this implementation
		err = m.codec.DecodeToPointer(mergeData[i].data, &value)
		if err != nil {
			return nil, fmt.Errorf("failed to apply merged for key %q: %w", mergeData[i].key, err)
		}

		merged, err := m.codec.Encode(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode merged value for key %q: %w", mergeData[i].key, err)
		}

		ops = append(ops, clientv3.OpPut(mergeData[i].key, string(merged)))
	}
	return ops, nil
}
