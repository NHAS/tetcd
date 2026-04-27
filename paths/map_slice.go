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
	return NewMapPath(path.Join(m.prefix, k), m.codec, m.presenceOnly)
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
		rel := strings.TrimPrefix(string(kv.Key), m.prefix)
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
			return strings.TrimPrefix(key, m.prefix)
		}))
}

func (m MapSlicePath[V]) Apply(ctx context.Context, cli *clientv3.Client, change json.RawMessage) ([]clientv3.Op, error) {

	if change == nil {
		return nil, fmt.Errorf("nil change provided for prefix %q", m.prefix)
	}

	isNull := func(v json.RawMessage) bool {
		return strings.TrimSpace(string(v)) == "null"
	}

	if isNull(change) {
		return []clientv3.Op{
			clientv3.OpDelete(m.prefix, clientv3.WithPrefix()),
		}, nil
	}

	var outerEntries map[string]json.RawMessage
	if err := json.Unmarshal(change, &outerEntries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal map slice patch for prefix %q: %w", m.prefix, err)
	}

	ops := make([]clientv3.Op, 0, len(outerEntries))
	mergeGetOps := make([]clientv3.Op, 0, len(outerEntries))

	type merge struct {
		key  string
		data json.RawMessage
	}

	mergeData := make([]merge, 0, len(outerEntries))

	for outerKey, outerChange := range outerEntries {
		if outerChange == nil {
			return nil, fmt.Errorf("nil change provided for outer key %q under prefix %q", outerKey, m.prefix)
		}

		outerPrefix := path.Join(m.prefix, outerKey)
		if !strings.HasSuffix(outerPrefix, "/") {
			outerPrefix += "/"
		}

		if isNull(outerChange) {
			ops = append(ops, clientv3.OpDelete(outerPrefix, clientv3.WithPrefix()))
			continue
		}

		var innerEntries map[string]json.RawMessage
		if err := json.Unmarshal(outerChange, &innerEntries); err != nil {
			return nil, fmt.Errorf("failed to unmarshal map slice patch for prefix %q outer key %q: %w", m.prefix, outerKey, err)
		}

		for innerKey, innerChange := range innerEntries {
			etcdKey := path.Join(m.prefix, outerKey, innerKey)

			if innerChange == nil {
				return nil, fmt.Errorf("nil change provided for key %q", etcdKey)
			}

			if isNull(innerChange) {
				ops = append(ops, clientv3.OpDelete(etcdKey))
				continue
			}

			if m.presenceOnly {
				ops = append(ops, clientv3.OpPut(etcdKey, ""))
				continue
			}

			mergeGetOps = append(mergeGetOps, clientv3.OpGet(etcdKey))
			mergeData = append(mergeData, merge{
				key:  etcdKey,
				data: innerChange,
			})
		}
	}

	if len(mergeGetOps) == 0 {
		return ops, nil
	}

	resp, err := cli.Txn(ctx).Then(mergeGetOps...).Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to get previous values to merge: %w", err)
	}

	if len(resp.Responses) != len(mergeData) {
		return nil, fmt.Errorf("failed to get the same number of responses for merges than issued")
	}

	for i, response := range resp.Responses {
		rangeResp := response.GetResponseRange()
		if rangeResp == nil {
			return nil, fmt.Errorf("failed to get the same number of responses for merges than issued")
		}

		var value V
		if len(rangeResp.Kvs) > 0 {
			kv := rangeResp.Kvs[0]
			if err := m.codec.DecodeToPointer(kv.Value, &value); err != nil {
				return nil, fmt.Errorf("invalid initial type for key %q: %w", mergeData[i].key, err)
			}
		}

		if err := m.codec.DecodeToPointer(mergeData[i].data, &value); err != nil {
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
