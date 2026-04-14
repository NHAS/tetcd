package paths

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/NHAS/tetcd/codecs"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MapPath represents an etcd prefix where each key is a map entry.
// e.g. wag/Acls/Groups/{k} -> V
type MapPath[Inner any] struct {
	prefix string
	codec  codecs.Codec[Inner]
}

func NewMapPath[Inner any](prefix string, codec codecs.Codec[Inner]) MapPath[Inner] {
	return MapPath[Inner]{
		prefix: prefix,
		codec:  codec,
	}
}

func (m MapPath[V]) Codec() codecs.Codec[V] {
	return m.codec
}

func (m MapPath[V]) Prefix() string {
	return m.prefix
}

func (m MapPath[V]) Key(k string) Path[V] {
	return Path[V]{
		key:   filepath.Join(m.prefix, k),
		codec: m.codec,
	}
}

// Get all sub keys stripped of their parent prefix
func (m MapPath[V]) Keys(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) ([]string, error) {
	options := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly()}
	options = append(options, opts...)

	resp, err := cli.Get(context.Background(), m.prefix, options...)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(resp.Kvs))
	for _, r := range resp.Kvs {
		result = append(result, strings.TrimPrefix(string(r.Key), m.prefix+"/"))
	}

	return result, nil
}

func (m MapPath[V]) List(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (map[string]V, error) {
	options := []clientv3.OpOption{clientv3.WithPrefix()}
	options = append(options, opts...)

	resp, err := cli.Get(ctx, m.prefix, options...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]V, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		// Strip prefix + trailing slash to get the map key
		k := strings.TrimPrefix(string(kv.Key), m.prefix+"/")
		v, err := m.codec.Decode(kv.Value)
		if err != nil {
			return nil, err
		}
		result[k] = v
	}

	return result, nil
}

func (m MapPath[V]) Delete(ctx context.Context, cli *clientv3.Client, k string) error {
	_, err := cli.Delete(ctx, filepath.Join(m.prefix, k))
	return err
}

func (m MapPath[V]) DeleteAll(ctx context.Context, cli *clientv3.Client) (int64, error) {
	delResponse, err := cli.Delete(ctx, m.prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}

	return delResponse.Deleted, err
}

type MapWatchEvent[V any] struct {
	Key     string
	Value   V
	Deleted bool
}

func (m MapPath[V]) Watch(ctx context.Context, cli *clientv3.Client) <-chan MapWatchEvent[V] {
	ch := make(chan MapWatchEvent[V])

	go func() {
		defer close(ch)

		watchCh := cli.Watch(ctx, m.prefix, clientv3.WithPrefix())
		for resp := range watchCh {
			for _, ev := range resp.Events {
				k := strings.TrimPrefix(string(ev.Kv.Key), m.prefix+"/")

				if ev.Type == clientv3.EventTypeDelete {
					var zero V
					ch <- MapWatchEvent[V]{Key: k, Value: zero, Deleted: true}
					continue
				}

				v, err := m.codec.Decode(ev.Kv.Value)
				if err != nil {
					// Could return an error channel alongside, but keeping it simple for now
					continue
				}

				ch <- MapWatchEvent[V]{Key: k, Value: v}
			}
		}
	}()

	return ch
}
