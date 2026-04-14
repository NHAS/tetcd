package paths

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NHAS/tetcd/codecs"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MapSlicePath represents a two-level expanded structure:
// prefix/{mapKey}/{index} -> V
// e.g.
// Thing/bloop
//
//	             -> "noot1":{data: 1}
//		            -> "noot2"
type MapSlicePath[V any] struct {
	prefix string
	codec  codecs.Codec[V]
}

type Tuple[V any] struct {
	Key   string
	Value V
}

func NewMapSlicePath[V any](prefix string, codec codecs.Codec[V]) MapSlicePath[V] {
	return MapSlicePath[V]{prefix: prefix, codec: codec}
}

func (m MapSlicePath[V]) Prefix() string { return m.prefix }

// Key drops down to an MapPath for a specific entry
func (m MapSlicePath[V]) Key(k string) MapPath[V] {
	return MapPath[V]{
		prefix: filepath.Join(m.prefix, k),
		codec:  m.codec,
	}
}

// List reads the entire two-level structure
func (m MapSlicePath[V]) List(ctx context.Context, cli *clientv3.Client) (map[string]map[string]V, error) {
	resp, err := cli.Get(ctx, m.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make(map[string]map[string]V, len(resp.Kvs))
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
			return nil, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
		}

		if result[outerKey] == nil {
			result[outerKey] = make(map[string]V)
		}
		result[outerKey][innerKey] = v
	}
	return result, nil
}

func (m MapSlicePath[V]) DeleteAll(ctx context.Context, cli *clientv3.Client) error {
	_, err := cli.Delete(ctx, m.prefix, clientv3.WithPrefix())
	return err
}
