package paths

import (
	"context"
	"errors"

	"github.com/NHAS/tetcd/codecs"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrNotFound = errors.New("key not found")

type Path[T any] struct {
	key   string
	codec codecs.Codec[T]
}

func NewPath[T any](key string, codec codecs.Codec[T]) Path[T] {
	return Path[T]{
		key:   key,
		codec: codec,
	}
}

func (p Path[T]) Codec() codecs.Codec[T] {
	return p.codec
}

func (p Path[T]) Key() string { return p.key }

// Get a single key -> value, do not add Op WithPrefix in here use the properly generated map types :)
func (p Path[T]) Get(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (T, error) {

	var zero T

	resp, err := cli.Get(ctx, p.key, opts...)
	if err != nil {
		return zero, err
	}
	if len(resp.Kvs) == 0 {
		return zero, ErrNotFound
	}
	return p.codec.Decode(resp.Kvs[0].Value)
}

func (p Path[T]) Put(ctx context.Context, cli *clientv3.Client, val T, opts ...clientv3.OpOption) error {
	data, err := p.codec.Encode(val)
	if err != nil {
		return err
	}
	_, err = cli.Put(ctx, p.key, string(data), opts...)
	return err
}

func (p Path[T]) Delete(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (int64, error) {

	res, err := cli.Delete(ctx, p.key, opts...)
	if err != nil {
		return 0, err
	}

	return res.Deleted, err
}
