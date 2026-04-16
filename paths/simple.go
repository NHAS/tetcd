package paths

import (
	"context"
	"errors"
	"fmt"

	"github.com/NHAS/tetcd/codecs"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"
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

// Update changes the value of a single key path, and does so safely
// if the key is updated by another process while attempting to mutate the value, the value is run again
// a max of 10 attempt will occur before quitting
// as such, other than modifying the T value, do not have updateFunc retain state or modify things outside of updateFunc
// upsert will create the key if it doesnt exist
func (p Path[T]) Update(ctx context.Context, cli *clientv3.Client, upsert bool, updateFunc func(T) (T, error)) error {
	//https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L382

	if updateFunc == nil {
		return errors.New("no updateFunc function set in safe update")
	}

	origState, err := cli.Get(ctx, p.key)
	if err != nil {
		return err
	}

	if upsert && origState.Count == 0 {

		var zero T
		newValue, err := updateFunc(zero)
		if err != nil {
			return err
		}

		data, err := p.codec.Encode(newValue)
		if err != nil {
			return err
		}

		txnResp, err := cli.Txn(ctx).If(
			clientv3util.KeyMissing(p.key),
		).Then(
			clientv3.OpPut(p.key, string(data)),
		).Else(
			clientv3.OpGet(p.key),
		).Commit()

		if err != nil {
			return err
		}

		if txnResp.Succeeded {
			return nil
		}
		// If the key was created while we were trying to create it, do the normal update proceedure

		origState = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
	}

	for range 10 {
		if origState.Count == 0 {
			return errors.New("no record found")
		}

		initialState, err := p.codec.Decode(origState.Kvs[0].Value)
		if err != nil {
			return err
		}

		newValue, err := updateFunc(initialState)
		if err != nil {
			return err
		}

		data, err := p.codec.Encode(newValue)
		if err != nil {
			return err
		}

		// if the key hasnt been updated while we've been working on it, do the placement, otherwise, repull, remutate, try again
		txnResp, err := cli.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(p.key), "=", origState.Kvs[0].ModRevision),
		).Then(
			clientv3.OpPut(p.key, string(data)),
		).Else(
			clientv3.OpGet(p.key),
		).Commit()

		if err != nil {
			return err
		}

		if !txnResp.Succeeded {
			origState = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			continue
		}

		return nil
	}

	return fmt.Errorf("failed to update %q safetly after 10 attempts", p.key)
}
