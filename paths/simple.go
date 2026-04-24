package paths

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/tree/kind"
	"github.com/NHAS/tetcd/watch"
	"github.com/wI2L/jsondiff"

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
		key:   strings.TrimSuffix(key, "/"),
		codec: codec,
	}
}

func (p Path[T]) Codec() codecs.Codec[T] {
	return p.codec
}

func (p Path[T]) Details() (string, kind.Kind) {
	return p.key, kind.KindSimple
}

func (p Path[T]) Key() string {
	return p.key
}

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

type DeleteResult[T any] struct {
	Count int64

	PrevValues []T
	PrevKeys   []string
}

func (p Path[T]) Delete(ctx context.Context, cli *clientv3.Client, opts ...clientv3.OpOption) (DeleteResult[T], error) {

	res, err := cli.Delete(ctx, p.key, opts...)
	if err != nil {
		return DeleteResult[T]{}, err
	}

	result := DeleteResult[T]{
		Count: res.Deleted,
	}

	if len(res.PrevKvs) > 0 {
		result.PrevValues = make([]T, 0, len(res.PrevKvs))
		result.PrevKeys = make([]string, 0, len(res.PrevKvs))
		for _, kv := range res.PrevKvs {

			// if we were issued with keys only
			if len(kv.Value) != 0 {
				v, err := p.codec.Decode(kv.Value)
				if err != nil {
					return DeleteResult[T]{}, fmt.Errorf("decoding %q: %w", string(kv.Key), err)
				}
				result.PrevValues = append(result.PrevValues, v)
			}

			result.PrevKeys = append(result.PrevKeys, string(kv.Key))
		}
	}

	return result, nil
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

func (p Path[T]) Watch(ctx context.Context, cli *clientv3.Client) *watch.Watcher[T] {

	return watch.NewWatch(cli,
		p.key,
		p.codec,
		watch.WithPrefixTrimFunc[T](func(key string) string {
			return filepath.Base(p.key)
		}))
}

func (p Path[T]) Apply(op jsondiff.Operation) ([]clientv3.Op, error) {

	switch op.Type {
	case jsondiff.OperationAdd, jsondiff.OperationReplace:
		if op.Value == nil {
			return nil, fmt.Errorf("no value provided for operation %q: %q", op.Type, p.key)
		}

		b, err := p.codec.EncodeRaw(op.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to re-encode value for operation %q: %q", op.Type, p.key)
		}

		if _, err := p.codec.Decode(b); err != nil {
			return nil, fmt.Errorf("invalid value type for operation %q: %q", op.Type, p.key)
		}

		return []clientv3.Op{
			clientv3.OpPut(p.key, string(b)),
		}, nil

	case jsondiff.OperationRemove:
		return []clientv3.Op{
			clientv3.OpDelete(p.key),
		}, nil

	default:
		return nil, fmt.Errorf("unsupported operation %q: %q", op.Type, p.key)
	}
}
