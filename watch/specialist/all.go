package specialist

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/NHAS/tetcd"
	"github.com/NHAS/tetcd/codecs"
	"github.com/NHAS/tetcd/watch"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type GetFunc[T any] func(ctx context.Context, cli *clientv3.Client, failEarly bool, opts ...tetcd.TxnOp) (result T, err error)

func NewAllWatcher[T any](ctx context.Context, cli *clientv3.Client, prefix string, get GetFunc[T]) *watch.Watcher[T] {
	var (
		mu           sync.Mutex
		lastRevision int64
	)
	parser := func(event *clientv3.Event, _ codecs.Codec[T]) (p watch.Event[T], skip bool, err error) {

		mu.Lock()
		if event.Kv.ModRevision == lastRevision {
			mu.Unlock()
			return p, true, fmt.Errorf("already processed revision %d", event.Kv.ModRevision)
		}
		lastRevision = event.Kv.ModRevision
		mu.Unlock()

		p.Key = strings.TrimPrefix(string(event.Kv.Key), prefix)

		switch event.Type {
		case mvccpb.DELETE:
			p.Type = watch.DELETED

			previous, err := get(ctx, cli, true, tetcd.WithRev(event.Kv.ModRevision-1))
			if err != nil {
				return p, false, err
			}
			p.Previous = previous
			return p, false, nil

		case mvccpb.PUT:
			p.Type = watch.CREATED

			current, err := get(ctx, cli, true, tetcd.WithRev(event.Kv.ModRevision))
			if err != nil {
				return p, false, err
			}
			p.Current = current

			if event.IsModify() {
				p.Type = watch.MODIFIED
				previous, err := get(ctx, cli, true, tetcd.WithRev(event.Kv.ModRevision-1))
				if err != nil {
					return p, false, err
				}
				p.Previous = previous
			}

			return p, false, nil

		default:
			return p, false, errors.New("unknown event type")
		}

	}

	return watch.NewWatch(cli,
		prefix,
		codecs.NewNoopCodec[T](),

		watch.WithContext[T](ctx),
		watch.WithPrefix[T](),
		watch.WithEventParser(parser),
	)
}
