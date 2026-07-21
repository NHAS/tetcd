package tree

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/NHAS/tetcd/tree/kind"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ignored struct {
	prefix string
}

func newIgnoredPath(path string) Applier {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	return &ignored{
		prefix: path,
	}
}

func (i *ignored) Apply(ctx context.Context, cli *clientv3.Client, change json.RawMessage) ([]clientv3.Op, error) {
	return nil, nil
}

func (i *ignored) Details() (path string, pathType kind.Kind) {
	return i.prefix, kind.Ignored
}
