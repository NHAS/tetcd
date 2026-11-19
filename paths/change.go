package paths

import (
	"encoding/json"
	"fmt"

	"github.com/NHAS/tetcd/codecs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ChangeType is what a plan does to a key.
type ChangeType int

const (
	// Add creates the key, or overwrites it if it exists.
	Add ChangeType = iota + 1
	// Remove deletes the key.
	Remove
)

// Change is a single key that a plan creates, overwrites or deletes. A key is only in a plan once, so a changed
// entry is a lone Add, never a Remove followed by an Add.
type Change struct {
	Type  ChangeType
	Key   string          // the full etcd key
	Value json.RawMessage // the new value for an Add, the value that was stored for a Remove
}

// applyChanges turns changes into etcd operations. Keys that are presenceOnly hold no value, their existence is the data.
func applyChanges[T any](codec codecs.Codec[T], presenceOnly bool, changes []Change) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(changes))

	for _, change := range changes {
		switch {
		case change.Type == Remove:
			ops = append(ops, clientv3.OpDelete(change.Key))
		case presenceOnly:
			ops = append(ops, clientv3.OpPut(change.Key, ""))
		default:
			// round trip through the codec so the stored bytes are the codec's, and the type is checked
			var value T
			if err := codec.DecodeToPointer(change.Value, &value); err != nil {
				return nil, fmt.Errorf("invalid value for key %q: %w", change.Key, err)
			}

			encoded, err := codec.Encode(value)
			if err != nil {
				return nil, fmt.Errorf("failed to encode value for key %q: %w", change.Key, err)
			}

			ops = append(ops, clientv3.OpPut(change.Key, string(encoded)))
		}
	}

	return ops, nil
}
