package history

import "github.com/NHAS/tetcd/cmd/test/config"

// Applied records which configs have been applied to etcd.
//
// It is stored in etcd on its own, under its own prefix, and is not part of the config's tree. The tree
// only ever sees the config, and this is only used to remember what the config was.
type Applied struct {
	// The version of the config that is in etcd now, 0 if nothing has been applied.
	Current int

	// Every config that has been applied, by version number. Each is kept whole, as the JSON it was applied as.
	Versions map[string]config.Config
}
