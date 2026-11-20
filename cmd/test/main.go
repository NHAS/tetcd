package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/NHAS/tetcd"
	"github.com/NHAS/tetcd/cmd/test/config"
	"github.com/NHAS/tetcd/paths"

	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
This runs a test example where you can go and change a config on disk and apply it automagically to etcd

sudo docker run -d \
  --name etcd \
  -p 127.0.0.1:2379:2379 \
  quay.io/coreos/etcd:v3.5.30 \
  etcd \
    --name node1 \
    --data-dir /etcd-data \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://localhost:2379 \
    --listen-peer-urls http://localhost:2380 \
    --initial-advertise-peer-urls http://localhost:2380 \
    --initial-cluster node1=http://localhost:2380
*/

// The config, and the tree that applies it (ConfigDiffer):
//go:generate go run github.com/NHAS/tetcd/cmd/tetcd-gen -type=github.com/NHAS/tetcd/cmd/test/config.Config -out=config_etcd.go -prefix=wagtest

// The record of which configs have been applied, which is separate and isn't applied through the tree:
//go:generate go run github.com/NHAS/tetcd/cmd/tetcd-gen -type=github.com/NHAS/tetcd/cmd/test/history.Applied -out=applied_etcd.go -prefix=wagtest-history

func main() {
	if len(os.Args) != 2 {
		log.Fatal("incorrect number of args")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	desired, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal("could not read new desired config: ", err)
	}

	version, err := apply(ctx, client, desired)
	if err != nil {
		ConfigDiffer.Print()
		log.Fatalf("failed to apply config: %v", err)
	}

	log.Printf("config is at version %d", version)

	name, err := Config.Name().Get(context.Background(), client)
	log.Printf("name: %q: %v", name, err)

	server, err := tetcd.IgnoreEmpty(Config.Server.Get(context.Background(), client))
	log.Printf("server: %+v: %v", server, err)
}

// apply works out what is different between the config that was last applied and desired, applies
// only that, and records desired as the new version. It returns the version etcd is at afterwards.
func apply(ctx context.Context, client *clientv3.Client, desired []byte) (int, error) {
	// The last applied config is the "before" of the plan, it is the only thing the history is used for.
	// The first time there is none, and an empty "before" plans the whole config.
	current, err := tetcd.IgnoreEmpty(Applied.Current().Get(ctx, client))
	if err != nil {
		return 0, fmt.Errorf("failed to read the current version: %w", err)
	}

	var before []byte
	if current != 0 {
		last, err := Applied.Versions().Key(strconv.Itoa(current)).Get(ctx, client)
		if err != nil {
			return 0, fmt.Errorf("failed to read version %d: %w", current, err)
		}

		if before, err = json.Marshal(last); err != nil {
			return 0, fmt.Errorf("failed to encode version %d: %w", current, err)
		}
	}

	// The tree is all there is to using tetcd: plan what changed, then apply it.
	plan, err := ConfigDiffer.Plan(ctx, before, desired)
	if err != nil {
		return 0, fmt.Errorf("failed to plan: %w", err)
	}

	if !plan.Changed() {
		log.Println("nothing to apply")
		return current, nil
	}

	for _, change := range plan.Changes() {
		action := "add"
		if change.Type == paths.Remove {
			action = "remove"
		}

		log.Printf("%s %s", action, change.Key)
	}

	if err := ConfigDiffer.Apply(ctx, client, plan); err != nil {
		return 0, fmt.Errorf("failed to apply: %w", err)
	}

	// Record what was applied. The config goes in first, so Current never points at a version that isn't there.
	version := current + 1

	var d config.Config
	err = json.Unmarshal(desired, &d)
	if err != nil {
		return 0, fmt.Errorf("could not unmarshal: %w", err)
	}

	if err := Applied.Versions().Key(strconv.Itoa(version)).Put(ctx, client, d); err != nil {
		return 0, fmt.Errorf("failed to record version %d: %w", version, err)
	}

	if err := Applied.Current().Put(ctx, client, version); err != nil {
		return 0, fmt.Errorf("failed to record the current version: %w", err)
	}

	return version, nil
}
