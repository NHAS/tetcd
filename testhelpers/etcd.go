package testhelpers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func SetupEtcdContainer(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/coreos/etcd:v3.6.7",
		ExposedPorts: []string{"2379/tcp"},
		Env: map[string]string{
			"ETCD_LISTEN_CLIENT_URLS":    "http://0.0.0.0:2379",
			"ETCD_ADVERTISE_CLIENT_URLS": "http://0.0.0.0:2379",
		},
		WaitingFor: wait.ForLog("ready to serve client requests").
			WithStartupTimeout(10 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start etcd container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "2379")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://%s:%s", host, port.Port())},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}

	return client, func() {
		client.Close()
		container.Terminate(ctx)
	}
}
