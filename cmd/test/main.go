package main

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

//go:generate go run github.com/NHAS/tetcd/cmd/tetcd-gen -type=github.com/NHAS/tetcd/cmd/test/config.Config -out=config_etcd.go -prefix=wagtest

func main() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create etcd client: %v", err)
	}

	err = Config.Labels().Key("new-label").Put(context.Background(), client, "label-contents")
	if err != nil {
		log.Fatalf("failed to put label: %v", err)
	}
}
