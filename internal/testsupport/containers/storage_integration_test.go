//go:build integration

package containers_test

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestPinnedStorageDependencies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	postgresInstance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, postgresInstance)
	postgresPool, err := postgresstore.Open(ctx, postgresstore.DefaultConfig(postgresInstance.URI))
	if err != nil {
		t.Fatalf("open PostgreSQL test dependency: %v", err)
	}
	postgresPool.Close()

	clickHouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, clickHouseInstance)
	clickHouseConnection, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickHouseInstance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse test dependency: %v", err)
	}
	_ = clickHouseConnection.Close()

	valkeyInstance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, valkeyInstance)
	valkeyClient, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(valkeyInstance.URI))
	if err != nil {
		t.Fatalf("open Valkey test dependency: %v", err)
	}
	valkeyClient.Close()
}

func closeInstance(t *testing.T, instance *containers.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := instance.Close(ctx); err != nil {
		t.Errorf("terminate test dependency: %v", err)
	}
}
