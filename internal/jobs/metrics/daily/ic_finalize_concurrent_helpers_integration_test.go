//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func dayLockerForTest(t *testing.T, ctx context.Context) func(context.Context, string, time.Time) (func(), error) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return store.LockICFinalizeDay
}

func setDayLockerForTest(executor *ICFinalizeExecutor, locker func(context.Context, string, time.Time) (func(), error)) {
	executor.SetDayLocker(locker)
}
