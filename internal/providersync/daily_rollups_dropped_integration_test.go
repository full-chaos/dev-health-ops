//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestDailyRollupsAreDroppedByMigrations guards against a silent re-add of
// commit_daily_rollup, ci_daily_rollup, deployment_daily_rollup, or their
// materialized views. Those views fired once per INSERT block on top of
// ReplacingMergeTree sources, so a re-synced commit/run/deployment (which
// legitimately arrives as more than one physical INSERT for the same
// logical key) was double-counted. No Go or Python code reads any of the
// three, so the migration chain drops them outright instead of reworking
// the views. If a future migration reintroduces one of these names, this
// test fails -- making the reintroduction a deliberate, reviewed decision
// rather than an accident.
func TestDailyRollupsAreDroppedByMigrations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	}()

	// Apply the REAL migration chain -- the same one that drops these
	// objects -- rather than asserting against hand-typed DDL that would
	// only prove what the test itself declared.
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	dropped := []string{
		"commit_daily_rollup",
		"commit_daily_rollup_mv",
		"ci_daily_rollup",
		"ci_daily_rollup_mv",
		"deployment_daily_rollup",
		"deployment_daily_rollup_mv",
	}
	for _, name := range dropped {
		var count uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = ?",
			name,
		).Scan(&count); err != nil {
			t.Fatalf("querying system.tables for %s: %v", name, err)
		}
		if count != 0 {
			t.Errorf("%s still exists after migrations ran; it was dropped for its "+
				"per-INSERT-block double count on ReplacingMergeTree sources -- "+
				"reintroducing it must be a deliberate, reviewed decision", name)
		}
	}
}
