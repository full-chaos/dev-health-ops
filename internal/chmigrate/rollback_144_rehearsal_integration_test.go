//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The live roll's hand-only rollback, rollback/144_llm_attempts_endpoint.down.sql, rehearsed up -> down -> up
// (acp ruling on CHAOS-11751: kept until a later migration replaces it as the live rollback). The head holds
// 144; the down file drops both columns and the version row, after which this build reads the database as below
// the head; the pre-squash build (the archived 098 baseline and chain, which an image rollback runs) applies 144
// again from its archived file, and this build then reads the head again.
func TestRollback144UpDownUpThroughAPreSquashBuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	head, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	const version = "144_llm_attempts_endpoint.sql"
	columns := func() []string {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'llm_categorization_attempts' AND name IN ('endpoint_type', 'gateway_id') ORDER BY name")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			out = append(out, name)
		}
		return out
	}
	versionRows := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM schema_migrations WHERE version = ?", version).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	both := []string{"endpoint_type", "gateway_id"}
	if got := columns(); !reflect.DeepEqual(got, both) || versionRows() == 0 {
		t.Fatalf("at the head: columns %v, version rows %d; want %v and the version", got, versionRows(), both)
	}

	body, err := os.ReadFile("rollback/144_llm_attempts_endpoint.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if got := columns(); len(got) != 0 || versionRows() != 0 {
		t.Fatalf("after the rollback: columns %v, version rows %d; want neither", got, versionRows())
	}
	status, err := chmigrate.ReadStatus(ctx, db, head, live)
	if err != nil || status.State != "below_head" || !reflect.DeepEqual(status.Missing, []string{version}) {
		t.Fatalf("this build after the rollback: %+v, %v; want below_head missing [%s] (only an image rollback runs the down file)", status, err, version)
	}

	result, err := chmigrate.Upgrade(ctx, db, loadArchivedBaseline(t), loadArchivedChain(t))
	if err != nil || result.Action != "chain_applied" || !reflect.DeepEqual(result.Applied, []string{version}) {
		t.Fatalf("the pre-squash build's upgrade after the rollback = %+v, %v; want chain_applied [%s]", result, err, version)
	}
	if got := columns(); !reflect.DeepEqual(got, both) || versionRows() == 0 {
		t.Fatalf("the upgrade after the rollback: columns %v, version rows %d; want %v and the version", got, versionRows(), both)
	}
	status, err = chmigrate.ReadStatus(ctx, db, head, live)
	if err != nil || status.State != "at_head" || len(status.Pending) != 0 {
		t.Fatalf("this build after the pre-squash upgrade: %+v, %v; want at_head with nothing pending", status, err)
	}
}
