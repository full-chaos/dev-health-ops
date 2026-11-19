//go:build integration

package chmigrate_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const storedVersionKeptGroupsVersion = "165_stored_version_kept_groups.sql"

var keptGroupObjects = []string{"git_pull_request_kept", "deployment_kept", "git_pull_request_kept_mv", "deployment_kept_mv"}

// CHAOS-6014 C9: versions written before 165 (merges stopped, so the loser stays a physical row) feed the kept tables
// through the backfill, and the migration's last statements repair a NULL lead that a newer version carried; a second
// upgrade writes nothing; the hand-run rollback drops the four objects and its version row, and up again restores them.
func TestStoredVersionKeptGroupsBackfillRepairAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	db, conn := startDropRetiredInvestmentClickHouse(t, ctx)
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	cut := -1
	for i, file := range chain {
		if file.Version == storedVersionKeptGroupsVersion {
			cut = i
		}
	}
	if cut < 0 {
		t.Fatalf("the chain holds no %s", storedVersionKeptGroupsVersion)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain[:cut]); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"git_pull_requests", "deployments"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatal(err)
		}
	}
	const repo = "c7198fbc-1945-3717-05d8-eb78866b4e79"
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, merged_at, first_review_at, reviews_count, last_synced) VALUES ('o1', ?, 1, ?, ?, ?, 2, ?)`,
			[]any{repo, base.Add(-2 * time.Hour), base, base.Add(-30 * time.Minute), base.Add(time.Hour)}},
		{`INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, last_synced) VALUES ('o1', ?, 1, ?, ?)`,
			[]any{repo, base.Add(-2 * time.Hour), base.Add(2 * time.Hour)}},
		{`INSERT INTO deployments (org_id, repo_id, deployment_id, merged_at, pull_request_number, pull_request_tier, last_synced) VALUES ('o1', ?, 'd1', ?, 7, 'sha', ?)`,
			[]any{repo, base, base.Add(time.Hour)}},
		{`INSERT INTO deployments (org_id, repo_id, deployment_id, last_synced) VALUES ('o1', ?, 'd1', ?)`,
			[]any{repo, base.Add(2 * time.Hour)}},
	} {
		if err := conn.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	present := func() []uint64 {
		counts := countRetiredInvestmentTables(t, ctx, conn, keptGroupObjects...)
		out := make([]uint64, 0, len(keptGroupObjects))
		for _, name := range keptGroupObjects {
			out = append(out, counts[name])
		}
		return out
	}
	if got := present(); !reflect.DeepEqual(got, []uint64{0, 0, 0, 0}) {
		t.Fatalf("before 165: objects %v, want none", got)
	}

	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := present(); !reflect.DeepEqual(got, []uint64{1, 1, 1, 1}) {
		t.Fatalf("after 165: objects %v, want all four", got)
	}
	rows := readKeptGroupRows(t, ctx, conn)
	want := keptGroupRows{
		pullRequest: "2026-07-23 12:00:00.000|2026-07-23 11:30:00.000|2|2026-07-23 14:00:00.001",
		deployment:  "2026-07-23 12:00:00.000|7|sha|2026-07-23 14:00:00.001",
		prRows:      3, deploymentRows: 3,
	}
	if rows != want {
		t.Fatalf("after 165: %+v\nwant %+v (the backfill read the pre-165 loser, and the repair filled the NULL leads)", rows, want)
	}

	// The migration can run again and writes nothing more.
	if err := conn.Exec(ctx, `ALTER TABLE schema_migrations DELETE WHERE version = ? SETTINGS mutations_sync = 2`, storedVersionKeptGroupsVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("second run of 165: %v", err)
	}
	if again := readKeptGroupRows(t, ctx, conn); again != want {
		t.Fatalf("second run of 165: %+v, want %+v", again, want)
	}

	body, err := os.ReadFile("rollback/165_stored_version_kept_groups.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(body)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	var versionRows uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM schema_migrations WHERE version = ?`, storedVersionKeptGroupsVersion).Scan(&versionRows); err != nil {
		t.Fatal(err)
	}
	if got := present(); !reflect.DeepEqual(got, []uint64{0, 0, 0, 0}) || versionRows != 0 {
		t.Fatalf("after the rollback: objects %v, version rows %d; want none", got, versionRows)
	}
	if after := readKeptGroupRows(t, ctx, conn); after.prRows != 3 || after.deploymentRows != 3 {
		t.Fatalf("the rollback changed the base rows: %+v", after)
	}

	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := present(); !reflect.DeepEqual(got, []uint64{1, 1, 1, 1}) {
		t.Fatalf("after up-down-up: objects %v, want all four", got)
	}
	if final := readKeptGroupRows(t, ctx, conn); final != want {
		t.Fatalf("after up-down-up: %+v, want %+v", final, want)
	}
}

type keptGroupRows struct {
	pullRequest, deployment string
	prRows, deploymentRows  uint64
}

func readKeptGroupRows(t *testing.T, ctx context.Context, conn driver.Conn) keptGroupRows {
	t.Helper()
	var rows keptGroupRows
	if err := conn.QueryRow(ctx, `SELECT concat(ifNull(toString(merged_at), 'NULL'), '|', ifNull(toString(first_review_at), 'NULL'), '|',
toString(reviews_count), '|', toString(last_synced)) FROM git_pull_requests FINAL WHERE org_id = 'o1'`).Scan(&rows.pullRequest); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT concat(ifNull(toString(merged_at), 'NULL'), '|', ifNull(toString(pull_request_number), 'NULL'), '|',
pull_request_tier, '|', toString(last_synced)) FROM deployments FINAL WHERE org_id = 'o1'`).Scan(&rows.deployment); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT (SELECT count() FROM git_pull_requests WHERE org_id = 'o1'), (SELECT count() FROM deployments WHERE org_id = 'o1')`).Scan(
		&rows.prRows, &rows.deploymentRows); err != nil {
		t.Fatal(err)
	}
	return rows
}
