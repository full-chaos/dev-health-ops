//go:build integration

package chmigrate_test

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// INVARIANT (CHAOS-7265): every row of project_membership_transitions and work_items carries
// ingested_at = the server's time at insert (never a provider or client time), last_synced (client
// stamp, RMT version) is untouched, and project_membership_presence.last_synced is that ingested_at on
// BOTH arms. (The legacy-row cells and the rerun of migration 100 are archived with it, CHAOS-11751.)
func TestIngestedAtIsTheServerStampAndTheViewExposesIt(t *testing.T) {
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
	chschema.Apply(ctx, t, instance) // the head, migration 100 included
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	readInt := func(query string) int64 {
		t.Helper()
		var out int64
		if err := conn.QueryRow(ctx, "SELECT toInt64(("+query+"))").Scan(&out); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	const zero = `00000000-0000-0000-0000-000000000000`
	for _, subject := range []string{"1", "2"} {
		exec(`INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, from_project_key, to_project_key, actor, occurred_at, last_synced, event_id) VALUES ('o', '` + zero + `', 'pull_request', '` + subject + `', 'github', '', 'P` + subject + `', '', 'K` + subject + `', '', '2020-01-01 00:00:00', '2020-01-02 00:00:00', 'e` + subject + `')`)
		exec(`INSERT INTO work_items (org_id, repo_id, work_item_id, provider, project_id, project_key, updated_at, last_synced) VALUES ('o', '` + zero + `', 'W` + subject + `', 'jira', 'J` + subject + `', 'JK', '2020-01-01 00:00:00', '2020-01-02 00:00:00')`)
	}
	for _, table := range []string{"project_membership_transitions", "work_items"} {
		if n := readInt(`SELECT countIf(ingested_at = last_synced) FROM ` + table); n != 0 {
			t.Fatalf("%s: %d row(s) took the client last_synced as ingested_at", table, n)
		}
		if n := readInt(`SELECT count() FROM ` + table + ` WHERE toDate(last_synced) != '2020-01-02'`); n != 0 {
			t.Fatalf("%s: the RMT version last_synced was not kept as written", table)
		}
	}

	// A writer that names its columns and omits ingested_at is stamped by the server, on both tables.
	if n := readInt(`SELECT countIf(ingested_at > now64(3) - INTERVAL 1 MINUTE) FROM project_membership_transitions WHERE subject_id = '2'`); n != 1 {
		t.Fatalf("a transitions writer that omits ingested_at got %d server-stamped rows, want 1", n)
	}
	if n := readInt(`SELECT countIf(ingested_at > now64(3) - INTERVAL 1 MINUTE) FROM work_items WHERE work_item_id = 'W2'`); n != 1 {
		t.Fatalf("a work_items writer that omits ingested_at got %d server-stamped rows, want 1", n)
	}

	// The view: last_synced is ingested_at on each arm, never the client last_synced, and observed_at
	// keeps its event-time meaning.
	if n := readInt(`SELECT count() FROM project_membership_presence`); n != 4 {
		t.Fatalf("view rows = %d, want 4 (two transition memberships, two column-arm work items)", n)
	}
	viewStamp := func(source, subject string) int64 {
		return readInt(`SELECT toUnixTimestamp64Milli(last_synced) FROM project_membership_presence WHERE source = '` + source + `' AND subject_id = '` + subject + `'`)
	}
	if got, want := viewStamp("transition", "1"), readInt(`SELECT toUnixTimestamp64Milli(max(ingested_at)) FROM project_membership_transitions FINAL WHERE subject_id = '1'`); got != want {
		t.Fatalf("transition arm: view.last_synced = %d, want max(ingested_at) %d", got, want)
	}
	if got, want := viewStamp("work_item_column", "W1"), readInt(`SELECT toUnixTimestamp64Milli(ingested_at) FROM work_items WHERE work_item_id = 'W1'`); got != want {
		t.Fatalf("work_item_column arm: view.last_synced = %d, want ingested_at %d", got, want)
	}
	if n := readInt(`SELECT countIf(last_synced > now64(3) - INTERVAL 1 HOUR AND toDate(observed_at) = '2020-01-01') FROM project_membership_presence`); n != 4 {
		t.Fatalf("view rows with last_synced = ingest time and observed_at = event time: %d, want 4", n)
	}
	if n := readInt(`SELECT countIf(toDate(last_synced) = '2020-01-02') FROM project_membership_presence`); n != 0 {
		t.Fatalf("%d view row(s) expose the client-stamped last_synced", n)
	}
}
