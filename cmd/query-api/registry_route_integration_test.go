//go:build integration

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The three drift outcomes, against a REAL Postgres table.
//
// The unit tests beside these cover the nil-pool and unreachable-registry
// paths, which need no database. What they cannot cover is the distinction
// that actually matters: an EMPTY table and a table whose rows are all at a
// superseded digest produce identical downstream behaviour (nothing
// reachable, everything served by Python) and mean opposite things. Getting
// that distinction wrong is the entire six-day September failure, so it is
// proven here against real rows rather than argued.

func seedRoutingRowAtDigest(t *testing.T, pool *pgxpool.Pool, schemaDigest, operation string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, mode)
		VALUES ($1, $2, $3, 'canary')
		ON CONFLICT (schema_digest, document_digest, selected_operation) DO NOTHING
	`, schemaDigest, "document-digest-for-"+operation, operation); err != nil {
		t.Fatal(err)
	}
}

func TestLogRoutingStateDrift_EmptyTableIsNotAnIncident(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	logged := captureLog(t, func() { logRoutingStateDrift(pool, "sha256:live") })

	if !strings.Contains(logged, "table is empty") {
		t.Fatalf("empty table was not reported as empty; got %q", logged)
	}
	if strings.Contains(logged, "ROUTING ROWS STALE") {
		t.Fatalf("empty table was reported as STALE -- 'nothing enabled' is the default posture, not an outage; got %q", logged)
	}
}

func TestLogRoutingStateDrift_RowsAtLiveDigestReportLive(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	seedRoutingRowAtDigest(t, pool, "sha256:live", "featureFlags")
	seedRoutingRowAtDigest(t, pool, "sha256:live", "reviewEdges")
	// One row at an older digest alongside them: real tables keep history
	// after a schema move, and their presence must not make a healthy
	// deployment look broken.
	seedRoutingRowAtDigest(t, pool, "sha256:superseded", "featureFlags")

	logged := captureLog(t, func() { logRoutingStateDrift(pool, "sha256:live") })

	if strings.Contains(logged, "ROUTING ROWS STALE") {
		t.Fatalf("rows exist at the live digest but the check reported STALE; got %q", logged)
	}
	if !strings.Contains(logged, "2 at live schema digest sha256:live") {
		t.Fatalf("live-row count not reported; got %q", logged)
	}
	if !strings.Contains(logged, "1 at other digests") {
		t.Fatalf("superseded rows not counted separately; got %q", logged)
	}
}

// The regression the whole change exists for. This test FAILS against the
// pre-change binary, which logged nothing at all in this state.
func TestLogRoutingStateDrift_RowsOnlyAtSupersededDigestReportStale(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	// The real shape of 2026-09-01: every row present, every row canary,
	// every row keyed to a digest the binary no longer computes.
	for i := 0; i < 12; i++ {
		seedRoutingRowAtDigest(t, pool, "sha256:67b87d38", fmt.Sprintf("operation-%d", i))
	}

	logged := captureLog(t, func() { logRoutingStateDrift(pool, "sha256:29d509cd") })

	if !strings.Contains(logged, "ROUTING ROWS STALE") {
		t.Fatalf("12 rows at a superseded digest were not reported as stale -- this is the exact state that went undetected for six days; got %q", logged)
	}
	if !strings.Contains(logged, "12 rows at sha256:67b87d38") {
		t.Fatalf("stale row count/digest not named; got %q", logged)
	}
	if !strings.Contains(logged, "0 at sha256:29d509cd") {
		t.Fatalf("live digest not named in the stale line; got %q", logged)
	}
	if strings.Contains(logged, "table is empty") {
		t.Fatalf("stale rows were reported as an empty table; got %q", logged)
	}
	// The line must tell an operator what to do, not merely that something
	// is wrong -- the failure mode here is a human not knowing this table
	// exists.
	if !strings.Contains(logged, "dev-hops go-api routing enable") {
		t.Fatalf("stale line does not name the recovery command; got %q", logged)
	}
}
