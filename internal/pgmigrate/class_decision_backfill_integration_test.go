//go:build integration

package pgmigrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// CHAOS-8735: revision 0146 creates go_api_class_decision and backfills the NEWEST go_api_routing_state row of
// each MCP class operation. On an equal timestamp the pick fails closed (a row not in a served mode beats a served
// one), then the greater schema digest. Rows of other operations are not copied, and the source rows are left in place.
func TestClassDecisionBackfillPicksTheNewestRowAndFailsClosedOnATie(t *testing.T) {
	ctx := context.Background()
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	uri := d.at(t, len(chain)-1)
	conn := connect(t, uri)

	older := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	rows := []struct {
		operation, schema, mode string
		at                      time.Time
	}{
		{"mcp:newest", "sha256:A", "canary", older},
		{"mcp:newest", "sha256:B", "python", newer},
		{"mcp:tie-served-vs-dark", "sha256:A", "canary", older},
		{"mcp:tie-served-vs-dark", "sha256:B", "disabled", older},
		{"mcp:tie-both-served", "sha256:A", "canary", older},
		{"mcp:tie-both-served", "sha256:B", "primary", older},
		{"mcp:only", "sha256:A", "canary", older},
		{"featureFlags", "sha256:A", "canary", newer},
	}
	for _, row := range rows {
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, 'doc', $2, 'build-'||$1) ON CONFLICT DO NOTHING`, row.schema, row.operation); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, updated_at)
			VALUES ($1, 'doc', $2, 'build-'||$1, 'go', $3, 100, $4)`, row.schema, row.operation, row.mode, row.at); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("upgrade to the head: %v", err)
	}

	type decision struct {
		mode, build, schema string
		decidedAt           time.Time
	}
	got := map[string]decision{}
	result, err := conn.Query(ctx, `SELECT operation, mode, current_candidate_build, schema_digest, decided_at FROM go_api_class_decision`)
	if err != nil {
		t.Fatal(err)
	}
	for result.Next() {
		var operation string
		var d decision
		if err := result.Scan(&operation, &d.mode, &d.build, &d.schema, &d.decidedAt); err != nil {
			t.Fatal(err)
		}
		got[operation] = d
	}
	result.Close()
	want := map[string]decision{
		"mcp:newest":             {"python", "build-sha256:B", "sha256:B", newer},
		"mcp:tie-served-vs-dark": {"disabled", "build-sha256:B", "sha256:B", older},
		"mcp:tie-both-served":    {"primary", "build-sha256:B", "sha256:B", older},
		"mcp:only":               {"canary", "build-sha256:A", "sha256:A", older},
	}
	if len(got) != len(want) {
		t.Fatalf("decisions %+v, want exactly the four class operations (featureFlags is not one)", got)
	}
	for operation, w := range want {
		g, ok := got[operation]
		if !ok || g.mode != w.mode || g.build != w.build || g.schema != w.schema || !g.decidedAt.Equal(w.decidedAt) {
			t.Errorf("%s: got %+v, want %+v", operation, g, w)
		}
	}
	var sources int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&sources); err != nil || sources != len(rows) {
		t.Errorf("source rows = %d (err %v), want all %d left in place", sources, err, len(rows))
	}
}
