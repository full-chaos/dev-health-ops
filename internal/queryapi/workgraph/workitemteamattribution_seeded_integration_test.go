//go:build integration

// Executed-SQL proof against a REAL ClickHouse container, schema applied
// through the canonical migration chain (chschema.Apply): the FINAL +
// latest-computed_at snapshot mechanism this query shares with
// ResolveWorkUnitTeamAttributions' own inner join (already proven
// elsewhere) is proven here for THIS query's own shape -- no membership
// join, direct per-work-item read.
package workgraph

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestResolveWorkItemTeamAttributions_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "wita-seeded-it"
	const repoID = "11111111-1111-1111-1111-111111111111"
	const tOld = "2026-04-08 03:00:00.000"
	const tNew = "2026-04-08 04:00:00.000"

	exec := func(stmt string) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	// Same (org_id, repo_id, work_item_id, team_id, source) key, two
	// computed_at versions -- proves the snapshot IN (SELECT ... max
	// (computed_at) ...) picks the latest compute, not an older row FINAL
	// alone would still leave (FINAL only collapses exact-key duplicates
	// within ONE compute; a re-org appends a NEW row with a different
	// team_id/source and both keys survive FINAL, needing the snapshot
	// to pick only the latest computed_at's rows -- see this package's
	// own resolveWorkItemTeamAttributions doc comment).
	exec(`INSERT INTO work_item_team_attributions
		(org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
		('` + orgID + `', '` + repoID + `', 'wi-1', 'linear', 'team-old', 'Team Old', 'assignee_membership', 1, 'low', 'stale', toDateTime64('` + tOld + `',3))`)
	exec(`INSERT INTO work_item_team_attributions
		(org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
		('` + orgID + `', '` + repoID + `', 'wi-1', 'linear', 'team-new', 'Team New', 'native_team', 1, 'high', 'fresh', toDateTime64('` + tNew + `',3))`)

	// A different work item in the same org must not leak in when the
	// caller asks for wi-1 specifically.
	exec(`INSERT INTO work_item_team_attributions
		(org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
		('` + orgID + `', '` + repoID + `', 'wi-2', 'linear', 'team-new', 'Team New', 'native_team', 1, 'high', 'other item', toDateTime64('` + tNew + `',3))`)

	got, err := ResolveWorkItemTeamAttributions(ctx, client, orgID, []string{"wi-1"}, nil)
	if err != nil {
		t.Fatalf("ResolveWorkItemTeamAttributions: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1: %+v", len(got), got)
	}
	row := got[0]
	if row.WorkItemID != "wi-1" {
		t.Errorf("WorkItemID = %q, want wi-1", row.WorkItemID)
	}
	if row.TeamID == nil || *row.TeamID != "team-new" {
		t.Errorf("TeamID = %v, want team-new (the fresh row, not the stale one)", row.TeamID)
	}
	if row.Evidence != "fresh" {
		t.Errorf("Evidence = %q, want fresh", row.Evidence)
	}
}
