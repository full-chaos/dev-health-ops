//go:build integration

package cognitiveload

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestPatternFallbackTeamsHoldOnlyActiveTeams: the team-id carry leaves a
// retired bare twin (same repo_patterns) beside the active keyed team. The
// pattern resolver must see the active team only; with no retired row the
// output is exactly what the pinned read returns.
func TestPatternFallbackTeamsHoldOnlyActiveTeams(t *testing.T) {
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
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgTwin, orgPlain = "org-9032-cl-twin", "org-9032-cl-plain"
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	insert := `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, is_active)
		VALUES (?, generateUUIDv4(), 'Platform', [], ['org/*'], ?, ?, ?)`
	for _, row := range []struct {
		org, id string
		at      time.Time
		active  uint8
	}{
		{orgTwin, "github:platform", base.Add(time.Hour), 1},
		{orgTwin, "platform", base, 1},
		{orgTwin, "platform", base.Add(time.Hour), 0},
		{orgPlain, "github:platform", base, 1},
	} {
		if err := conn.Exec(ctx, insert, row.id, row.at, row.org, row.active); err != nil {
			t.Fatalf("insert team %+v: %v", row, err)
		}
	}

	ids := func(org string) []string {
		teams, err := fetchAllTeamsForPatternFallback(ctx, client, org)
		if err != nil {
			t.Fatalf("fetchAllTeamsForPatternFallback(%s): %v", org, err)
		}
		var out []string
		for _, team := range teams {
			out = append(out, team.id)
		}
		return out
	}
	if got, want := ids(orgTwin), []string{"github:platform"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with a retired twin: ids = %v, want %v", got, want)
	}
	if got, want := ids(orgPlain), []string{"github:platform"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with no retired row: ids = %v, want %v", got, want)
	}
}
