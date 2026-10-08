//go:build integration

package filteroptions

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

// TestTeamNamesServeTheNewestNameOfEachActiveNamedTeam is the real-ClickHouse
// proof of CHAOS-8748: a renamed team serves its newest name, a blank name, an
// inactive team and another tenant's team are absent, and the id forms of all
// four providers pass through unchanged.
func TestTeamNamesServeTheNewestNameOfEachActiveNamedTeam(t *testing.T) {
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
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-8748-team-names"
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	insert := `INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, is_active)
		VALUES (?, generateUUIDv4(), ?, [], ?, ?, ?)`
	for _, row := range []struct {
		id, name, org string
		at            time.Time
		active        uint8
	}{
		{"ENG", "Engineering", org, base, 1},
		{"ENG", "Engineering Platform", org, base.Add(time.Hour), 1}, // a rename: the newest name wins
		{"gh:platform", "Platform", org, base, 1},
		{"gl:group/api", "  API  ", org, base, 1},
		{"jira-blank", "   ", org, base, 1},
		{"retired", "Retired", org, base, 0},
		{"other-tenant", "Other", "org-8748-other", base, 1},
	} {
		if err := conn.Exec(ctx, insert, row.id, row.name, row.at, row.org, row.active); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	got, err := namedTeams(ctx, client, []dhclickhouse.Binding{{Name: "org_id", Value: org}})
	if err != nil {
		t.Fatalf("namedTeams: %v", err)
	}
	want := map[string]string{"ENG": "Engineering Platform", "gh:platform": "Platform", "gl:group/api": "API"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("team names = %v, want %v", got, want)
	}
}
