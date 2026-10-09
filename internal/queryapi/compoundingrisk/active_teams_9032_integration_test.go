//go:build integration

package compoundingrisk

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestTeamLabelsListsOnlyActiveTeamsButLabelsRetiredIDs(t *testing.T) {
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

	const org = "org-9032-active-teams"
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	insert := `INSERT INTO teams (id, team_uuid, name, members, repo_patterns, updated_at, org_id, is_active)
		VALUES (?, generateUUIDv4(), ?, [], ['org/*'], ?, ?, ?)`
	const otherOrg = "org-9032-other-tenant"
	for _, row := range []struct {
		id, name, org string
		at            time.Time
		active        uint8
	}{
		{"github:platform", "Platform", org, base.Add(time.Hour), 1},
		{"platform", "Platform", org, base, 1}, // the bare row the carry retires below
		{"platform", "Platform", org, base.Add(time.Hour), 0},
		// A second provider id shape: the rule must not ride on `github:`.
		{"linear:growth", "Growth", org, base.Add(time.Hour), 1},
		{"growth", "Growth", org, base, 1},
		{"growth", "Growth", org, base.Add(time.Hour), 0},
		{"jira:payments", "Payments", org, base.Add(time.Hour), 1},
		// Another tenant holds the SAME bare ids as ACTIVE rows, and its own team.
		{"platform", "Other Platform", otherOrg, base, 1},
		{"growth", "Other Growth", otherOrg, base, 1},
		{"other:team", "Other Team", otherOrg, base, 1},
		{"github:platform", "Other GitHub Platform", otherOrg, base.Add(2 * time.Hour), 1},
	} {
		if err := conn.Exec(ctx, insert, row.id, row.name, row.at, row.org, row.active); err != nil {
			t.Fatalf("insert team %+v: %v", row, err)
		}
	}
	labels, order := teamLabels(ctx, client, org)
	if want := []string{"github:platform", "jira:payments", "linear:growth"}; !reflect.DeepEqual(sortedCopy(order), want) {
		t.Fatalf("candidate order = %v, want %v", order, want)
	}
	wantLabels := map[string]string{
		"github:platform": "Platform", "platform": "Platform",
		"linear:growth": "Growth", "growth": "Growth", "jira:payments": "Payments",
	}
	if !reflect.DeepEqual(labels, wantLabels) {
		t.Fatalf("labels = %v, want %v", labels, wantLabels)
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
