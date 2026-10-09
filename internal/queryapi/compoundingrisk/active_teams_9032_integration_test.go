//go:build integration

package compoundingrisk

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
	for _, row := range []struct {
		id     string
		at     time.Time
		active uint8
	}{
		{"github:platform", base.Add(time.Hour), 1},
		{"platform", base, 1}, // the bare row the carry retires below
		{"platform", base.Add(time.Hour), 0},
	} {
		if err := conn.Exec(ctx, insert, row.id, "Platform", row.at, org, row.active); err != nil {
			t.Fatalf("insert team %+v: %v", row, err)
		}
	}
	labels, order := teamLabels(ctx, client, org)
	if want := []string{"github:platform"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("candidate order = %v, want %v", order, want)
	}
	if labels["platform"] != "Platform" || labels["github:platform"] != "Platform" {
		t.Fatalf("labels = %v, want both ids labelled", labels)
	}
}
