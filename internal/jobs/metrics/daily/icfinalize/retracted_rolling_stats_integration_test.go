//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A key whose newest row is a retraction row (CHAOS-9135) is no sample of the
// 30-day window: a person with only such keys has no stat, and the row of
// zeros of one key does not pull the median of another key's cycle time down.
func TestTheRollingReadLeavesOutRetractedKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "00000000-0000-4000-8000-0000000093e1"
	asOf := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	older, newer := asOf.Add(-48*time.Hour), asOf.Add(-47*time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000093e2")
	store := func(person string, day time.Time, cycle float64, loc uint32, marker uint8, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, loc_touched, delivery_units, cycle_p50_hours, computed_at, org_id, retracted)
    VALUES (?, ?, ?, ?, 'unassigned', 'Unassigned', ?, 1, ?, ?, ?, ?)`, repo, day, person, person, loc, cycle, at, org, marker); err != nil {
			t.Fatal(err)
		}
	}
	d1, d2, d3 := asOf.AddDate(0, 0, -1), asOf.AddDate(0, 0, -2), asOf.AddDate(0, 0, -3)
	store("gone@example.com", d1, 12, 7, 0, older)
	store("gone@example.com", d1, 0, 0, 1, newer)
	store("mid@example.com", d1, 10, 5, 0, older)
	store("mid@example.com", d2, 30, 5, 0, older)
	store("mid@example.com", d3, 20, 5, 0, older)
	store("mid@example.com", d3, 0, 0, 1, newer)

	stats, err := LoadRollingStats(ctx, conn, org, asOf, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]RollingStat{}
	for _, stat := range stats {
		got[stat.IdentityID] = stat
	}
	if _, listed := got["gone@example.com"]; listed || len(got) != 1 {
		t.Errorf("stats = %v, want only mid@: gone@ holds only a retracted key", got)
	}
	if mid := got["mid@example.com"]; mid.ChurnLOC30d != 10 || mid.CycleP5030dHrs != 20 {
		t.Errorf("mid@ = %+v, want churn 10 and median cycle 20 over its two live keys", mid)
	}
}
