//go:build integration

package icfinalize

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The rolling read gives each (owner, person) one stat from the person's rows in
// the repositories that owner owns: a repository with two owners gives the
// person a stat in each, a repository no team owns gives "unassigned", and a
// blank or "unknown" identity is no person and keeps a blank team wherever it
// worked. The team id stored on a row is never read.
func TestTheRollingReadGroupsByTheOwnerOfTheRepository(t *testing.T) {
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

	const org = "00000000-0000-4000-8000-0000000093f1"
	asOf := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	shared := uuid.MustParse("00000000-0000-4000-8000-0000000093f2")
	onlyA := uuid.MustParse("00000000-0000-4000-8000-0000000093f3")
	unowned := uuid.MustParse("00000000-0000-4000-8000-0000000093f4")
	store := func(repo uuid.UUID, person string, loc uint32) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
    (repo_id, day, author_email, identity_id, team_id, team_name, loc_touched, delivery_units, computed_at, org_id)
    VALUES (?, ?, ?, ?, 'stored-decoy', 'Stored Decoy', ?, 1, ?, ?)`, repo, asOf, person, person, loc, asOf.Add(time.Hour), org); err != nil {
			t.Fatal(err)
		}
	}
	store(shared, "dev@example.com", 10)
	store(onlyA, "dev@example.com", 5)
	store(unowned, "dev@example.com", 7)
	store(shared, "", 3)
	store(unowned, "unknown", 2)

	owners := map[string][]string{shared.String(): {"team-a", "team-b"}, onlyA.String(): {"team-a"}}
	stats, err := LoadRollingStats(ctx, conn, org, asOf, owners)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, stat := range stats {
		got[stat.IdentityID+" in "+stat.TeamID] += stat.ChurnLOC30d
	}
	want := map[string]float64{
		"dev@example.com in team-a":     15,
		"dev@example.com in team-b":     10,
		"dev@example.com in unassigned": 7,
		" in ":                          3,
		"unknown in ":                   2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("stats (identity in team: churn) = %v, want %v", got, want)
	}
}
