//go:build integration

package people

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The collaboration read takes the newest row of each key with argMax and
// leaves a key out when that row is a retraction (CHAOS-9135).
func TestTheCollaborationReadLeavesOutAKeyWhoseNewestRowIsARetraction(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	for name, want := range map[string]float64{"alice": 10, "bob": 0, "carol": 10} {
		rows, err := fetchPersonCollaboration(ctx, client, []string{name + "@example.com"}, day, day.AddDate(0, 0, 1), f.OrgA)
		if err != nil {
			t.Fatal(err)
		}
		var given, completed float64
		for _, row := range rows {
			switch row.Label {
			case "Reviews given":
				given = row.Value
			case "Items completed":
				completed = row.Value
			}
		}
		if given != want || completed != want {
			t.Errorf("%s: reviews given %v, items completed %v, want %v for both: a retracted key adds nothing", name, given, completed, want)
		}
	}
}
