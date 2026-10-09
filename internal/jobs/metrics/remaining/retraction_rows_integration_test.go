//go:build integration

package remaining

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestJobTargetDiscoveryGivesRetractionRowsNoWeight reads the teams the
// capacity forecast job and the recommendations job would work on.
//
// Two organizations hold the same measurements; one of them also holds the
// old rows of the retired team ids and the retraction row over each (package
// retractionseed). Both give the same targets.
//
// A third organization holds retraction rows only: every key of its window
// was measured once and then retracted. It has no team to forecast or to
// evaluate. Before the rule, each retired id was a target there: the job
// wrote a forecast with a backlog of 0 and recommendation rows for it.
func TestJobTargetDiscoveryGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	capacity := &CapacityExecutor{conn: store.Conn}
	recommendations := &RecommendationsExecutor{conn: store.Conn}

	read := func(org string) ([]string, []string) {
		t.Helper()
		targets, err := capacity.resolveScopes(ctx, org, capacityScope{AllTeams: true})
		if err != nil {
			t.Fatalf("%s capacity scopes: %v", org, err)
		}
		var scopes []string
		for _, target := range targets {
			scopes = append(scopes, *target.TeamID+" @ "+*target.WorkScopeID)
		}
		sort.Strings(scopes)
		teams, err := recommendations.DiscoverTeamIDs(ctx, org)
		if err != nil {
			t.Fatalf("%s recommendation teams: %v", org, err)
		}
		sort.Strings(teams)
		return scopes, teams
	}

	controlScopes, controlTeams := read(retractionseed.ControlOrg)

	// The control targets, from the seed: the four keyed ids, and the four
	// retired ids, which still hold the measured rows of the one day that was
	// not computed again. Those rows are measurements, so they stay targets.
	wantTeams := []string{"ENG", "core", "github:platform", "gitlab:ops", "jira:ENG", "linear:core", "ops", "platform"}
	if !reflect.DeepEqual(controlTeams, wantTeams) {
		t.Fatalf("control recommendation teams = %v, want %v", controlTeams, wantTeams)
	}
	wantScopes := []string{
		"ENG @ ENGPROJ", "core @ CORE", "github:platform @ acme/platform", "gitlab:ops @ acme/ops",
		"jira:ENG @ ENGPROJ", "linear:core @ CORE", "ops @ acme/ops", "platform @ acme/platform",
	}
	if !reflect.DeepEqual(controlScopes, wantScopes) {
		t.Fatalf("control capacity scopes = %v, want %v", controlScopes, wantScopes)
	}

	retractedScopes, retractedTeams := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(controlScopes, retractedScopes) || !reflect.DeepEqual(controlTeams, retractedTeams) {
		t.Fatalf("the retraction rows changed the targets:\n control   %v %v\n retracted %v %v",
			controlScopes, controlTeams, retractedScopes, retractedTeams)
	}

	const org = "org-retraction-rows-only"
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, org, day, team, store.OldComputedAt, store.NewComputedAt)
	}
	scopes, teams := read(org)
	if len(scopes) != 0 || len(teams) != 0 {
		t.Fatalf("an organization of retraction rows only has targets: capacity %v, recommendations %v", scopes, teams)
	}
}
