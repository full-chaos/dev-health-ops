package graph

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The two coverage baseline fields (CHAOS-8111, CHAOS-8541) hand BOTH scope
// arguments to the read: a field that drops repoIds or teamIds answers for the
// whole org under the name of the selected scope.
func TestCoverageBaselineFields_PassTheScopeArgumentsToTheRead(t *testing.T) {
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	end := graphqldate.New(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	// The names of the team bindings belong to teamscope: read them from it.
	_, sample := teamscope.RepoCondition("org-1", "toString(repo_id)", []string{"team-a"}, time.Now().UTC())
	if len(sample) == 0 {
		t.Fatal("teamscope binds nothing for a team: nothing to measure")
	}
	carriesTeams := false
	for _, b := range sample {
		if ids, ok := b.Value.([]string); ok && reflect.DeepEqual(ids, []string{"team-a"}) {
			carriesTeams = true
		}
	}
	if !carriesTeams {
		t.Fatalf("no teamscope binding carries the team ids as []string: %+v", sample)
	}

	fields := map[string]func(r *Resolver, repoIds, teamIds []string){
		"coverageBaselines": func(r *Resolver, repoIds, teamIds []string) {
			_, _ = r.Query().CoverageBaselines(ctx, "org-1", end, repoIds, teamIds)
		},
		"coverageScopeBaseline": func(r *Resolver, repoIds, teamIds []string) {
			_, _ = r.Query().CoverageScopeBaseline(ctx, "org-1", end, repoIds, teamIds)
		},
	}
	for field, call := range fields {
		for label, tc := range map[string]struct{ repos, teams []string }{
			"no scope":     {nil, nil},
			"repositories": {[]string{"acme/alpha"}, nil},
			"teams":        {nil, []string{"team-a"}},
			"both":         {[]string{"acme/alpha"}, []string{"team-a"}},
		} {
			ch := &scopeRecordingClient{}
			call(&Resolver{ClickHouse: ch}, tc.repos, tc.teams)
			if len(ch.calls) != 1 {
				t.Fatalf("%s/%s: %d reads, want 1", field, label, len(ch.calls))
			}
			got, has := bindingValueByName(ch.calls[0], "repo_ids")
			if has != (tc.repos != nil) || (has && !reflect.DeepEqual(got, tc.repos)) {
				t.Errorf("%s/%s: repo_ids binding = %v (present %v), want %v", field, label, got, has, tc.repos)
			}
			for _, b := range sample {
				got, has := bindingValueByName(ch.calls[0], b.Name)
				if has != (tc.teams != nil) {
					t.Errorf("%s/%s: team binding %s present = %v, want %v", field, label, b.Name, has, tc.teams != nil)
					continue
				}
				if _, isIDs := b.Value.([]string); has && isIDs && !reflect.DeepEqual(got, tc.teams) {
					t.Errorf("%s/%s: team binding %s = %v, want %v", field, label, b.Name, got, tc.teams)
				}
			}
		}
	}
}
