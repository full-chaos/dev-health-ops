package compoundingrisk

import (
	"context"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// servedCutField reports a CompoundingRiskResult field outside the fields of
// today that holds a value: a served field that names the cut of repoIds.
func servedCutField(got *model.CompoundingRiskResult) (string, bool) {
	known := map[string]bool{"OrgID": true, "Breakout": true, "Rows": true, "Trend": true, "GeneratedAt": true}
	v := reflect.ValueOf(*got)
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.IsExported() && !known[f.Name] && !v.Field(i).IsZero() {
			return f.Name, true
		}
	}
	return "", false
}

// CHAOS-14515: a repoIds list over maxRows entries must not be cut with no
// notice. Resolve refuses it with an error, or serves a field that names the
// cut. big-500 is entry 501 and scores 1; the first 500 score 0.25.
func TestRepoIdsOver500IsRefusedOrDisclosed(t *testing.T) {
	now := sweepNow()
	tokens := make([]string, 0, maxRows+1)
	for i := 0; i <= maxRows; i++ {
		tokens = append(tokens, bigRepoID(i))
	}
	for _, c := range []struct {
		name     string
		breakout model.CompoundingRiskScope
	}{
		{"REPO", model.CompoundingRiskScopeRepo},
		{"TEAM-no-team-named", model.CompoundingRiskScopeTeam},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := &sweepStore{world: bigTeamWorld(now, false)}
			got, err := Resolve(context.Background(), store, sweepOrg, &model.CompoundingRiskFilterInput{
				Breakout: c.breakout, RepoIds: tokens, TrendDays: 30,
			}, now)
			for _, st := range store.unknown {
				t.Fatalf("a statement the model does not know:\n%s", st)
			}
			if err != nil {
				return
			}
			if _, ok := servedCutField(got); ok {
				return
			}
			last := false
			for _, r := range got.Rows {
				if r.ScopeID == bigRepoID(maxRows) {
					last = true
				}
			}
			t.Fatalf("%d repoIds: no error and no served field names the cut; rows=%d trend=%d, entry %d (%s) in rows=%v; want an error or a served cut field",
				len(tokens), len(got.Rows), len(got.Trend), maxRows+1, bigRepoID(maxRows), last)
		})
	}
}
