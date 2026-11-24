package compoundingrisk

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14515: a repoIds list over maxRows entries is not cut with no notice:
// Resolve refuses it with the BAD_USER_INPUT error that names the limit and
// the count sent, before any statement. big-500 is entry 501.
func TestRepoIdsOver500IsRefusedWithBadUserInput(t *testing.T) {
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
			rec := &sweepRecorder{sweepStore: &sweepStore{world: bigTeamWorld(now, false)}}
			got, err := Resolve(context.Background(), rec, sweepOrg, &model.CompoundingRiskFilterInput{
				Breakout: c.breakout, RepoIds: tokens, TrendDays: 30,
			}, now)
			wantRepoIdsRefused(t, got, err, len(tokens))
			if len(rec.statements) != 0 {
				t.Errorf("%d statements ran; want 0 (refused before any read)", len(rec.statements))
			}
		})
	}
}
