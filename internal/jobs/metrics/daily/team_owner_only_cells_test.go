package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// D7013b cells: an unowned repository whose name a team's repo_patterns would
// match ("acme/patterned" under "acme/*") gets no team in each family. The
// owned repository keeps its owner.
var (
	ownerOnlyOwnedRepo     = uuid.MustParse("00000000-0000-4000-8000-0000000007b1")
	ownerOnlyPatternedRepo = uuid.MustParse("00000000-0000-4000-8000-0000000007b2")
	ownerOnlyDay           = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
)

func ownerOnlyCellConn() *governanceQueryRecorder {
	return &governanceQueryRecorder{rows: &fakeOwnershipRows{rows: []fakeOwnershipRow{
		{repoID: ownerOnlyOwnedRepo.String(), teamID: "github:owner", mainOwner: 1},
	}}}
}

func ownerOnlyCellNames() map[string]string {
	return map[string]string{
		ownerOnlyOwnedRepo.String():     "acme/owned",
		ownerOnlyPatternedRepo.String(): "acme/patterned",
	}
}

func assertOwnerOnly(t *testing.T, family string, got map[string][]teamownership.OwnerShare, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", family, err)
	}
	if owners := got[ownerOnlyOwnedRepo.String()]; len(owners) != 1 || owners[0].TeamID != "github:owner" {
		t.Fatalf("%s: owned repository owners = %v, want github:owner", family, owners)
	}
	if owners, ok := got[ownerOnlyPatternedRepo.String()]; ok {
		t.Fatalf("%s: unowned repository got a team %v, want none (no repo_patterns fallback)", family, owners)
	}
}

func TestTeamCognitiveLoadUnownedPatternRepoGetsNoTeam(t *testing.T) {
	got, err := resolveDailyFinalizeRepoOwnerShares(context.Background(), ownerOnlyCellConn(), "acme", ownerOnlyDay,
		[]uuid.UUID{ownerOnlyOwnedRepo, ownerOnlyPatternedRepo}, ownerOnlyCellNames())
	assertOwnerOnly(t, "team_cognitive_load", got, err)
}

// team_complexity resolves through the same function as team_cognitive_load
// (team_complexity_native_executor.go); this cell pins that call shape.
func TestTeamComplexityUnownedPatternRepoGetsNoTeam(t *testing.T) {
	got, err := resolveDailyFinalizeRepoOwnerShares(context.Background(), ownerOnlyCellConn(), "acme", ownerOnlyDay,
		[]uuid.UUID{ownerOnlyPatternedRepo, ownerOnlyOwnedRepo}, ownerOnlyCellNames())
	assertOwnerOnly(t, "team_complexity", got, err)
}

func TestCompoundingRiskTeamUnownedPatternRepoGetsNoTeam(t *testing.T) {
	got, err := resolveCompoundingRepoOwners(context.Background(), ownerOnlyCellConn(), "acme", ownerOnlyDay,
		[]uuid.UUID{ownerOnlyOwnedRepo, ownerOnlyPatternedRepo}, ownerOnlyCellNames())
	assertOwnerOnly(t, "compounding_risk_team", got, err)
}
