package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// Each owning team's claim starts at the earliest timed evidence of THAT team
// (another team's earlier evidence is not its start); a claim with no timed
// evidence has no evidence start.
func TestDerivedOwnershipStartsAtTheWinnersEarliestEvidence(t *testing.T) {
	early := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	mid := early.Add(48 * time.Hour)
	links := []TeamRepoOwnershipProjectLink{
		{Provider: "github", ProjectID: "p-win", TeamID: "team-win", IsPrimary: true, Specificity: 100},
		{Provider: "github", ProjectID: "p-lose", TeamID: "team-lose", IsPrimary: true, Specificity: 100},
		{Provider: "github", ProjectID: "p-untimed", TeamID: "team-untimed", IsPrimary: true, Specificity: 100},
	}
	items := []TeamRepoOwnershipWorkItem{
		{WorkItemID: "w1", Provider: "github", Type: "pr", RepoID: "repo-a", ProjectID: "p-win", CreatedAt: mid},
		{WorkItemID: "w2", Provider: "github", Type: "pr", RepoID: "repo-a", ProjectID: "p-win", CreatedAt: mid.Add(time.Hour)},
		{WorkItemID: "l1", Provider: "github", Type: "pr", RepoID: "repo-a", ProjectID: "p-lose", CreatedAt: early},
		{WorkItemID: "u1", Provider: "github", Type: "pr", RepoID: "repo-b", ProjectID: "p-untimed"},
	}
	derivation := deriveTeamRepoOwnership("org", links, items, nil, nil, nil)
	got := map[[2]string]DerivedTeamRepoOwnershipRow{}
	for _, row := range derivation.Rows {
		got[[2]string{row.RepoID, row.TeamID}] = row
	}
	if row := got[[2]string{"repo-a", "team-win"}]; row.IsPrimary != teamRepoOwnershipMainOwner || !row.EvidenceFrom.Equal(mid) {
		t.Errorf("repo-a main = %+v, want team-win from %v (its own earliest, not the co-owner's %v)", row, mid, early)
	}
	if row := got[[2]string{"repo-a", "team-lose"}]; row.IsPrimary != teamRepoOwnershipCoOwner || !row.EvidenceFrom.Equal(early) {
		t.Errorf("repo-a co-owner = %+v, want team-lose from its own %v", row, early)
	}
	if row := got[[2]string{"repo-b", "team-untimed"}]; row.IsPrimary != teamRepoOwnershipMainOwner || !row.EvidenceFrom.IsZero() {
		t.Errorf("repo-b = %+v, want team-untimed with no evidence start", row)
	}
	if len(got) != 3 {
		t.Errorf("rows = %+v, want three", derivation.Rows)
	}
}

// A claim with an evidence start is unchanged only when an open row starts at
// it; every other open row of its identity is superseded. A claim with no
// evidence start keeps whatever open row it has.
func TestAnOpenRowAtTheRunTimeIsSupersededByTheEvidenceStart(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	evidence := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000f1")
	untimedRepo := uuid.MustParse("00000000-0000-4000-8000-0000000000f2")
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{
		repoID: {Provider: "github", FullName: "acme/a"}, untimedRepo: {Provider: "github", FullName: "acme/b"},
	}
	inferred := teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred]
	whole := 1.0
	active := func(repo uuid.UUID, name string, start time.Time) teamRepoOwnershipActiveRow {
		return teamRepoOwnershipActiveRow{Provider: "github", TeamID: "team", RepoID: repo, RepoFullName: name, MatchType: "exact",
			Specificity: inferred.Specificity, Priority: inferred.Priority, ValidFrom: start,
			IsPrimary: teamRepoOwnershipMainOwner, Share: &whole}
	}
	runTime := now.Add(-40 * 24 * time.Hour)
	derived := []DerivedTeamRepoOwnershipRow{
		{TeamID: "team", RepoID: repoID.String(), Specificity: inferred.Specificity, EvidenceFrom: evidence, IsPrimary: teamRepoOwnershipMainOwner, Share: 1},
		{TeamID: "team", RepoID: untimedRepo.String(), Specificity: inferred.Specificity, IsPrimary: teamRepoOwnershipMainOwner, Share: 1},
	}

	activeRows := []teamRepoOwnershipActiveRow{active(repoID, "acme/a", runTime), active(untimedRepo, "acme/b", runTime)}
	if toWrite := filterUnchangedTeamRepoOwnershipRows(derived, repos, activeRows); len(toWrite) != 1 || toWrite[0].RepoID != repoID.String() {
		t.Fatalf("toWrite = %+v, want only the claim with an evidence start and no row at it", toWrite)
	}
	if superseded := supersededTeamRepoOwnershipRows(derived, repos, activeRows, now); len(superseded) != 1 || !superseded[0].ValidFrom.Equal(runTime) || superseded[0].RepoID != repoID {
		t.Fatalf("superseded = %+v, want the run-time row of acme/a only", superseded)
	}

	settled := []teamRepoOwnershipActiveRow{active(repoID, "acme/a", evidence), active(untimedRepo, "acme/b", runTime)}
	if toWrite := filterUnchangedTeamRepoOwnershipRows(derived, repos, settled); len(toWrite) != 0 {
		t.Errorf("settled toWrite = %+v, want nothing", toWrite)
	}
	if superseded := supersededTeamRepoOwnershipRows(derived, repos, settled, now); len(superseded) != 0 {
		t.Errorf("settled superseded = %+v, want nothing", superseded)
	}

	leftover := append(settled, active(repoID, "acme/a", runTime))
	if superseded := supersededTeamRepoOwnershipRows(derived, repos, leftover, now); len(superseded) != 1 || !superseded[0].ValidFrom.Equal(runTime) {
		t.Errorf("leftover superseded = %+v, want the run-time row beside the evidence row", superseded)
	}

	future := []DerivedTeamRepoOwnershipRow{{TeamID: "team", RepoID: repoID.String(), EvidenceFrom: now.Add(time.Hour)}}
	if got := inferredValidFrom(future[0], now); !got.Equal(now) {
		t.Errorf("evidence in the future of the run starts at %v, want the run %v", got, now)
	}
}
