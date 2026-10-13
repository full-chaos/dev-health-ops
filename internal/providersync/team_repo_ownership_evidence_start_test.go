package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A co-owner's claim starts at the earliest timed evidence of THAT team; the
// main owner's at the earliest evidence of any claim on the repo, because it
// is the main owner over the whole history; a claim with no timed evidence
// has no evidence start.
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
	if row := got[[2]string{"repo-a", "team-win"}]; row.IsPrimary != teamRepoOwnershipMainOwner || !row.EvidenceFrom.Equal(early) {
		t.Errorf("repo-a main = %+v, want team-win from %v (the repo's earliest evidence, not its own %v)", row, early, mid)
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

// A main owner's start moves to the end of another team's main tenure on the
// repository, read from that team's closed main rows or from the open main row
// this run retracts (the read of the closed rows may not see this run's own
// write yet). Co-owners own the repository beside the main owner: a co-owner's
// start is never moved by another team, and the end of another team's
// co-owner row never moves a main owner.
func TestSettledEvidenceStartsWhereTheOtherOwnerEnds(t *testing.T) {
	repoID := uuid.New()
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repoID: {FullName: "acme/api", Provider: "github"}}
	evidence := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	priorEnd := evidence.Add(30 * 24 * time.Hour)
	now := evidence.Add(90 * 24 * time.Hour)
	main := []DerivedTeamRepoOwnershipRow{{TeamID: "team-b", RepoID: repoID.String(), EvidenceFrom: evidence, IsPrimary: teamRepoOwnershipMainOwner}}
	coOwner := []DerivedTeamRepoOwnershipRow{{TeamID: "team-b", RepoID: repoID.String(), EvidenceFrom: evidence, IsPrimary: teamRepoOwnershipCoOwner}}
	ended := func(repo string, mainRow bool) teamRepoOwnershipTenureEnds {
		claim := teamRepoOwnershipClaim{"github", repo, "team-a"}
		tenure := teamRepoOwnershipTenureEnds{retractedUntil: map[teamRepoOwnershipClaim]time.Time{claim: priorEnd}, mainUntil: map[teamRepoOwnershipClaim]time.Time{}}
		if mainRow {
			tenure.mainUntil[claim] = priorEnd
		}
		return tenure
	}
	retracting := func(primary uint8) []teamRepoOwnershipActiveRow {
		return []teamRepoOwnershipActiveRow{{Provider: "github", RepoFullName: "acme/api", TeamID: "team-a", RepoID: repoID, IsPrimary: primary}}
	}
	cases := []struct {
		name       string
		derived    []DerivedTeamRepoOwnershipRow
		tenure     teamRepoOwnershipTenureEnds
		retracting []teamRepoOwnershipActiveRow
		want       time.Time
		moved      int
	}{
		{"a main owner after another team's closed main row", main, ended("acme/api", true), nil, priorEnd, 1},
		{"a main owner when this run retracts another team's main row", main, teamRepoOwnershipTenureEnds{}, retracting(teamRepoOwnershipMainOwner), now, 1},
		{"a row of the one-owner derivation is a main row", main, teamRepoOwnershipTenureEnds{}, retracting(0), now, 1},
		{"another repository's main owner", main, ended("acme/other", true), nil, evidence, 0},
		{"another team's closed co-owner row", main, ended("acme/api", false), nil, evidence, 0},
		{"this run retracts another team's co-owner row", main, teamRepoOwnershipTenureEnds{}, retracting(teamRepoOwnershipCoOwner), evidence, 0},
		{"a co-owner beside another team's closed main row", coOwner, ended("acme/api", true), nil, evidence, 0},
		{"a co-owner when this run retracts the main row", coOwner, teamRepoOwnershipTenureEnds{}, retracting(teamRepoOwnershipMainOwner), evidence, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			settled, counts := settleInferredEvidence(tc.derived, repos, tc.tenure, tc.retracting, now)
			if !settled[0].EvidenceFrom.Equal(tc.want) || counts.afterOtherOwner != tc.moved {
				t.Errorf("start %v (%+v), want %v and %d moved", settled[0].EvidenceFrom, counts, tc.want, tc.moved)
			}
		})
	}
}
