package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A tie for the largest linked share keeps the main owner it already has: the
// one tied team whose open row is a main row (is_primary 1, or 0 from the
// one-owner derivation) stays the main owner and starts at the repo's earliest
// evidence. With no such team, or two, the repository has no main owner. A
// team outside the tie is never promoted.
func TestATieKeepsTheMainOwnerItAlreadyHas(t *testing.T) {
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: {Provider: "github", FullName: "acme/a"}}
	repoStart := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	tied := func(team string) DerivedTeamRepoOwnershipRow {
		return DerivedTeamRepoOwnershipRow{TeamID: team, RepoID: repo.String(), IsPrimary: teamRepoOwnershipCoOwner, Share: 0.4, Tied: true,
			EvidenceFrom: repoStart.Add(24 * time.Hour), repoEvidenceFrom: repoStart}
	}
	third := DerivedTeamRepoOwnershipRow{TeamID: "team-c", RepoID: repo.String(), IsPrimary: teamRepoOwnershipCoOwner, Share: 0.2}
	open := func(team string, primary uint8) teamRepoOwnershipActiveRow {
		return teamRepoOwnershipActiveRow{Provider: "github", RepoFullName: "acme/a", TeamID: team, RepoID: repo, IsPrimary: primary}
	}
	derived := []DerivedTeamRepoOwnershipRow{tied("team-a"), tied("team-b"), third}
	cases := []struct {
		name string
		open []teamRepoOwnershipActiveRow
		want map[string]uint8
	}{
		{"no open row", nil, map[string]uint8{"team-a": 2, "team-b": 2, "team-c": 2}},
		{"team-b is the main owner", []teamRepoOwnershipActiveRow{open("team-a", 2), open("team-b", 1)}, map[string]uint8{"team-a": 2, "team-b": 1, "team-c": 2}},
		{"team-a holds a row of the one-owner derivation", []teamRepoOwnershipActiveRow{open("team-a", 0)}, map[string]uint8{"team-a": 1, "team-b": 2, "team-c": 2}},
		{"two tied main rows name no main owner", []teamRepoOwnershipActiveRow{open("team-a", 1), open("team-b", 0)}, map[string]uint8{"team-a": 2, "team-b": 2, "team-c": 2}},
		{"the main row of a team outside the tie is not kept", []teamRepoOwnershipActiveRow{open("team-c", 1)}, map[string]uint8{"team-a": 2, "team-b": 2, "team-c": 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := map[string]uint8{}
			for _, row := range resolveTiedMainOwners(derived, repos, tc.open) {
				got[row.TeamID] = row.IsPrimary
				if row.IsPrimary == teamRepoOwnershipMainOwner && !row.EvidenceFrom.Equal(repoStart) {
					t.Errorf("the kept main owner %s starts at %v, want the repo's earliest evidence %v", row.TeamID, row.EvidenceFrom, repoStart)
				}
			}
			for team, want := range tc.want {
				if got[team] != want {
					t.Errorf("%s is_primary = %d, want %d (all %v)", team, got[team], want, got)
				}
			}
		})
	}
}

// A claim with evidence starts at it. A claim with no timed evidence keeps the
// start of its earliest open row, so a new share replaces that row (same key)
// and leaves one open row; a new claim with no evidence starts at the run.
func TestAClaimWithNoEvidenceKeepsTheStartOfItsOpenRow(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	firstRun := now.Add(-72 * time.Hour)
	evidence := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000b1")
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: {Provider: "gitlab", FullName: "grp/p"}}
	oldShare := 1.0
	active := []teamRepoOwnershipActiveRow{{Provider: "gitlab", RepoFullName: "grp/p", TeamID: "team-a", RepoID: repo,
		MatchType: "exact", IsPrimary: teamRepoOwnershipMainOwner, Share: &oldShare, ValidFrom: firstRun,
		Specificity: teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred].Specificity}}
	derived := []DerivedTeamRepoOwnershipRow{
		{TeamID: "team-a", RepoID: repo.String(), IsPrimary: teamRepoOwnershipMainOwner, Share: 0.75,
			Specificity: teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred].Specificity},
		{TeamID: "team-b", RepoID: repo.String(), IsPrimary: teamRepoOwnershipCoOwner, Share: 0.25},
		{TeamID: "team-c", RepoID: repo.String(), IsPrimary: teamRepoOwnershipCoOwner, Share: 0.25, EvidenceFrom: evidence},
		{TeamID: "team-d", RepoID: repo.String(), IsPrimary: teamRepoOwnershipCoOwner, Share: 0.25, EvidenceFrom: now.Add(time.Hour)},
	}
	stamped := stampTeamRepoOwnershipClaimStarts(derived, repos, active, now)
	want := map[string]time.Time{"team-a": firstRun, "team-b": now, "team-c": evidence, "team-d": now}
	for _, row := range stamped {
		if got := inferredValidFrom(row, now); !got.Equal(want[row.TeamID]) {
			t.Errorf("%s starts at %v, want %v", row.TeamID, got, want[row.TeamID])
		}
	}
	toWrite := filterUnchangedTeamRepoOwnershipRows(stamped[:1], repos, active)
	if len(toWrite) != 1 {
		t.Fatalf("a new share must be written, got %+v", toWrite)
	}
	if superseded := supersededTeamRepoOwnershipRows(stamped[:1], repos, active, now); len(superseded) != 0 {
		t.Errorf("the new version replaces the open row under its key; superseded %+v", superseded)
	}
}
