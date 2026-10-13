package providersync

import (
	"fmt"
	"sort"
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

// A role change is dated at the run: the claim whose role changes starts its
// new row at the run and closes its open row of the old role, and a new main
// owner of a repository that has another team's open main row starts at the
// run. A held close or retraction on a repository holds every role change of
// that repository, so it keeps one main row.
func TestARoleChangeIsDatedAtTheRunAndHeldPerRepository(t *testing.T) {
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	evidence := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000c1")
	repos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: {Provider: "jira", FullName: "acme/r"}}
	row := func(team string, primary uint8) DerivedTeamRepoOwnershipRow {
		return DerivedTeamRepoOwnershipRow{TeamID: team, RepoID: repo.String(), IsPrimary: primary, Share: 0.5, validFrom: evidence}
	}
	open := func(team string, primary uint8) teamRepoOwnershipActiveRow {
		return teamRepoOwnershipActiveRow{Provider: "jira", RepoFullName: "acme/r", TeamID: team, RepoID: repo, IsPrimary: primary, ValidFrom: evidence}
	}
	cases := []struct {
		name       string
		derived    []DerivedTeamRepoOwnershipRow
		open       []teamRepoOwnershipActiveRow
		wantAtRun  []string
		wantCloses []string
	}{
		{"no change", []DerivedTeamRepoOwnershipRow{row("a", 1), row("b", 2)}, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 2)}, nil, nil},
		{"winner change A -> B", []DerivedTeamRepoOwnershipRow{row("a", 2), row("b", 1)}, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 2)}, []string{"a", "b"}, []string{"a", "b"}},
		{"a row of the one-owner derivation counts as main", []DerivedTeamRepoOwnershipRow{row("a", 1), row("b", 2)}, []teamRepoOwnershipActiveRow{open("a", 0)}, nil, nil},
		{"a new main owner beside another team's open main row", []DerivedTeamRepoOwnershipRow{row("b", 1)}, []teamRepoOwnershipActiveRow{open("a", 1)}, []string{"b"}, nil},
		{"of two open main rows the derived main owner keeps its start", []DerivedTeamRepoOwnershipRow{row("a", 1), row("b", 2)}, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 1)}, []string{"b"}, []string{"b"}},
		{"a new co-owner keeps its evidence start", []DerivedTeamRepoOwnershipRow{row("a", 1), row("c", 2)}, []teamRepoOwnershipActiveRow{open("a", 1)}, nil, nil},
		{"a main owner with no open row on a repository with none keeps its evidence start", []DerivedTeamRepoOwnershipRow{row("a", 1)}, nil, nil, nil},
	}
	names := func(rows []teamRepoOwnershipActiveRow) []string {
		var out []string
		for _, r := range rows {
			out = append(out, r.TeamID)
		}
		sort.Strings(out)
		return out
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, closes, changed := applyTeamRepoOwnershipRoleChanges(tc.derived, repos, tc.open, now)
			var atRun []string
			for _, r := range got {
				switch {
				case r.validFrom.Equal(now):
					atRun = append(atRun, r.TeamID)
					if !changed[teamRepoOwnershipClaim{"jira", "acme/r", r.TeamID}] {
						t.Errorf("%s starts at the run but is not a changed claim", r.TeamID)
					}
				case !r.validFrom.Equal(evidence):
					t.Errorf("%s starts at %v, want the evidence or the run", r.TeamID, r.validFrom)
				}
			}
			sort.Strings(atRun)
			if fmt.Sprint(atRun) != fmt.Sprint(tc.wantAtRun) || fmt.Sprint(names(closes)) != fmt.Sprint(tc.wantCloses) {
				t.Errorf("at run %v closes %v, want %v and %v", atRun, names(closes), tc.wantAtRun, tc.wantCloses)
			}
		})
	}

	t.Run("a held close or retraction holds a repository whose roles change", func(t *testing.T) {
		otherRepo := uuid.MustParse("00000000-0000-4000-8000-0000000000c2")
		bothRepos := map[uuid.UUID]teamRepoOwnershipRepoInfo{repo: repos[repo], otherRepo: {Provider: "jira", FullName: "acme/s"}}
		elsewhere := DerivedTeamRepoOwnershipRow{TeamID: "a", RepoID: otherRepo.String(), IsPrimary: 1, Share: 1, validFrom: evidence}
		retractC := open("c", 2)

		// A -> B where the guard holds A's close: nothing of acme/r changes, acme/s is written.
		derived, closes, changed := applyTeamRepoOwnershipRoleChanges([]DerivedTeamRepoOwnershipRow{row("a", 2), row("b", 1), elsewhere}, bothRepos, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 2)}, now)
		keptByGuard := withoutRows(closes, closes[:1])
		held := teamRepoOwnershipReposOf(withoutRows(closes, keptByGuard))
		out, retractions, kept, n := withoutHeldRoleChanges(derived, changed, []teamRepoOwnershipActiveRow{retractC}, keptByGuard, held, bothRepos)
		if len(out) != 1 || out[0].RepoID != otherRepo.String() || len(retractions) != 0 || len(kept) != 0 || n != 4 {
			t.Errorf("held close: derived %+v retractions %v closes %v held %d, want only acme/s, no retraction, no close, 4 held", out, names(retractions), names(kept), n)
		}

		// The guard holds A's retraction while B becomes the main owner: B waits.
		derived, closes, changed = applyTeamRepoOwnershipRoleChanges([]DerivedTeamRepoOwnershipRow{row("b", 1)}, repos, []teamRepoOwnershipActiveRow{open("a", 1)}, now)
		held = teamRepoOwnershipReposOf([]teamRepoOwnershipActiveRow{open("a", 1)}, nil)
		out, _, kept, n = withoutHeldRoleChanges(derived, changed, nil, closes, held, repos)
		if len(out) != 0 || len(kept) != 0 || n != 1 {
			t.Errorf("held main retraction: derived %+v closes %v held %d, want B held", out, names(kept), n)
		}

		// A held retraction on a repository whose roles do not change holds nothing else.
		derived, closes, changed = applyTeamRepoOwnershipRoleChanges([]DerivedTeamRepoOwnershipRow{row("a", 1), row("b", 2)}, repos, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 2), retractC}, now)
		held = teamRepoOwnershipReposOf([]teamRepoOwnershipActiveRow{retractC}, nil)
		out, _, kept, n = withoutHeldRoleChanges(derived, changed, nil, closes, held, repos)
		if len(out) != 2 || len(kept) != 0 || n != 0 {
			t.Errorf("held co-owner retraction: derived %d closes %d held %d, want 2 0 0", len(out), len(kept), n)
		}

		// Nothing held: the role change goes through.
		derived, closes, changed = applyTeamRepoOwnershipRoleChanges([]DerivedTeamRepoOwnershipRow{row("a", 2), row("b", 1)}, repos, []teamRepoOwnershipActiveRow{open("a", 1), open("b", 2)}, now)
		out, retractions, kept, n = withoutHeldRoleChanges(derived, changed, []teamRepoOwnershipActiveRow{retractC}, closes, nil, repos)
		if len(out) != 2 || len(retractions) != 1 || len(kept) != 2 || n != 0 {
			t.Errorf("nothing held: derived %d retractions %d closes %d held %d, want 2 1 2 0", len(out), len(retractions), len(kept), n)
		}
	})
}
