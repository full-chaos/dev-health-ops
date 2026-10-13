//go:build integration

package providersync

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// TestAnInactiveTeamsTenureNeverDelaysTheNewMainOwner is the team id
// replacement: the old team id owned the repository over its history and is
// inactive now, and the new id holds the same repository with evidence older
// than the replacement. An inactive team owns nothing for any reader
// (team-attribution.md section 0.4g rule 1), so the new id's main row starts
// at its evidence, whether the old row was closed at the replacement by an
// earlier run or is closed by a run before. The prod shape is the first with the
// new id's open row that an earlier build started at the replacement: the run
// moves it back to the evidence. An active prior owner still moves the start
// to its end, so the two never own the repository at once.
func TestAnInactiveTeamsTenureNeverDelaysTheNewMainOwner(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Now().UTC()
	evidence := now.Add(-300 * 24 * time.Hour).Truncate(time.Second)
	replacedAt := now.Add(-4 * 24 * time.Hour).Truncate(time.Second)
	history := now.Add(-700 * 24 * time.Hour).Truncate(time.Second)
	beforeReplacement := evidence.Add(24 * time.Hour)

	for _, provider := range evidenceSettleProviders {
		for _, shape := range []string{"closed_at_replacement", "closed_by_this_run", "new_row_at_replacement"} {
			for _, oldActive := range []bool{false, true} {
				closedEarlier := shape != "closed_by_this_run"
				state := map[bool]string{true: "active", false: "inactive"}[oldActive]
				t.Run(provider+"/"+shape+"/old_"+state, func(t *testing.T) {
					org := fmt.Sprintf("org-inactive-floor-%s-%s-%v", provider, shape, oldActive)
					oldTeam, newTeam := provider+"-team-old", provider+"-team-new"
					repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
					repoProvider := provider
					if provider == "jira" || provider == "linear" {
						repoProvider = "github"
					}
					active := uint8(0)
					if oldActive {
						active = 1
					}
					for team, isActive := range map[string]uint8{oldTeam: active, newTeam: 1} {
						if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], ?, ?, ?, ?, '')`,
							team, uuid.New(), "team "+team, isActive, replacedAt, org, provider); err != nil {
							t.Fatal(err)
						}
					}
					var validTo any
					if closedEarlier {
						validTo = replacedAt
					}
					if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, ?, ?)`,
						org, repoProvider, oldTeam, repo, "acme/"+org, history, validTo, replacedAt); err != nil {
						t.Fatal(err)
					}
					if shape == "new_row_at_replacement" {
						if err := conn.Exec(ctx, teamRepoOwnershipShareInsert+` VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, NULL, ?, 1)`,
							org, repoProvider, newTeam, repo, "acme/"+org, replacedAt, replacedAt); err != nil {
							t.Fatal(err)
						}
					}
					seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-new", newTeam, true, now.Add(-time.Hour))
					seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-new", 1, evidence)
					seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-new", 2, evidence.Add(time.Hour))

					service := TeamRepoOwnershipDerivationService{Conn: conn}
					beforeRun := time.Now().UTC().Add(-time.Second)
					// A run that closes the old team's open main row starts the
					// new one at that run, and the next run moves it back to its
					// evidence, so two main rows are never open at once.
					for run := 1; run <= 3; run++ {
						written, _, ready, _, stats, err := service.DeriveWithStats(ctx, org)
						if err != nil || !ready {
							t.Fatalf("run %d: ready %v err %v", run, ready, err)
						}
						if run == 3 && (written != 0 || stats.Superseded != 0) {
							t.Errorf("rerun wrote %d and superseded %d, want 0 and 0", written, stats.Superseded)
						}
						openMain := map[string]bool{}
						for _, span := range ownershipRowSpans(t, ctx, conn, org) {
							if span.validTo == nil && span.primary != teamRepoOwnershipCoOwner {
								openMain[span.team] = true
							}
						}
						if openMain[oldTeam] && openMain[newTeam] {
							t.Fatalf("run %d: the old and the new team both hold an open main row", run)
						}
					}

					starts := openOwnershipStarts(t, ctx, conn, org, newTeam)
					if provider == "linear" && oldActive && !closedEarlier {
						// The run holds no Linear work item that names the
						// active old team's key, so the retraction guard keeps
						// its row and the new owner waits: one owner at a time.
						if len(starts) != 0 {
							t.Fatalf("new team open rows start at %v, want none while the old team's retraction is held", starts)
						}
						assertSingleOwner(t, ctx, conn, org, repo, oldTeam, newTeam, beforeReplacement)
						return
					}
					if len(starts) != 1 {
						t.Fatalf("new team open rows start at %v, want one", starts)
					}
					switch {
					case !oldActive:
						if !starts[0].Equal(evidence) {
							t.Fatalf("new team starts at %v, want its evidence %v: an inactive team's tenure moved it", starts[0], evidence)
						}
						owned, err := teamownership.OwnedRepoIDs(ctx, conn, org, newTeam, beforeReplacement)
						if err != nil {
							t.Fatal(err)
						}
						if len(owned) != 1 || owned[0] != repo {
							t.Errorf("new team owns %v at %v, want [%s]", owned, beforeReplacement, repo)
						}
					case closedEarlier:
						if !starts[0].Equal(replacedAt) {
							t.Fatalf("new team starts at %v, want the active prior owner's end %v", starts[0], replacedAt)
						}
						assertSingleOwner(t, ctx, conn, org, repo, oldTeam, newTeam, beforeReplacement)
					default:
						if starts[0].Before(beforeRun) {
							t.Fatalf("new team starts at %v, want the run that closed the active prior owner (after %v)", starts[0], beforeRun)
						}
						assertSingleOwner(t, ctx, conn, org, repo, oldTeam, newTeam, beforeReplacement)
					}
					if !closedEarlier {
						for _, span := range ownershipRowSpans(t, ctx, conn, org) {
							if span.team == oldTeam && span.validTo == nil {
								t.Errorf("old team's row is still open, want it closed by the run")
							}
						}
					}
				})
			}
		}
	}
}

// TestANewMainOwnerWaitsWhileAnInactiveTeamsRetractionIsHeld: the retraction
// guard keeps the open main row of an inactive old id (no Linear work item
// names a Linear team key, so a row of the linear_team_key arm cannot be
// proven gone). The new main owner waits for a run that closes it, so the two
// never hold open main rows at once, and if the old id is active again it is
// the one owner of that time. The hold of every other kind goes through the
// same repository freeze (TestARoleChangeIsDatedAtTheRunAndHeldPerRepository).
func TestANewMainOwnerWaitsWhileAnInactiveTeamsRetractionIsHeld(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Now().UTC()
	evidence := now.Add(-300 * 24 * time.Hour).Truncate(time.Second)
	history := now.Add(-700 * 24 * time.Hour).Truncate(time.Second)
	replacedAt := now.Add(-4 * 24 * time.Hour).Truncate(time.Second)
	org := "org-inactive-floor-held-linear"
	oldTeam, newTeam := "linear:OLD", "linear:NEW"
	repo := seedEvidenceSettleRepo(t, ctx, conn, org, "linear")
	setActive := func(team string, active uint8, at time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], ?, ?, ?, 'linear', '')`,
			team, uuid.New(), "team "+team, active, at, org); err != nil {
			t.Fatal(err)
		}
	}
	setActive(oldTeam, 0, replacedAt)
	setActive(newTeam, 1, replacedAt)
	if err := conn.Exec(ctx, teamRepoOwnershipShareInsert+` VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 1, 1, 0, ?, NULL, ?, 1)`,
		org, oldTeam, repo, "acme/"+org, history, replacedAt); err != nil {
		t.Fatal(err)
	}
	seedTeamProjectOwnership(t, ctx, conn, org, "linear", "proj-new", newTeam, true, now.Add(-time.Hour))
	seedOwnershipEvidence(t, ctx, conn, org, "linear", repo, "proj-new", 1, evidence)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	for run := 1; run <= 2; run++ {
		written, _, ready, _, stats, err := service.DeriveWithStats(ctx, org)
		if err != nil || !ready {
			t.Fatalf("run %d: ready %v err %v", run, ready, err)
		}
		if written != 0 || stats.RetractionsHeld == 0 {
			t.Fatalf("run %d: written %d held %d, want the new owner held beside the held retraction", run, written, stats.RetractionsHeld)
		}
	}
	if starts := openOwnershipStarts(t, ctx, conn, org, newTeam); len(starts) != 0 {
		t.Fatalf("new team open rows start at %v, want none while the old row is held", starts)
	}
	setActive(oldTeam, 1, time.Now().UTC())
	assertSingleOwner(t, ctx, conn, org, repo, oldTeam, newTeam, evidence.Add(24*time.Hour))
}
