//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

var evidenceSettleProviders = []string{"github", "gitlab", "jira", "linear"}

// TestInferredOwnershipWithFutureEvidenceIsWrittenOnce: a claim whose only
// evidence is in the future of the run starts at the run, and later runs write
// nothing (no new row, no supersede), so the claim keeps one stored row.
func TestInferredOwnershipWithFutureEvidenceIsWrittenOnce(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	future := time.Now().UTC().Add(365 * 24 * time.Hour).Truncate(time.Second)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-evidence-future-" + provider
			team := provider + "-team-core"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-core", team, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-core", 1, future)

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			before := time.Now().UTC().Add(-time.Second)
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			starts := openOwnershipStarts(t, ctx, conn, org, team)
			if len(starts) != 1 || starts[0].Before(before) || !starts[0].Before(future) {
				t.Fatalf("open rows start at %v, want one at the run", starts)
			}
			for run := 2; run <= 3; run++ {
				written, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				if written != 0 || stats.Superseded != 0 {
					t.Errorf("run %d wrote %d and superseded %d, want 0 and 0", run, written, stats.Superseded)
				}
			}
			if stored := storedInferredOwnershipRows(t, ctx, conn, org, team); stored != 1 {
				t.Errorf("stored inferred rows of the claim = %d, want 1", stored)
			}
		})
	}
}

// TestReactivatedInferredOwnershipKeepsItsRetraction: team A owns the
// repository from its evidence, then A's links all move to team B's project
// and A is retracted (a team with any link stays an owner, ruling D5988, so
// only losing every link retracts it), then A wins again. A's new row starts at that run, not at A's old evidence (a row
// at the old evidence would have the key of the retracted row and reopen it):
// a read between the retraction and the new win does not name A, and a read
// of A's first tenure still does.
func TestReactivatedInferredOwnershipKeepsItsRetraction(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	oldEvidence := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	midEvidence := time.Date(2026, 8, 3, 4, 5, 6, 0, time.UTC)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-evidence-reactivate-" + provider
			teamA, teamB := provider+"-team-a", provider+"-team-b"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-a", teamA, true, time.Now().UTC().Add(-time.Hour))
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 1, oldEvidence)
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 2, oldEvidence.Add(time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 3, midEvidence)

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			if starts := openOwnershipStarts(t, ctx, conn, org, teamA); len(starts) != 1 || !starts[0].Equal(oldEvidence) {
				t.Fatalf("A open rows start at %v, want one at %v", starts, oldEvidence)
			}

			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 1, oldEvidence)
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 2, oldEvidence.Add(time.Hour))
			if _, retracted, _, _, err := service.Derive(ctx, org); err != nil || retracted != 1 {
				t.Fatalf("A loses every link: retracted %d err %v, want A's row retracted", retracted, err)
			}
			gapAt := time.Now().UTC()
			time.Sleep(50 * time.Millisecond)

			for number := uint32(3); number <= 5; number++ {
				seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", number, midEvidence.Add(time.Duration(number-3)*time.Hour))
			}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("A wins again: ready %v err %v", ready, err)
			}
			starts := openOwnershipStarts(t, ctx, conn, org, teamA)
			if len(starts) != 1 || !starts[0].After(gapAt) {
				t.Fatalf("A open rows start at %v, want one after the retraction gap %v", starts, gapAt)
			}

			inGap, err := teamownership.OwnedRepoIDs(ctx, conn, org, teamA, gapAt)
			if err != nil {
				t.Fatal(err)
			}
			if len(inGap) != 0 {
				t.Errorf("owned repositories of A between its retraction and its new win = %v, want none", inGap)
			}
			firstTenure, err := teamownership.OwnedRepoIDs(ctx, conn, org, teamA, oldEvidence.Add(24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if len(firstTenure) != 1 || firstTenure[0] != repo {
				t.Errorf("owned repositories of A in its first tenure = %v, want [%s]", firstTenure, repo)
			}

			var roles []string
			rows, err := conn.Query(ctx, `SELECT team_id, is_primary, share FROM team_repo_ownership FINAL
WHERE org_id = ? AND source = 'inferred' AND valid_to IS NULL ORDER BY team_id`, org)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var team string
				var primary uint8
				var share *float64
				if err := rows.Scan(&team, &primary, &share); err != nil {
					t.Fatal(err)
				}
				if share == nil {
					t.Fatalf("open row of %s has no share", team)
				}
				roles = append(roles, fmt.Sprintf("%s:%d:%.2f", team, primary, *share))
			}
			_ = rows.Close()
			if want := []string{teamA + ":1:0.60", teamB + ":2:0.40"}; fmt.Sprint(roles) != fmt.Sprint(want) {
				t.Errorf("open rows after A wins again = %v, want %v", roles, want)
			}

			written, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			if written != 0 || stats.Superseded != 0 {
				t.Errorf("rerun wrote %d and superseded %d, want 0 and 0", written, stats.Superseded)
			}
		})
	}
}

// TestEarlierEvidenceMovesTheStartPastASupersededRow: a claim first written
// at the run, then moved to its evidence (the run-time row is superseded, a
// row closed at its own start), moves again when earlier evidence is synced.
// A superseded row is no retraction, so it does not hold the start back.
func TestEarlierEvidenceMovesTheStartPastASupersededRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	evidence := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
	earlier := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-evidence-earlier-" + provider
			team := provider + "-team-core"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-core", team, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-core", 1, time.Time{})

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-core", 1, evidence)
			if _, _, _, _, stats, err := service.DeriveWithStats(ctx, org); err != nil || stats.Superseded != 1 {
				t.Fatalf("second derive: superseded %d err %v, want the run-time row superseded", stats.Superseded, err)
			}
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-core", 2, earlier)
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("third derive: ready %v err %v", ready, err)
			}
			if starts := openOwnershipStarts(t, ctx, conn, org, team); len(starts) != 1 || !starts[0].Equal(earlier) {
				t.Errorf("open rows start at %v, want one at the earlier evidence %v", starts, earlier)
			}
		})
	}
}

// TestANewWinnerStartsWhenThePriorOwnerEnds: team A owns the repository, then
// team B wins with evidence older than the run. B's row starts at the run that
// retracts A, not at B's evidence, so one team owns the repository at a time;
// a rerun writes nothing.
func TestANewWinnerStartsWhenThePriorOwnerEnds(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	oldEvidence := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	winnerEvidence := time.Now().UTC().Add(-30 * 24 * time.Hour).Truncate(time.Second)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-evidence-handover-" + provider
			teamA, teamB := provider+"-team-a", provider+"-team-b"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-a", teamA, true, time.Now().UTC().Add(-time.Hour))
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 1, oldEvidence)
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 2, oldEvidence.Add(time.Hour))

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			for number := uint32(3); number <= 5; number++ {
				seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", number, winnerEvidence.Add(time.Duration(number-3)*time.Hour))
			}
			beforeHandover := time.Now().UTC().Add(-time.Second)
			// A keeps its links, so it stays an owner (D5988): its main row
			// closes at the run and it continues as a co-owner.
			if _, retracted, _, _, stats, err := service.DeriveWithStats(ctx, org); err != nil || retracted != 0 || stats.RoleChanges != 1 {
				t.Fatalf("B wins: retracted %d role changes %d err %v, want A's main row closed as a role change", retracted, stats.RoleChanges, err)
			}

			starts := openOwnershipStarts(t, ctx, conn, org, teamB)
			if len(starts) != 1 || starts[0].Before(beforeHandover) {
				t.Fatalf("B open rows start at %v, want one at the run that closed A's main row (after %v)", starts, beforeHandover)
			}
			var mains []string
			for _, span := range ownershipRowSpans(t, ctx, conn, org) {
				if span.validTo == nil {
					mains = append(mains, fmt.Sprintf("%s:%d", span.team, span.primary))
				}
			}
			if want := []string{teamA + ":2", teamB + ":1"}; fmt.Sprint(mains) != fmt.Sprint(want) && fmt.Sprint(mains) != fmt.Sprint([]string{want[1], want[0]}) {
				t.Fatalf("open rows %v, want %v", mains, want)
			}
			assertSingleOwner(t, ctx, conn, org, repo, teamA, teamB, winnerEvidence.Add(24*time.Hour))

			written, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			if written != 0 || stats.Superseded != 0 {
				t.Errorf("rerun wrote %d and superseded %d, want 0 and 0", written, stats.Superseded)
			}
		})
	}
}

// TestANewMainOwnerStartsAtAnEndedMainTenureAndAnOpenOneIsKept: team A's
// main row ended at priorEnd (the rows an earlier run left). A new claim of
// team B whose evidence is older than that end starts at the end, so B never
// takes over before A's tenure ends. A stored open main row of B that already
// started before that end was a main owner beside A (a full tie, D5990) and
// keeps its start: the run writes and supersedes nothing.
func TestANewMainOwnerStartsAtAnEndedMainTenureAndAnOpenOneIsKept(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	winnerEvidence := time.Now().UTC().Add(-60 * 24 * time.Hour).Truncate(time.Second)
	priorEnd := time.Now().UTC().Add(-30 * 24 * time.Hour).Truncate(time.Second)

	for _, provider := range evidenceSettleProviders {
		for _, bOpenFirst := range []bool{false, true} {
			name := provider + "/new_claim"
			if bOpenFirst {
				name = provider + "/open_main_row"
			}
			t.Run(name, func(t *testing.T) {
				org := fmt.Sprintf("org-evidence-overlap-%s-%v", provider, bOpenFirst)
				teamA, teamB := provider+"-team-a", provider+"-team-b"
				repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
				repoProvider := provider
				if provider == "jira" || provider == "linear" {
					repoProvider = "github"
				}
				endedA := func() {
					t.Helper()
					if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, ?, ?, ?, ?, 'exact', 'inferred', 0, 1, 0, ?, ?, now64(3))`,
						org, repoProvider, teamA, repo, "acme/"+org, winnerEvidence.Add(-365*24*time.Hour), priorEnd); err != nil {
						t.Fatal(err)
					}
				}
				seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
				seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 1, winnerEvidence)
				service := TeamRepoOwnershipDerivationService{Conn: conn}
				want := priorEnd
				if bOpenFirst {
					if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
						t.Fatalf("first derive: ready %v err %v", ready, err)
					}
					want = winnerEvidence
				}
				endedA()

				for run := 1; run <= 2; run++ {
					written, _, _, _, stats, err := service.DeriveWithStats(ctx, org)
					if err != nil {
						t.Fatal(err)
					}
					wantWritten := 0
					if run == 1 && !bOpenFirst {
						wantWritten = 1
					}
					if written != wantWritten || stats.Superseded != 0 {
						t.Errorf("run %d: written %d superseded %d, want %d and 0", run, written, stats.Superseded, wantWritten)
					}
				}
				if starts := openOwnershipStarts(t, ctx, conn, org, teamB); len(starts) != 1 || !starts[0].Equal(want) {
					t.Fatalf("B open rows start at %v, want one at %v", starts, want)
				}
				if !bOpenFirst {
					assertSingleOwner(t, ctx, conn, org, repo, teamA, teamB, priorEnd.Add(-24*time.Hour))
				}
			})
		}
	}
}

// assertSingleOwner holds that at asOf team A owns the repository and team B
// does not.
func assertSingleOwner(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, teamA, teamB string, asOf time.Time) {
	t.Helper()
	ownedA, err := teamownership.OwnedRepoIDs(ctx, conn, org, teamA, asOf)
	if err != nil {
		t.Fatal(err)
	}
	ownedB, err := teamownership.OwnedRepoIDs(ctx, conn, org, teamB, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if len(ownedA) != 1 || ownedA[0] != repo || len(ownedB) != 0 {
		t.Errorf("owners at %v: A %v, B %v; want A [%s] and B none", asOf, ownedA, ownedB, repo)
	}
}

func seedEvidenceSettleRepo(t *testing.T, ctx context.Context, conn driver.Conn, org, provider string) uuid.UUID {
	t.Helper()
	repo := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org))
	repoProvider := provider
	if provider == "jira" || provider == "linear" {
		repoProvider = "github"
	}
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, now64(3))`,
		repo, "acme/"+org, org, repoProvider); err != nil {
		t.Fatal(err)
	}
	return repo
}

// seedOwnershipEvidence stores one piece of evidence number n of the
// repository in project at time at (written again, it moves the evidence to
// project): a pull or merge request work item (github, gitlab), or an issue
// linked to pull request n (jira, linear).
func seedOwnershipEvidence(
	t *testing.T, ctx context.Context, conn driver.Conn,
	org, provider string, repo uuid.UUID, project string, n uint32, at time.Time,
) {
	t.Helper()
	switch provider {
	case "github", "gitlab":
		itemType := map[string]string{"github": "pr", "gitlab": "merge_request"}[provider]
		insertTimedWorkItem(t, ctx, conn, org, fmt.Sprintf("%s:acme#%d", provider, n), provider, itemType, repo, project, at)
	default:
		issue := fmt.Sprintf("%s:CORE-%d", provider, n)
		insertTimedWorkItem(t, ctx, conn, org, issue, provider, "issue", uuid.Nil, project, time.Time{})
		seedWorkGraphIssuePR(t, ctx, conn, org, repo, issue, n, time.Now().UTC())
		created := at
		if created.IsZero() {
			created = time.Unix(0, 0).UTC()
		}
		insertPullRequest(t, ctx, conn, org, repo, n, created)
	}
}

func storedInferredOwnershipRows(t *testing.T, ctx context.Context, conn driver.Conn, org, team string) uint64 {
	t.Helper()
	var stored uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_repo_ownership FINAL
WHERE org_id = ? AND team_id = ? AND source = 'inferred'`, org, team).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	return stored
}
