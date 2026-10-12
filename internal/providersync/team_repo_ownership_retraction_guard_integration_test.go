//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// CHAOS-9045 against the real migration chain: the retraction of a derived
// team_repo_ownership row is guarded per provider. Every ordered pair of
// jira, gitlab, github and linear is provider A (its links closed) and
// provider B (its link open) once.

type retractionGuardOrg struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	orgID string
	at    time.Time
	repos map[string]uuid.UUID
	issue int
}

func newRetractionGuardOrg(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repos ...string) *retractionGuardOrg {
	t.Helper()
	org := &retractionGuardOrg{t: t, ctx: ctx, conn: conn, orgID: orgID,
		at: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC), repos: map[string]uuid.UUID{}}
	byID := map[uuid.UUID]string{}
	for _, repo := range repos {
		org.repos[repo] = uuid.New()
		byID[org.repos[repo]] = repo
	}
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, byID)
	return org
}

// own seeds an open link of the team to the provider's project, and one issue
// of that project linked to a PR of the repo.
func (org *retractionGuardOrg) own(provider, projectID, teamID, repo string) {
	org.t.Helper()
	org.issue++
	workItemID := fmt.Sprintf("%s:G-%d", provider, org.issue)
	seedTeamProjectOwnership(org.t, org.ctx, org.conn, org.orgID, provider, projectID, teamID, true, org.at)
	seedWorkItem(org.t, org.ctx, org.conn, org.orgID, workItemID, provider, uuid.Nil, projectID, org.at)
	seedWorkGraphIssuePR(org.t, org.ctx, org.conn, org.orgID, org.repos[repo], workItemID, uint32(org.issue), org.at)
}

// closeLink writes the link's own key again with valid_to set: what an
// upstream close (right or wrong) of the provider's ownership writer stores.
func (org *retractionGuardOrg) closeLink(provider, projectID, teamID string) {
	org.t.Helper()
	closedAt := org.at.Add(time.Hour)
	batch, err := org.conn.PrepareBatch(org.ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`)
	if err != nil {
		org.t.Fatalf("prepare close: %v", err)
	}
	if err := batch.Append(org.orgID, provider, teamID, projectID, "native", uint8(1), uint16(100), int32(0), org.at, closedAt, closedAt); err != nil {
		org.t.Fatalf("append close: %v", err)
	}
	if err := batch.Send(); err != nil {
		org.t.Fatalf("send close: %v", err)
	}
}

func (org *retractionGuardOrg) derive() (written, retracted int, inputsReady bool, stats TeamRepoOwnershipDerivationStats) {
	org.t.Helper()
	written, retracted, inputsReady, _, stats, err := TeamRepoOwnershipDerivationService{Conn: org.conn}.DeriveWithStats(org.ctx, org.orgID)
	if err != nil {
		org.t.Fatalf("Derive: %v", err)
	}
	return written, retracted, inputsReady, stats
}

func (org *retractionGuardOrg) assertOpenOwner(repo, team string) {
	org.t.Helper()
	got := readTeamRepoOwnership(org.t, org.ctx, org.conn, org.orgID)
	row, ok := got[repo]
	if !ok || row.teamID != team {
		org.t.Fatalf("open inferred owner of %s = %+v, want %s (all rows %+v)", repo, row, team, got)
	}
}

func (org *retractionGuardOrg) assertNoOpenOwner(repo string) {
	org.t.Helper()
	if row, ok := readTeamRepoOwnership(org.t, org.ctx, org.conn, org.orgID)[repo]; ok {
		org.t.Fatalf("%s still has an open inferred owner %+v, want none", repo, row)
	}
}

var retractionGuardIntegrationProviders = []string{"jira", "gitlab", "github", "linear"}

// TestDerivedRepoOwnershipRetractionIsGuardedPerProvider: provider A's only
// link is closed and provider B keeps one. The run cannot reconfirm A's row,
// so the row stays open (before CHAOS-9045, B's link let the run retract it).
// Then A gets an open link again, of another team and project: A's own input
// is in the run and does not derive A's old row, so the row is retracted.
func TestDerivedRepoOwnershipRetractionIsGuardedPerProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, a := range retractionGuardIntegrationProviders {
		for _, b := range retractionGuardIntegrationProviders {
			if a == b {
				continue
			}
			t.Run(a+"_closed_"+b+"_open", func(t *testing.T) {
				org := newRetractionGuardOrg(t, ctx, conn, "chaos-9045-"+a+"-"+b, "acme/repo-a", "acme/repo-b", "acme/repo-a2")
				org.own(a, a+"-proj-a", "team-a", "acme/repo-a")
				org.own(b, b+"-proj-b", "team-b", "acme/repo-b")
				if written, retracted, _, _ := org.derive(); written != 2 || retracted != 0 {
					t.Fatalf("first Derive: written=%d retracted=%d, want 2 and 0", written, retracted)
				}

				org.closeLink(a, a+"-proj-a", "team-a")
				written, retracted, inputsReady, stats := org.derive()
				if !inputsReady || written != 0 || retracted != 0 || stats.RetractionsHeld != 1 {
					t.Fatalf("A closed, B open: inputsReady=%v written=%d retracted=%d held=%d, want true 0 0 1",
						inputsReady, written, retracted, stats.RetractionsHeld)
				}
				org.assertOpenOwner("acme/repo-a", "team-a")
				org.assertOpenOwner("acme/repo-b", "team-b")

				org.own(a, a+"-proj-a2", "team-a2", "acme/repo-a2")
				written, retracted, _, stats = org.derive()
				if written != 1 || retracted != 1 || stats.RetractionsHeld != 0 {
					t.Fatalf("A open again: written=%d retracted=%d held=%d, want 1 1 0", written, retracted, stats.RetractionsHeld)
				}
				org.assertNoOpenOwner("acme/repo-a")
				org.assertOpenOwner("acme/repo-a2", "team-a2")
				org.assertOpenOwner("acme/repo-b", "team-b")
			})
		}
	}
}

// TestDerivedRepoOwnershipSingleProviderClosedLinkKeepsTheRow: the
// single-provider org keeps its row when its only link closes, as before.
func TestDerivedRepoOwnershipSingleProviderClosedLinkKeepsTheRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, a := range retractionGuardIntegrationProviders {
		t.Run(a, func(t *testing.T) {
			org := newRetractionGuardOrg(t, ctx, conn, "chaos-9045-single-"+a, "acme/repo-a")
			org.own(a, a+"-proj-a", "team-a", "acme/repo-a")
			if written, _, _, _ := org.derive(); written != 1 {
				t.Fatalf("first Derive: written=%d, want 1", written)
			}
			org.closeLink(a, a+"-proj-a", "team-a")
			if _, retracted, _, _ := org.derive(); retracted != 0 {
				t.Fatalf("second Derive: retracted=%d, want 0", retracted)
			}
			org.assertOpenOwner("acme/repo-a", "team-a")
		})
	}
}
