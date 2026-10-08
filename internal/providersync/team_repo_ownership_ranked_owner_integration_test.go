//go:build integration

package providersync

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The ranked-owner rule against the real migration chain: a repo that two
// or more teams reach goes to the team with the most native links, then the
// most explicit_text links, then the most heuristic links
// (work_graph_issue_pr.provenance). A repo is never dropped only because
// two teams have links to it, and a full tie keeps the existing open owner.
// Every contest runs for each provider pair, so each of jira, github, gitlab
// and linear is the winning and the losing team's provider at least once.

// rankedOwnerOrg is one org's fixture: one repo, and per team one project of
// the team's provider that the team owns.
type rankedOwnerOrg struct {
	t      *testing.T
	ctx    context.Context
	conn   driver.Conn
	orgID  string
	repoID uuid.UUID
	repo   string
	at     time.Time
	issue  int
	pr     uint32
}

func newRankedOwnerOrg(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) *rankedOwnerOrg {
	t.Helper()
	org := &rankedOwnerOrg{t: t, ctx: ctx, conn: conn, orgID: orgID, repoID: uuid.New(), repo: "acme/" + orgID,
		at: time.Date(2026, 10, 7, 19, 0, 0, 0, time.UTC)}
	seedTeamRepoOwnershipRepos(t, ctx, conn, orgID, map[uuid.UUID]string{org.repoID: org.repo})
	return org
}

// ownProject seeds the team's ownership of its provider's project.
func (org *rankedOwnerOrg) ownProject(provider, teamID string) {
	seedTeamProjectOwnership(org.t, org.ctx, org.conn, org.orgID, provider, provider+"-proj-"+teamID, teamID, true, org.at)
}

// link seeds count issues of the team's project, each linked to its own PR
// in the repo by a work_graph_issue_pr row of the given provenance.
func (org *rankedOwnerOrg) link(provider, teamID, provenance string, count int) {
	for range count {
		org.issue++
		org.pr++
		workItemID := fmt.Sprintf("%s:%s-%d", provider, teamID, org.issue)
		seedWorkItem(org.t, org.ctx, org.conn, org.orgID, workItemID, provider, uuid.Nil, provider+"-proj-"+teamID, org.at)
		seedWorkGraphIssuePRWithProvenance(org.t, org.ctx, org.conn, org.orgID, org.repoID, workItemID, org.pr, provenance, org.at)
	}
}

func (org *rankedOwnerOrg) derive() (written, retracted int, stats TeamRepoOwnershipDerivationStats) {
	org.t.Helper()
	written, retracted, inputsReady, _, stats, err := TeamRepoOwnershipDerivationService{Conn: org.conn}.DeriveWithStats(org.ctx, org.orgID)
	if err != nil || !inputsReady {
		org.t.Fatalf("Derive: inputsReady=%v err=%v", inputsReady, err)
	}
	return written, retracted, stats
}

// openOwners reads the repo's open inferred owner rows back from ClickHouse.
func (org *rankedOwnerOrg) openOwners() []string {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT team_id
FROM team_repo_ownership FINAL
WHERE org_id = ? AND repo_full_name = ? AND source = 'inferred' AND valid_to IS NULL`, org.orgID, org.repo)
	if err != nil {
		org.t.Fatalf("read open owners: %v", err)
	}
	defer rows.Close()
	var owners []string
	for rows.Next() {
		var teamID string
		if err := rows.Scan(&teamID); err != nil {
			org.t.Fatalf("scan open owner: %v", err)
		}
		owners = append(owners, teamID)
	}
	if err := rows.Err(); err != nil {
		org.t.Fatalf("open owners rows.Err: %v", err)
	}
	sort.Strings(owners)
	return owners
}

func (org *rankedOwnerOrg) assertOpenOwners(want ...string) {
	org.t.Helper()
	if got := org.openOwners(); !reflect.DeepEqual(got, want) {
		org.t.Fatalf("open inferred owners of %s = %v, want %v", org.repo, got, want)
	}
}

var rankedOwnerIntegrationProviderPairs = [][2]string{
	{"linear", "jira"}, {"jira", "github"}, {"github", "gitlab"}, {"gitlab", "linear"},
}

// TestRankedOwnerKeepsTheNativeMajorityTeamWhenFewExplicitTextLinksArrive is
// the shape that lost a repo's owner: a team owns the repo through many
// native links, then four teams of another provider each get two
// explicit_text links to it. The owner stays, its row is not retracted, and
// no rival row is written.
func TestRankedOwnerKeepsTheNativeMajorityTeamWhenFewExplicitTextLinksArrive(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_owner_"+pair[1]+"_rivals", func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-majority-%d", index))
			org.ownProject(pair[0], "owner")
			org.link(pair[0], "owner", "native", 12)
			org.link(pair[0], "owner", "explicit_text", 2)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("owner")

			for _, rival := range []string{"rival-a", "rival-b", "rival-c", "rival-d"} {
				org.ownProject(pair[1], rival)
				org.link(pair[1], rival, "explicit_text", 2)
			}
			written, retracted, stats := org.derive()
			if written != 0 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 0 and 0 (owner unchanged)", written, retracted)
			}
			if stats.Derived != 1 || stats.Unchanged != 1 {
				t.Fatalf("second Derive stats: %+v, want derived=1 unchanged=1", stats)
			}
			org.assertOpenOwners("owner")
		})
	}
}

// TestRankedOwnerOneNativeLinkTakesTheRepoFromFiftyExplicitTextLinks: a
// count in a lower tier never outweighs a higher tier. The repo moves to the
// native team, and the old owner's row is retracted because the ranked owner
// changed.
func TestRankedOwnerOneNativeLinkTakesTheRepoFromFiftyExplicitTextLinks(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_native_over_"+pair[1]+"_text", func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-tier-%d", index))
			org.ownProject(pair[1], "text-team")
			org.link(pair[1], "text-team", "explicit_text", 50)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("text-team")

			org.ownProject(pair[0], "native-team")
			org.link(pair[0], "native-team", "native", 1)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 1 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 1 (native-team) and 1 (text-team)", written, retracted)
			}
			org.assertOpenOwners("native-team")
		})
	}
}

// TestRankedOwnerFullTieKeepsTheExistingOwnerAndSignalsTheTie: a second team
// reaches the repo with the owner's exact link counts at every tier. No
// owner is named, nothing is retracted, the existing owner row stays open,
// and the run reports the tie.
func TestRankedOwnerFullTieKeepsTheExistingOwnerAndSignalsTheTie(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for index, pair := range rankedOwnerIntegrationProviderPairs {
		t.Run(pair[0]+"_owner_ties_"+pair[1], func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, fmt.Sprintf("ranked-tie-%d", index))
			org.ownProject(pair[0], "team-a")
			org.link(pair[0], "team-a", "native", 1)
			org.link(pair[0], "team-a", "heuristic", 2)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			org.assertOpenOwners("team-a")

			org.ownProject(pair[1], "team-b")
			org.link(pair[1], "team-b", "native", 1)
			org.link(pair[1], "team-b", "heuristic", 2)
			written, retracted, stats := org.derive()
			if written != 0 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 0 and 0 (tie keeps the owner)", written, retracted)
			}
			org.assertOpenOwners("team-a")
			wantTies := []TeamRepoOwnershipTie{{RepoID: org.repoID.String(), TeamIDs: []string{"team-a", "team-b"}}}
			if !reflect.DeepEqual(stats.Ties, wantTies) {
				t.Fatalf("second Derive ties = %+v, want %+v", stats.Ties, wantTies)
			}

			org.link(pair[1], "team-b", "heuristic", 1)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 1 {
				t.Fatalf("third Derive: written=%d retracted=%d, want 1 (team-b) and 1 (team-a) once the tie breaks", written, retracted)
			}
			org.assertOpenOwners("team-b")
		})
	}
}

// TestRankedOwnerSingleTeamIsUnchangedAgainstMigratedSchema: a repo only one team reaches is
// owned by that team whatever its tier mix, and a re-run writes nothing.
func TestRankedOwnerSingleTeamIsUnchangedAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, provider := range []string{"jira", "github", "gitlab", "linear"} {
		t.Run(provider, func(t *testing.T) {
			org := newRankedOwnerOrg(t, ctx, conn, "ranked-single-"+provider)
			org.ownProject(provider, "only")
			org.link(provider, "only", "native", 2)
			org.link(provider, "only", "explicit_text", 3)
			org.link(provider, "only", "heuristic", 1)
			if written, retracted, _ := org.derive(); written != 1 || retracted != 0 {
				t.Fatalf("first Derive: written=%d retracted=%d, want 1 and 0", written, retracted)
			}
			if written, retracted, _ := org.derive(); written != 0 || retracted != 0 {
				t.Fatalf("second Derive: written=%d retracted=%d, want 0 and 0", written, retracted)
			}
			org.assertOpenOwners("only")
		})
	}
}
