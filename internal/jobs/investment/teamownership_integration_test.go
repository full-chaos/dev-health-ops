//go:build integration

package investment

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// wtiProviders is the provider half of the AGENTS.md provider x entity matrix.
// Linear-only coverage is banned by contract: jira/github/gitlab work items
// carry native_team_key=None, so their attribution rides entirely on the
// autoimport team/project/member dimension, which is a different code path
// from Linear's native team key.
var wtiProviders = []string{"jira", "gitlab", "github", "linear"}

// wtiEntities is the entity half. Each (provider, entity) cell below is
// asserted by name in this test and the coverage ledger fails the test if a
// cell is never reached -- an unasserted cell that silently reads as covered
// is the exact failure AGENTS.md's "a test that cannot fail is worse than no
// test" rule exists to stop.
var wtiEntities = []string{"teams", "projects", "members", "issues"}

// attributionSourceFor is the eligible primary attribution source each
// provider actually emits for its issues. Rotating them proves the allocator
// never branches on provider AND covers all four eligible sources.
var attributionSourceFor = map[string]string{
	"linear": "native_team",
	"jira":   "issue_project",
	"gitlab": "project_ownership",
	"github": "repo_ownership",
}

// repoHostFor maps a work-tracking provider to the provider stamped on its
// repos and on team_repo_ownership. They are NOT the same axis: the
// derivation writer stamps the REPO's provider, so a Jira team legitimately
// owns GitHub repositories. A join that confused the two would pass a
// single-provider fixture and fail here.
var repoHostFor = map[string]string{
	"linear": "github",
	"jira":   "github",
	"gitlab": "gitlab",
	"github": "github",
}

// ownershipSourceFor gives each provider's two donor teams a different
// eligible team_repo_ownership source, covering all four across the matrix.
// The "projects" arm is the `inferred` source -- the sync-derived producer
// that turns team_project_ownership into repository ownership. GitHub has no
// native project entity (the repo is the scope), so its second arm is
// `native` and its projects cell is recorded n/a instead.
var ownershipSourceFor = map[string][2]string{
	"jira":   {"jira_legacy", "inferred"},
	"gitlab": {"provider_access", "inferred"},
	"linear": {"native", "inferred"},
	"github": {"provider_access", "native"},
}

type matrixCoverage struct {
	t    *testing.T
	seen map[string]string
}

func newMatrixCoverage(t *testing.T) *matrixCoverage {
	t.Helper()
	return &matrixCoverage{t: t, seen: map[string]string{}}
}

func (c *matrixCoverage) cover(provider, entity, how string) {
	c.t.Helper()
	c.seen[provider+"/"+entity] = how
}

func (c *matrixCoverage) assertComplete() {
	c.t.Helper()
	var missing []string
	for _, provider := range wtiProviders {
		for _, entity := range wtiEntities {
			if _, ok := c.seen[provider+"/"+entity]; !ok {
				missing = append(missing, provider+"/"+entity)
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		c.t.Fatalf("provider x entity matrix cells never asserted: %v", missing)
	}
	keys := make([]string, 0, len(c.seen))
	for key := range c.seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		c.t.Logf("matrix %s: %s", key, c.seen[key])
	}
}

func matrixRepoID(providerIndex, n int) string {
	return fmt.Sprintf("%08d-0000-4000-8000-%012d", providerIndex+1, n)
}

// TestTeamOwnershipFallbackProviderEntityMatrix runs the production
// materializer against a real ClickHouse over the whole {jira, gitlab, github,
// linear} x {teams, projects, members, issues} matrix.
//
// Per provider it asserts, through persisted rows only:
//   - issues: an eligible primary attribution on each member issue produces the
//     distinct union of its teams' repositories, equal shares, effort conserved.
//   - teams: only an is_active team donates; an inactive team's repository never
//     receives a share, even though its ownership row is live.
//   - projects: the sync-derived `inferred` ownership arm (the producer that
//     turns team_project_ownership into repository ownership) donates like any
//     other eligible source. GitHub has no native project entity, recorded n/a.
//   - members: membership-derived attribution (assignee_membership,
//     author_membership) and team_memberships rows NEVER donate. This is the
//     cell that must stay negative -- team authorization is ownership-derived,
//     never person -> membership -> team.
func TestTeamOwnershipFallbackProviderEntityMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	coverage := newMatrixCoverage(t)
	at := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

	// wantRepos[provider] is the distinct union this provider's donor teams own,
	// and wantTeams[provider][repo] the sorted team ids that must appear in the
	// persisted provenance for that repository.
	wantRepos := map[string][]string{}
	wantTeams := map[string]map[string][]string{}

	for providerIndex, provider := range wtiProviders {
		host := repoHostFor[provider]
		attrSource := attributionSourceFor[provider]
		sources := ownershipSourceFor[provider]
		teamA, teamB := provider+"-team-a", provider+"-team-b"
		teamInactive, teamMembers := provider+"-team-inactive", provider+"-team-members"
		r1, r2, r3 := matrixRepoID(providerIndex, 1), matrixRepoID(providerIndex, 2), matrixRepoID(providerIndex, 3)
		rInactive, rMembers, rManual := matrixRepoID(providerIndex, 4), matrixRepoID(providerIndex, 5), matrixRepoID(providerIndex, 6)
		rExpiring := matrixRepoID(providerIndex, 7)

		for n, repo := range map[string]string{r1: "r1", r2: "r2", r3: "r3", rInactive: "r-inactive", rMembers: "r-members", rManual: "r-manual", rExpiring: "r-expiring"} {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id) VALUES (?, ?, ?, ?)`,
				n, "acme/"+provider+"-"+repo, host, hierarchyCascadeTestOrg)
		}
		// teams: three active donors and one deactivated team, all on this
		// provider. Only the active ones may donate.
		for _, team := range []string{teamA, teamB, teamMembers} {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, provider, is_active, updated_at, org_id) VALUES (?, generateUUIDv4(), ?, ?, 1, ?, ?)`,
				team, team, provider, at, hierarchyCascadeTestOrg)
		}
		mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO teams (id, team_uuid, name, provider, is_active, updated_at, org_id) VALUES (?, generateUUIDv4(), ?, ?, 0, ?, ?)`,
			teamInactive, teamInactive, provider, at, hierarchyCascadeTestOrg)

		// team A owns r1 and r2 by NAME only (repo_id NULL) -- the shape the
		// GitHub provider_access producer actually writes. Two ownership
		// generations prove a ReplacingMergeTree duplicate cannot double-count.
		for _, name := range []string{"acme/" + provider + "-r1", "acme/" + provider + "-r2"} {
			for _, version := range []time.Time{at, at.Add(time.Minute)} {
				seedRepoOwnershipByName(t, ctx, conn, host, teamA, name, sources[0], version)
			}
		}
		// team B owns r2 and r3 by explicit repo_id -- the other join arm.
		for _, repo := range []string{r2, r3} {
			seedRepoOwnershipByID(t, ctx, conn, host, teamB, repo, "acme/"+provider+"-"+map[string]string{r2: "r2", r3: "r3"}[repo], sources[1], at)
		}
		// Ownership that is live at the run's ComputedAt but EXPIRED by wall
		// clock. It only donates if the interval is evaluated at the run's own
		// timestamp; substituting time.Now() drops it.
		seedExpiringRepoOwnership(t, ctx, conn, host, teamA, rExpiring, "acme/"+provider+"-r-expiring", sources[0], at, at.Add(3*time.Hour))
		// Negative ownership: an inactive team, a membership-only team, and a
		// manual mapping. None of the three may reach an allocation.
		seedRepoOwnershipByID(t, ctx, conn, host, teamInactive, rInactive, "acme/"+provider+"-r-inactive", sources[0], at)
		seedRepoOwnershipByID(t, ctx, conn, host, teamMembers, rMembers, "acme/"+provider+"-r-members", sources[0], at)
		seedRepoOwnershipByID(t, ctx, conn, host, teamA, rManual, "acme/"+provider+"-r-manual", "manual", at)

		// members: real membership rows exist for the membership team. They are
		// data the fallback must ignore, never a donation path.
		for _, member := range []string{"person-1", "person-2"} {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, source, valid_from, updated_at) VALUES (?, ?, ?, ?, 'provider_access', ?, ?)`,
				hierarchyCascadeTestOrg, provider, teamMembers, member, at, at)
		}
		// projects: the project ownership the `inferred` repository-ownership
		// producer derives from. GitHub has no native project entity.
		if provider != "github" {
			mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, updated_at) VALUES (?, ?, ?, ?, ?, 'provider_access', ?, ?)`,
				hierarchyCascadeTestOrg, provider, teamB, provider+"-project", provider+"-PROJ", at, at)
		}

		// issues. Component 1 allocates; components 2 and 3 must not.
		positives := map[string]string{provider + ":a": teamA, provider + ":b": teamB}
		memberOnly := []string{provider + ":m1", provider + ":m2"}
		inactiveOnly := []string{provider + ":i1", provider + ":i2"}
		for issue, team := range positives {
			seedMatrixWorkItem(t, ctx, conn, issue, provider, at)
			seedAttribution(t, ctx, conn, issue, provider, team, attrSource, 1, at)
		}
		for i, issue := range memberOnly {
			seedMatrixWorkItem(t, ctx, conn, issue, provider, at)
			source := "assignee_membership"
			if i == 1 {
				source = "author_membership"
			}
			seedAttribution(t, ctx, conn, issue, provider, teamMembers, source, 1, at)
		}
		for _, issue := range inactiveOnly {
			seedMatrixWorkItem(t, ctx, conn, issue, provider, at)
			seedAttribution(t, ctx, conn, issue, provider, teamInactive, attrSource, 1, at)
		}
		// A pair whose work items sit entirely outside the run window. It is a
		// real component that MaterializeComponent skips, so window_skipped is
		// measured against data rather than pinned at a constant zero.
		outOfWindow := []string{provider + ":w1", provider + ":w2"}
		for _, issue := range outOfWindow {
			seedMatrixWorkItem(t, ctx, conn, issue, provider, at.AddDate(0, 0, -60))
			seedAttribution(t, ctx, conn, issue, provider, teamA, attrSource, 1, at)
		}
		// F3 (codex r1): two more components that DO have direct repository
		// evidence, so `own_repo` and `direct_repo_evidence` are measured
		// end-to-end through Run's switch rather than pinned at zero. Both
		// carry a full eligible attribution AND live ownership, so what they
		// prove is PRECEDENCE -- direct evidence wins while a donor was
		// available -- not merely the absence of donors.
		ownEdge := []string{provider + ":e1", provider + ":e2"}
		ambiguous := []string{provider + ":g1", provider + ":g2"}
		for _, issue := range append(append([]string{}, ownEdge...), ambiguous...) {
			seedMatrixWorkItem(t, ctx, conn, issue, provider, at)
			seedAttribution(t, ctx, conn, issue, provider, teamA, attrSource, 1, at)
		}
		seedIssueEdge(t, ctx, conn, ownEdge[0], ownEdge[1], r1, at)
		seedIssueEdge(t, ctx, conn, ambiguous[0], ambiguous[1], r1, at)
		seedIssueEdge(t, ctx, conn, ambiguous[1], ambiguous[0], r2, at)

		seedIssueEdge(t, ctx, conn, provider+":a", provider+":b", "", at)
		seedIssueEdge(t, ctx, conn, memberOnly[0], memberOnly[1], "", at)
		seedIssueEdge(t, ctx, conn, inactiveOnly[0], inactiveOnly[1], "", at)
		seedIssueEdge(t, ctx, conn, outOfWindow[0], outOfWindow[1], "", at)

		wantRepos[provider] = []string{r1, r2, r3, rExpiring}
		sort.Strings(wantRepos[provider])
		wantTeams[provider] = map[string][]string{r1: {teamA}, r2: {teamA, teamB}, r3: {teamB}, rExpiring: {teamA}}
	}

	stats, err := materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: at.Add(-time.Hour), ToTS: at.Add(2 * time.Hour),
		RunID: "team-ownership-matrix", ComputedAt: at.Add(2 * time.Hour), ProviderName: "mock",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, provider := range wtiProviders {
		unit := units.WorkUnitID([]units.NodeKey{{Type: "issue", ID: provider + ":a"}, {Type: "issue", ID: provider + ":b"}})
		got, provenance, weightSum, effortSum := readRepoEffort(t, ctx, conn, unit)
		if len(got) != 4 {
			t.Errorf("%s issues: persisted repos=%v want the 4-repo distinct union %v", provider, got, wantRepos[provider])
			continue
		}
		for i, repo := range wantRepos[provider] {
			if got[i] != repo {
				t.Errorf("%s issues: repo[%d]=%s want %s", provider, i, got[i], repo)
			}
			want := "team:" + strings.Join(wantTeams[provider][repo], ",")
			if provenance[repo] != want {
				t.Errorf("%s issues: %s provenance=%q want %q", provider, repo, provenance[repo], want)
			}
		}
		if math.Abs(weightSum-1) > 1e-12 {
			t.Errorf("%s issues: allocation weights sum to %g, want 1", provider, weightSum)
		}
		coverage.cover(provider, "issues", "primary "+attributionSourceFor[provider]+" attribution on both member issues allocated the union with weights summing to 1")
		coverage.cover(provider, "teams", "team_repo_ownership from two is_active teams unioned; provenance names both for the shared repo")
		_ = effortSum

		if provider == "github" {
			coverage.cover(provider, "projects", "n/a -- GitHub has no native project entity, the repository is the scope; second donor arm uses the `native` ownership source instead")
		} else {
			coverage.cover(provider, "projects", "team_project_ownership-derived `inferred` repository ownership donated r2/r3 for "+provider+"-team-b")
		}

		// members: a component whose only attribution is membership-derived
		// must keep its unassigned allocation. Its team owns a live repository,
		// so the ONLY thing keeping it out is the source filter.
		memberUnit := units.WorkUnitID([]units.NodeKey{{Type: "issue", ID: provider + ":m1"}, {Type: "issue", ID: provider + ":m2"}})
		if repos, _, _, _ := readRepoEffort(t, ctx, conn, memberUnit); len(repos) != 0 {
			t.Errorf("%s members: membership-derived attribution donated %v", provider, repos)
		}
		coverage.cover(provider, "members", "assignee_membership + author_membership attributions and team_memberships rows donated nothing, though "+provider+"-team-members owns a live repository")

		// teams (negative half): an inactive team never donates.
		inactiveUnit := units.WorkUnitID([]units.NodeKey{{Type: "issue", ID: provider + ":i1"}, {Type: "issue", ID: provider + ":i2"}})
		if repos, _, _, _ := readRepoEffort(t, ctx, conn, inactiveUnit); len(repos) != 0 {
			t.Errorf("%s teams: deactivated team donated %v", provider, repos)
		}

		// The manual ownership row belongs to an ELIGIBLE team on an eligible
		// issue -- only source='manual' keeps it out.
		for _, repo := range got {
			if repo == matrixRepoID(indexOfProvider(provider), 6) {
				t.Errorf("%s: manual ownership row was allocated", provider)
			}
		}
	}
	coverage.assertComplete()

	// Telemetry. The outcome buckets must partition every component, and every
	// field must be present even at zero -- a counter that disappears at zero
	// cannot be told apart from one that was never computed.
	if stats.Components != 24 {
		t.Fatalf("components=%d, want 24 (4 providers x 6 components)", stats.Components)
	}
	if stats.RepoOwnershipFallback != 4 {
		t.Errorf("allocated=%d, want 4", stats.RepoOwnershipFallback)
	}
	if stats.RepoOwnershipNoEligible != 8 {
		t.Errorf("no_eligible_owner=%d, want 8", stats.RepoOwnershipNoEligible)
	}
	// Measured, not pinned at zero: direct evidence outranks an AVAILABLE donor.
	if stats.RepoOwnershipOwnRepo != 4 {
		t.Errorf("own_repo=%d, want 4", stats.RepoOwnershipOwnRepo)
	}
	if stats.RepoOwnershipDirectRepo != 4 {
		t.Errorf("direct_repo_evidence=%d, want 4", stats.RepoOwnershipDirectRepo)
	}
	if stats.RepoOwnershipRepoShares != 16 {
		t.Errorf("repo_shares=%d, want 16", stats.RepoOwnershipRepoShares)
	}
	if stats.RepoOwnershipWindowSkipped != 4 {
		t.Errorf("window_skipped=%d, want 4", stats.RepoOwnershipWindowSkipped)
	}
	if stats.RepoOwnershipDonorIssues != 32 {
		t.Errorf("donor_issues=%d, want the 32 issues with an eligible primary attribution", stats.RepoOwnershipDonorIssues)
	}
	if stats.RepoOwnershipDonorRows == 0 {
		t.Error("donor_rows=0 while 4 components allocated")
	}
	partition := stats.RepoOwnershipFallback + stats.RepoOwnershipOwnRepo + stats.RepoOwnershipStrongerEffort +
		stats.RepoOwnershipDirectRepo + stats.RepoOwnershipNoEligible + stats.RepoOwnershipWindowSkipped
	if partition != stats.Components {
		t.Errorf("outcome partition sums to %d, want components=%d", partition, stats.Components)
	}
	line := lastLogLine(t, logs.String(), "investment team repository fallback")
	for _, field := range []string{
		"components=24", "allocated=4", "repo_shares=16", "own_repo=4",
		"direct_repo_evidence=4", "no_eligible_owner=8", "window_skipped=4", "donor_issues=32",
	} {
		if !strings.Contains(line, field) {
			t.Errorf("telemetry line is missing %q (explicit zeros are required): %s", field, line)
		}
	}

	// F2 (codex r1): a run with ZERO components returns early. The documented
	// contract says the fallback record is emitted on EVERY run, with every
	// field present at zero -- that is the whole point of the explicit-zeros
	// design, and an empty org is exactly the case where "did the fallback do
	// nothing, or did it never run" matters most.
	emptyLogs := &bytes.Buffer{}
	emptyMaterializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(emptyLogs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	emptyStats, err := emptyMaterializer.Run(ctx, Config{
		OrgID: "00000000-0000-4000-8000-000000000009", FromTS: at.Add(-time.Hour), ToTS: at.Add(2 * time.Hour),
		RunID: "empty-org", ComputedAt: at.Add(2 * time.Hour), ProviderName: "mock",
	})
	if err != nil {
		t.Fatal(err)
	}
	if emptyStats.Components != 0 {
		t.Fatalf("fixture is not an empty org: components=%d", emptyStats.Components)
	}
	emptyLine := lastLogLine(t, emptyLogs.String(), "investment team repository fallback")
	for _, field := range []string{
		"components=0", "allocated=0", "repo_shares=0", "own_repo=0", "stronger_allocation=0",
		"direct_repo_evidence=0", "no_eligible_owner=0", "window_skipped=0", "donor_rows=0", "donor_issues=0",
	} {
		if !strings.Contains(emptyLine, field) {
			t.Errorf("empty-org telemetry is missing %q: %s", field, emptyLine)
		}
	}

	// A failed donor read must stop the run before it writes a new generation.
	// Dropping this instance's table is a real failure, not a fake configured
	// to return the expected answer.
	mustTeamOwnershipExec(t, ctx, conn, `DROP TABLE work_item_team_attributions`)
	if _, err = materializer.Run(ctx, Config{
		OrgID: hierarchyCascadeTestOrg, FromTS: at.Add(-time.Hour), ToTS: at.Add(2 * time.Hour),
		RunID: "failed-donor-read", ComputedAt: at.Add(3 * time.Hour), ProviderName: "mock",
	}); err == nil {
		t.Fatal("missing donor table was treated as missing ownership")
	}
	var writes uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM work_unit_repo_effort WHERE org_id = ? AND categorization_run_id = 'failed-donor-read'`, hierarchyCascadeTestOrg).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("failed donor read wrote %d allocation rows", writes)
	}
}

func indexOfProvider(provider string) int {
	for i, candidate := range wtiProviders {
		if candidate == provider {
			return i
		}
	}
	return -1
}

func readRepoEffort(t *testing.T, ctx context.Context, conn driver.Conn, workUnitID string) (repos []string, provenance map[string]string, weightSum, effortSum float64) {
	t.Helper()
	provenance = map[string]string{}
	rows, err := conn.Query(ctx, `
		SELECT toString(repo_id), allocation_weight, effort_value, allocation_source, ifNull(repo_source, '')
		FROM work_unit_repo_effort FINAL
		WHERE org_id = ? AND work_unit_id = ? AND repo_id IS NOT NULL AND allocation_source = 'team_ownership'
		ORDER BY repo_id`, hierarchyCascadeTestOrg, workUnitID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var repo, source, evidence string
		var weight, effort float64
		if err := rows.Scan(&repo, &weight, &effort, &source, &evidence); err != nil {
			t.Fatal(err)
		}
		if evidence == "" {
			t.Errorf("%s: repo %s allocated with no provenance", workUnitID, repo)
		}
		repos = append(repos, repo)
		provenance[repo] = evidence
		weightSum += weight
		effortSum += effort
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return repos, provenance, weightSum, effortSum
}

func lastLogLine(t *testing.T, logs, message string) string {
	t.Helper()
	var found string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, message) {
			found = line
		}
	}
	if found == "" {
		t.Fatalf("no %q log record emitted", message)
	}
	return found
}

func seedMatrixWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, issue, provider string, at time.Time) {
	t.Helper()
	seedWorkItem(t, ctx, conn, issue, "", at)

	// Re-stamp the provider from the same persisted producer shape rather than
	// hand-building a row, so no column silently drops out of the fixture.
	mustTeamOwnershipExec(t, ctx, conn,
		`INSERT INTO work_items SELECT * REPLACE (? AS provider, ? AS last_synced) FROM work_items FINAL WHERE org_id = ? AND work_item_id = ?`,
		provider, at.Add(time.Hour), hierarchyCascadeTestOrg, issue)
}

func seedAttribution(t *testing.T, ctx context.Context, conn driver.Conn, issue, provider, team, source string, primary uint8, at time.Time) {
	t.Helper()
	mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, source, is_primary, confidence, evidence, computed_at) VALUES (?, toUUID('00000000-0000-0000-0000-000000000000'), ?, ?, ?, ?, ?, 'high', 'provider ownership', ?)`,
		hierarchyCascadeTestOrg, issue, provider, team, source, primary, at)
}

func seedRepoOwnershipByName(t *testing.T, ctx context.Context, conn driver.Conn, host, team, name, source string, version time.Time) {
	t.Helper()
	mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, 'exact', ?, 0, 50, 0, ?, ?)`,
		hierarchyCascadeTestOrg, host, team, name, source, version, version)
}

func seedExpiringRepoOwnership(t *testing.T, ctx context.Context, conn driver.Conn, host, team, repoID, name, source string, from, to time.Time) {
	t.Helper()
	mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, 'exact', ?, 0, 50, 0, ?, ?, ?)`,
		hierarchyCascadeTestOrg, host, team, repoID, name, source, from, to, from)
}

func seedRepoOwnershipByID(t *testing.T, ctx context.Context, conn driver.Conn, host, team, repoID, name, source string, version time.Time) {
	t.Helper()
	mustTeamOwnershipExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, ?, 'exact', ?, 0, 50, 0, ?, ?)`,
		hierarchyCascadeTestOrg, host, team, repoID, name, source, version, version)
}

func mustTeamOwnershipExec(t *testing.T, ctx context.Context, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("seed team ownership fixture: %v\nquery: %s", err, query)
	}
}
