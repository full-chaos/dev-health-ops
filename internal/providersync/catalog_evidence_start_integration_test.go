//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-10521: each catalog writer runs its REAL collector against real
// ClickHouse. The first run sees no evidence, so it writes every link at the
// run, as every writer did before this change. Then the evidence is synced (a
// work item a member reported, a work item of a project, a pull request of a
// repository) and the next run moves the first row of each link with evidence
// to the evidence: the row is written again at the evidence start and its old
// key is retired as an empty interval. A third run writes nothing. A link with
// no evidence keeps its start.
//
// The plant is the state of the first run (valid_from = the sync): the same
// check finds it, so the check cannot pass on a writer that stamps the sync.

// Whole seconds: a positional ? bind of a time on this driver keeps whole seconds only.
var (
	catalogEvidenceRun    = []time.Time{time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)}
	catalogEvidenceFirst  = time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	catalogEvidenceSecond = time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
)

type catalogScopeOf struct {
	table, keyColumn, provider, source string
}

var (
	githubMembershipScope = catalogScopeOf{"team_memberships", "member_id", "github", "provider_access"}
	githubRepoScope       = catalogScopeOf{"team_repo_ownership", "repo_full_name", "github", "provider_access"}
	gitlabMembershipScope = catalogScopeOf{"team_memberships", "member_id", "gitlab", "provider_access"}
	gitlabProjectScope    = catalogScopeOf{"team_project_ownership", "project_id", "gitlab", "provider_access"}
	linearMembershipScope = catalogScopeOf{"team_memberships", "member_id", "linear", "native"}
	linearProjectScope    = catalogScopeOf{"team_project_ownership", "project_id", "linear", "native"}
	jiraLegacyScope       = catalogScopeOf{"team_project_ownership", "project_id", "jira", "jira_legacy"}
)

// catalogStoredRow is one row of a writer's scope after FINAL.
type catalogStoredRow struct {
	Fact string // "<team>|<key>"
	From time.Time
	To   *time.Time
}

func (row catalogStoredRow) String() string {
	to := "open"
	if row.To != nil {
		to = row.To.UTC().Format(time.RFC3339Nano)
	}
	return row.Fact + "@" + row.From.Format(time.RFC3339Nano) + ".." + to
}

func (row catalogStoredRow) empty() bool { return row.To != nil && !row.To.After(row.From) }

func readCatalogScope(ctx context.Context, t *testing.T, conn driver.Conn, orgID string, scope catalogScopeOf) []catalogStoredRow {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT concat(team_id, '|', `+scope.keyColumn+`), valid_from, valid_to FROM `+scope.table+` FINAL
WHERE org_id = ? AND provider = ? AND source = ? ORDER BY 1, 2`, orgID, scope.provider, scope.source)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []catalogStoredRow
	for rows.Next() {
		var row catalogStoredRow
		if err := rows.Scan(&row.Fact, &row.From, &row.To); err != nil {
			t.Fatal(err)
		}
		row.From = row.From.UTC()
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func countCatalogScope(ctx context.Context, t *testing.T, conn driver.Conn, orgID string, scope catalogScopeOf) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM `+scope.table+` WHERE org_id = ? AND provider = ? AND source = ?`,
		orgID, scope.provider, scope.source).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// catalogStartViolations is the documented precedence over one scope: each fact
// of want has exactly one open row, its first row that is not an empty interval
// starts at want (its evidence, or the first sync for a fact with none), and no
// fact of the scope is missing from want.
func catalogStartViolations(rows []catalogStoredRow, want map[string]time.Time) []string {
	first := map[string]time.Time{}
	open := map[string]int{}
	for _, row := range rows {
		if row.empty() {
			continue
		}
		if from, ok := first[row.Fact]; !ok || row.From.Before(from) {
			first[row.Fact] = row.From
		}
		if row.To == nil {
			open[row.Fact]++
		}
	}
	var out []string
	for fact, from := range first {
		start, ok := want[fact]
		switch {
		case !ok:
			out = append(out, fact+": a fact the test does not know")
		case !from.Equal(start):
			out = append(out, fmt.Sprintf("%s starts at %s, want %s", fact, from.Format(time.RFC3339), start.Format(time.RFC3339)))
		}
		if open[fact] != 1 {
			out = append(out, fmt.Sprintf("%s has %d open rows, want 1", fact, open[fact]))
		}
	}
	for fact := range want {
		if _, ok := first[fact]; !ok {
			out = append(out, fact+": no row")
		}
	}
	sort.Strings(out)
	return out
}

// catalogEvidenceWant is the start each fact of the first run must have after
// the evidence is synced: the evidence of the facts it names, else the start
// the first run gave the fact.
func catalogEvidenceWant(t *testing.T, label string, firstRun []catalogStoredRow, evidence func(fact string) (time.Time, bool)) map[string]time.Time {
	t.Helper()
	want := map[string]time.Time{}
	moved := 0
	for _, row := range firstRun {
		if row.empty() {
			continue
		}
		want[row.Fact] = row.From
		if from, ok := evidence(row.Fact); ok {
			want[row.Fact] = from
			moved++
		}
	}
	if moved == 0 || moved == len(want) {
		t.Fatalf("%s: %d of %d facts have evidence (%v): the test needs a fact with evidence and one without", label, moved, len(want), firstRun)
	}
	return want
}

// requireCatalogEvidenceStart holds the whole contract over one scope after the
// evidence run: the plant (the first run's rows) is red, the produced rows are
// green, and the old key of every moved fact is retired as an empty interval at
// its own start.
func requireCatalogEvidenceStart(t *testing.T, label string, firstRun, afterEvidence []catalogStoredRow, want map[string]time.Time) {
	t.Helper()
	if planted := catalogStartViolations(firstRun, want); len(planted) == 0 {
		t.Fatalf("%s: the check does not find the sync-time start of the first run (the plant): %v", label, firstRun)
	}
	if violations := catalogStartViolations(afterEvidence, want); len(violations) > 0 {
		t.Fatalf("%s: after the evidence run:\n  %s\nrows %v", label, strings.Join(violations, "\n  "), afterEvidence)
	}
	retired := map[string]bool{}
	for _, row := range afterEvidence {
		if row.empty() {
			retired[row.Fact+"|"+row.From.Format(time.RFC3339)] = true
		}
	}
	for _, row := range firstRun {
		if !row.From.Equal(want[row.Fact]) && !retired[row.Fact+"|"+row.From.Format(time.RFC3339)] {
			t.Errorf("%s: the old key %s at %s is not retired as an empty interval", label, row.Fact, row.From.Format(time.RFC3339))
		}
	}
}

// requireCatalogRepeatWritesNothing: the run after the evidence run adds no key
// and retires nothing; a scope whose writer skips an unchanged row (ownership)
// also adds no physical row.
func requireCatalogRepeatWritesNothing(ctx context.Context, t *testing.T, conn driver.Conn, orgID, label string, scope catalogScopeOf,
	before []catalogStoredRow, physicalBefore uint64, after []catalogStoredRow,
) {
	t.Helper()
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("%s: a repeat run changed the rows:\n  before %v\n  after  %v", label, before, after)
	}
	if scope.table != "team_memberships" {
		if physical := countCatalogScope(ctx, t, conn, orgID, scope); physical != physicalBefore {
			t.Errorf("%s: a repeat run wrote %d row(s), want none", label, int64(physical)-int64(physicalBefore))
		}
	}
}

func insertEvidenceWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID, provider, projectID, reporter string, createdAt time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, reporter, assignees, created_at, org_id, last_synced)
VALUES (?, ?, ?, 'issue', ?, ?, [], ?, ?, now64(3))`, uuid.Nil, workItemID, provider, projectID, reporter, createdAt, orgID); err != nil {
		t.Fatal(err)
	}
}

func factKeyOf(fact string) string {
	_, key, _ := strings.Cut(fact, "|")
	return key
}

func TestGitHubCatalogStartsAtTheEvidence(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-10521-github"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time) {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
			"/orgs/acme/teams/platform/repos":   `[{"name":"api"},{"name":"web"}]`,
			"/orgs/acme/teams/platform/members": `[{"login":"octocat"},{"login":"hubot"}]`,
		}}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	sync(catalogEvidenceRun[0])
	members := readCatalogScope(ctx, t, conn, orgID, githubMembershipScope)
	repos := readCatalogScope(ctx, t, conn, orgID, githubRepoScope)

	// octocat reported an issue; the first repository has pull requests (the earliest wins), the second none.
	insertEvidenceWorkItem(t, ctx, conn, orgID, "gh:acme/api#9", "github", "acme/api", "github:octocat", catalogEvidenceFirst)
	insertEvidenceWorkItem(t, ctx, conn, orgID, "gh:acme/api#10", "github", "acme/api", "github:octocat", catalogEvidenceSecond)
	var api string
	for _, row := range repos {
		if strings.HasSuffix(factKeyOf(row.Fact), "/api") {
			api = factKeyOf(row.Fact)
		}
	}
	if api == "" {
		t.Fatalf("the first run wrote no grant of the api repository: %v", repos)
	}
	repoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("10521:"+api))
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', now64(3))`, repoID, api, orgID); err != nil {
		t.Fatal(err)
	}
	insertPullRequest(t, ctx, conn, orgID, repoID, 2, catalogEvidenceSecond.Add(48*time.Hour))
	insertPullRequest(t, ctx, conn, orgID, repoID, 1, catalogEvidenceSecond)

	wantMembers := catalogEvidenceWant(t, "github memberships", members, func(fact string) (time.Time, bool) {
		return catalogEvidenceFirst, strings.HasSuffix(factKeyOf(fact), ":octocat")
	})
	wantRepos := catalogEvidenceWant(t, "github grants", repos, func(fact string) (time.Time, bool) {
		return catalogEvidenceSecond, factKeyOf(fact) == api
	})
	sync(catalogEvidenceRun[1])
	afterMembers := readCatalogScope(ctx, t, conn, orgID, githubMembershipScope)
	afterRepos := readCatalogScope(ctx, t, conn, orgID, githubRepoScope)
	requireCatalogEvidenceStart(t, "github memberships", members, afterMembers, wantMembers)
	requireCatalogEvidenceStart(t, "github grants", repos, afterRepos, wantRepos)

	physical := countCatalogScope(ctx, t, conn, orgID, githubRepoScope)
	sync(catalogEvidenceRun[2])
	requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "github memberships", githubMembershipScope, afterMembers, 0,
		readCatalogScope(ctx, t, conn, orgID, githubMembershipScope))
	requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "github grants", githubRepoScope, afterRepos, physical,
		readCatalogScope(ctx, t, conn, orgID, githubRepoScope))
}

func TestGitLabCatalogStartsAtTheEvidence(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)

	t.Run("memberships", func(t *testing.T) {
		const orgID = "org-10521-gitlab-members"
		fake := newGitLabMembersServer(t, "alice", "carol")
		credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
		sync := func(at time.Time) {
			t.Helper()
			collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
				Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			}, ScopeCensus: staticScopeCensus{}}
			ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"}
			if _, err := collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL),
				TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
				t.Fatalf("sync at %s: %v", at, err)
			}
		}
		sync(catalogEvidenceRun[0])
		members := readCatalogScope(ctx, t, conn, orgID, gitlabMembershipScope)
		insertEvidenceWorkItem(t, ctx, conn, orgID, "gitlab:org/api#1", "gitlab", "", "gitlab:alice", catalogEvidenceFirst)
		want := catalogEvidenceWant(t, "gitlab memberships", members, func(fact string) (time.Time, bool) {
			return catalogEvidenceFirst, strings.HasSuffix(factKeyOf(fact), ":alice")
		})
		sync(catalogEvidenceRun[1])
		after := readCatalogScope(ctx, t, conn, orgID, gitlabMembershipScope)
		requireCatalogEvidenceStart(t, "gitlab memberships", members, after, want)
		sync(catalogEvidenceRun[2])
		requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "gitlab memberships", gitlabMembershipScope, after, 0,
			readCatalogScope(ctx, t, conn, orgID, gitlabMembershipScope))
	})

	t.Run("project grants", func(t *testing.T) {
		org, fake := "org-10521-gitlab-projects", newGitLabSnapshotServer(t)
		fake.set("", "", []string{"org/team-a"}, map[string][]string{"org": {"org/root-svc"}, "org/team-a": {"org/team-a/api"}})
		projectsOnly := TeamCatalogSelections{Projects: true}
		sync := func(at time.Time) {
			t.Helper()
			if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, projectsOnly, false, at); err != nil {
				t.Fatal(err)
			}
		}
		sync(catalogEvidenceRun[0])
		grants := readCatalogScope(ctx, t, conn, org, gitlabProjectScope)
		api := org + ":gitlab:" + testGitLabNativeProjectID("org/team-a/api")
		insertEvidenceWorkItem(t, ctx, conn, org, "gitlab:org/team-a/api#1", "gitlab", api, "", catalogEvidenceSecond)
		insertEvidenceWorkItem(t, ctx, conn, org, "gitlab:org/team-a/api#2", "gitlab", api, "", catalogEvidenceSecond.Add(time.Hour))
		want := catalogEvidenceWant(t, "gitlab grants", grants, func(fact string) (time.Time, bool) {
			return catalogEvidenceSecond, factKeyOf(fact) == api
		})
		sync(catalogEvidenceRun[1])
		after := readCatalogScope(ctx, t, conn, org, gitlabProjectScope)
		requireCatalogEvidenceStart(t, "gitlab grants", grants, after, want)
		physical := countCatalogScope(ctx, t, conn, org, gitlabProjectScope)
		sync(catalogEvidenceRun[2])
		requireCatalogRepeatWritesNothing(ctx, t, conn, org, "gitlab grants", gitlabProjectScope, after, physical,
			readCatalogScope(ctx, t, conn, org, gitlabProjectScope))
	})
}

func TestLinearCatalogStartsAtTheEvidence(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	sync := func(at time.Time) {
		t.Helper()
		doer := &linearWorkItemsDoer{responses: []string{
			`{"data":{"teams":{"nodes":[` +
				`{"id":"team-raw-1","key":"ENG","name":"Provider Eng","members":{"nodes":[` + alice + `,` + bob + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
				`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"projects":{"nodes":[` + linearProjectNodeJSON("proj-1", "ENG") + `,` + linearProjectNodeJSON("proj-2", "ENG") +
				`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		}}
		collector := LinearTeamCatalogCollector{
			ScopeCensus: staticScopeCensus{},
			Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10, Now: func() time.Time { return at }},
			Sink:        LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: carrySeamLease()},
		}
		claim := nativeTestClaim("linear", "work-items")
		claim.OrgID = orgID
		ref := teamCatalogRefFromClaim(claim)
		ref.Strict = true
		if _, err := collector.CollectTeamCatalog(ctx, ref,
			providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}, linearWorkItemsClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	sync(catalogEvidenceRun[0])
	members := readCatalogScope(ctx, t, conn, orgID, linearMembershipScope)
	projects := readCatalogScope(ctx, t, conn, orgID, linearProjectScope)
	// alice reported an item of proj-1; bob and proj-2 have none.
	insertEvidenceWorkItem(t, ctx, conn, orgID, "linear:ENG-1", "linear", "proj-1", "alice@example.com", catalogEvidenceFirst)
	wantMembers := catalogEvidenceWant(t, "linear memberships", members, func(fact string) (time.Time, bool) {
		return catalogEvidenceFirst, strings.Contains(factKeyOf(fact), "alice@example.com")
	})
	wantProjects := catalogEvidenceWant(t, "linear projects", projects, func(fact string) (time.Time, bool) {
		return catalogEvidenceFirst, factKeyOf(fact) == "proj-1"
	})
	sync(catalogEvidenceRun[1])
	afterMembers := readCatalogScope(ctx, t, conn, orgID, linearMembershipScope)
	afterProjects := readCatalogScope(ctx, t, conn, orgID, linearProjectScope)
	requireCatalogEvidenceStart(t, "linear memberships", members, afterMembers, wantMembers)
	requireCatalogEvidenceStart(t, "linear projects", projects, afterProjects, wantProjects)
	physical := countCatalogScope(ctx, t, conn, orgID, linearProjectScope)
	sync(catalogEvidenceRun[2])
	requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "linear memberships", linearMembershipScope, afterMembers, 0,
		readCatalogScope(ctx, t, conn, orgID, linearMembershipScope))
	requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "linear projects", linearProjectScope, afterProjects, physical,
		readCatalogScope(ctx, t, conn, orgID, linearProjectScope))
}

// TestJiraLegacyLinksStartAtTheEvidence holds an open and a CLOSED first row:
// K000's link is removed and its row closed by the run before the evidence is
// synced, and the evidence run moves that closed row (its end kept) as it moves
// K001's open row. K002 has no evidence and keeps its start.
func TestJiraLegacyLinksStartAtTheEvidence(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	keys := []string{"K000", "K001", "K002"}
	for _, key := range keys {
		if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
			orgID, key, "ops-team-a", "Project "+key, "Ops Team", catalogEvidenceRun[0].Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	server := &jiraProjectServer{projects: keys}
	jiraProjectRun(ctx, t, conn, orgID, server, catalogEvidenceRun[0])
	links := readCatalogScope(ctx, t, conn, orgID, jiraLegacyScope)
	if err := conn.Exec(ctx, `ALTER TABLE jira_project_ops_team_links DELETE WHERE org_id = ? AND project_key = 'K000' SETTINGS mutations_sync = 2`, orgID); err != nil {
		t.Fatal(err)
	}
	closedAt := catalogEvidenceRun[0].Add(30 * time.Minute)
	jiraProjectRun(ctx, t, conn, orgID, server, closedAt)
	closedRun := readCatalogScope(ctx, t, conn, orgID, jiraLegacyScope)
	closedFirst, openFirst := jiraProjectNativeID("K000"), jiraProjectNativeID("K001")
	closed := false
	for _, row := range closedRun {
		closed = closed || (factKeyOf(row.Fact) == closedFirst && row.To != nil && row.To.Equal(closedAt))
	}
	if !closed {
		t.Fatalf("the removed link's row is not closed by the run: the closed first-row case is not set: %v", closedRun)
	}
	insertEvidenceWorkItem(t, ctx, conn, orgID, "jira:K000-1", "jira", closedFirst, "", catalogEvidenceFirst)
	insertEvidenceWorkItem(t, ctx, conn, orgID, "jira:K001-1", "jira", openFirst, "", catalogEvidenceSecond)
	want := catalogEvidenceWant(t, "jira legacy links", links, func(fact string) (time.Time, bool) {
		switch factKeyOf(fact) {
		case closedFirst:
			return catalogEvidenceFirst, true
		case openFirst:
			return catalogEvidenceSecond, true
		}
		return time.Time{}, false
	})
	jiraProjectRun(ctx, t, conn, orgID, server, catalogEvidenceRun[1])
	after := readCatalogScope(ctx, t, conn, orgID, jiraLegacyScope)
	for _, row := range after {
		if factKeyOf(row.Fact) == closedFirst && !row.empty() && (row.To == nil || !row.To.Equal(closedAt) || !row.From.Equal(catalogEvidenceFirst)) {
			t.Fatalf("the closed first row is %s, want [%s, %s): moved to the evidence with its end kept", row, catalogEvidenceFirst, closedAt)
		}
	}
	// The closed link has no open row: the check counts open rows of the open links only.
	open := map[string]time.Time{}
	var openLinks []catalogStoredRow
	for _, row := range after {
		if factKeyOf(row.Fact) != closedFirst {
			openLinks = append(openLinks, row)
		}
	}
	for fact, from := range want {
		if factKeyOf(fact) != closedFirst {
			open[fact] = from
		}
	}
	var firstOpen []catalogStoredRow
	for _, row := range links {
		if factKeyOf(row.Fact) != closedFirst {
			firstOpen = append(firstOpen, row)
		}
	}
	requireCatalogEvidenceStart(t, "jira legacy links", firstOpen, openLinks, open)
	retired := false
	for _, row := range after {
		retired = retired || (factKeyOf(row.Fact) == closedFirst && row.empty() && row.From.Equal(catalogEvidenceRun[0]))
	}
	if !retired {
		t.Errorf("the closed first row's old key is not retired as an empty interval: %v", after)
	}
	physical := countCatalogScope(ctx, t, conn, orgID, jiraLegacyScope)
	jiraProjectRun(ctx, t, conn, orgID, server, catalogEvidenceRun[2])
	requireCatalogRepeatWritesNothing(ctx, t, conn, orgID, "jira legacy links", jiraLegacyScope, after, physical,
		readCatalogScope(ctx, t, conn, orgID, jiraLegacyScope))
}

// catalogEvidenceFailingConn fails every read of work_items: the evidence read.
type catalogEvidenceFailingConn struct {
	driver.Conn
}

func (conn catalogEvidenceFailingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM work_items") {
		return nil, fmt.Errorf("simulated evidence read failure")
	}
	return conn.Conn.Query(ctx, query, args...)
}

// TestAFailedEvidenceReadKeepsTheStartAndTheSync: a writer whose evidence read
// fails writes as before (the open row keeps its start, a new link starts at
// the run), logs catalog_evidence_start_skipped and does not fail.
func TestAFailedEvidenceReadKeepsTheStartAndTheSync(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-10521-failed-read"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time, members string) {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
			"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
			"/orgs/acme/teams/platform/members": members,
		}}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: catalogEvidenceFailingConn{Conn: conn}}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("a failed evidence read failed the sync at %s: %v", at, err)
		}
	}
	sync(catalogEvidenceRun[0], `[{"login":"octocat"}]`)
	insertEvidenceWorkItem(t, ctx, conn, orgID, "gh:acme/api#9", "github", "acme/api", "github:octocat", catalogEvidenceFirst)
	insertEvidenceWorkItem(t, ctx, conn, orgID, "gh:acme/api#10", "github", "acme/api", "github:hubot", catalogEvidenceFirst)
	warnings := captureWarnings(t, func() { sync(catalogEvidenceRun[1], `[{"login":"octocat"},{"login":"hubot"}]`) })
	got := readCatalogScope(ctx, t, conn, orgID, githubMembershipScope)
	want := map[string]time.Time{"gh:platform|gh:octocat": catalogEvidenceRun[0], "gh:platform|gh:hubot": catalogEvidenceRun[1]}
	if violations := catalogStartViolations(got, want); len(violations) > 0 {
		t.Fatalf("after a failed evidence read:\n  %s", strings.Join(violations, "\n  "))
	}
	skipped := 0
	for _, entry := range warnings {
		if entry["msg"] == "catalog_evidence_start_skipped" && entry["reason"] == catalogEvidenceSkipEvidenceRead {
			skipped++
		}
	}
	if skipped == 0 {
		t.Fatalf("no catalog_evidence_start_skipped WARN for the failed read: %v", warnings)
	}
}

// captureWarnings runs fn with the default logger writing JSON and returns every
// WARN line it logged.
func captureWarnings(t *testing.T, fn func()) []map[string]any {
	t.Helper()
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, nil)))
	defer slog.SetDefault(previous)
	fn()
	var out []map[string]any
	for _, raw := range strings.Split(logged.String(), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(raw), &entry) == nil && entry["level"] == "WARN" {
			out = append(out, entry)
		}
	}
	return out
}
