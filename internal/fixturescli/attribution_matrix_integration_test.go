//go:build integration

package fixturescli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The cross-provider attribution matrix (CHAOS-9425): every provider world of `fixtures generate` is loaded
// into one ClickHouse at the migration head, the production attribution verbs run over it in production
// order (the inferred repository-ownership derivation, the daily work_item_attribution family for every
// day of the worlds, then the backstop), and every cell of contracts/attribution-matrix/v1/matrix.json is
// evaluated on what they wrote. A cell is one provider x entity x check:
//
//   - a cell no world exercises (no subject) FAILS as fixture_gap; it never passes;
//   - a "pass" cell fails on any violation;
//   - a "known_red" cell names its ticket and must show its violation: when it turns green it FAILS until
//     it is moved to "pass", so a fix is recorded by the matrix, never silently.
//
// Every provider x entity pair has a cell or a not_applicable entry with the rule that makes it so.

var attributionProviders = []string{"github", "gitlab", "jira", "linear"}

var attributionEntities = []string{"teams", "members", "repositories", "pull_requests", "issues", "projects"}

var providerTeamPrefix = map[string]string{"github": "gh", "gitlab": "gl", "jira": "jira", "linear": "linear"}

type matrixCell struct {
	Provider string `json:"provider"`
	Entity   string `json:"entity"`
	Check    string `json:"check"`
	State    string `json:"state"`
	Ticket   string `json:"ticket,omitempty"`
	Why      string `json:"why,omitempty"`
}

type matrixNotApplicable struct {
	Provider string `json:"provider"`
	Entity   string `json:"entity"`
	Rule     string `json:"rule"`
}

// matrixOmitted is one check of an entity that a provider's cells leave out, with the reason no world of
// that provider can judge it.
type matrixOmitted struct {
	Provider string `json:"provider"`
	Entity   string `json:"entity"`
	Check    string `json:"check"`
	Rule     string `json:"rule"`
}

type matrixContract struct {
	Version       int                   `json:"version"`
	Cells         []matrixCell          `json:"cells"`
	NotApplicable []matrixNotApplicable `json:"not_applicable"`
	Omitted       []matrixOmitted       `json:"omitted"`
}

func loadMatrixContract(t *testing.T) matrixContract {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "contracts", "attribution-matrix", "v1", "matrix.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract matrixContract
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("matrix.json: %v", err)
	}
	return contract
}

// ---- the facts the checks read, per organization ----

type ownershipRow struct {
	TeamID, Provider, RepoID, RepoName, ProjectID, Source string
	ValidFrom                                             time.Time
	ValidTo                                               *time.Time
	// IsPrimary, Specificity and Priority are the row's rank (repository and project ownership).
	IsPrimary             uint8
	Specificity, Priority int
}

func (row ownershipRow) validAt(at time.Time) bool {
	return !row.ValidFrom.After(at) && (row.ValidTo == nil || row.ValidTo.After(at))
}

type matrixItem struct {
	ID, Provider, Type, Status, ProjectID, RepoID, NativeTeamKey, Reporter string
	Assignees                                                              []string
	CreatedAt                                                              time.Time
	CompletedAt                                                            *time.Time
}

type attributionRow struct {
	TeamID, Source, Evidence string
	IsPrimary                uint8
}

type matrixLink struct {
	WorkItemID, RepoID, Provenance string
	PRNumber                       uint32
}

type matrixFacts struct {
	org         string
	now         time.Time
	teams       map[string]matrixTeam
	repoOwners  []ownershipRow
	projOwners  []ownershipRow
	memberships []ownershipRow // RepoName holds the member id
	repos       map[string]matrixRepo
	projects    map[string]string // id -> provider
	items       map[string]matrixItem
	prs         map[string]matrixPR // "<repo_id>#<number>"
	links       []matrixLink
	deps        map[string][]string // source work item -> "<target>|<relationship>"
	attribution map[string][]attributionRow
	authors     map[string]string // pull request work item -> author name
}

type matrixTeam struct {
	Provider string
	Active   bool
}

type matrixRepo struct {
	Name, Provider string
}

type matrixPR struct {
	RepoID    string
	Number    uint32
	CreatedAt time.Time
	MergedAt  *time.Time
}

func matrixQuery(ctx context.Context, t *testing.T, conn driver.Conn, query string, args []any, scan func(driver.Rows) error) {
	t.Helper()
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("%s: %v", strings.Fields(query)[0:4], err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func loadMatrixFacts(ctx context.Context, t *testing.T, conn driver.Conn, org string, now time.Time) matrixFacts {
	t.Helper()
	facts := matrixFacts{
		org: org, now: now, teams: map[string]matrixTeam{}, repos: map[string]matrixRepo{}, projects: map[string]string{},
		items: map[string]matrixItem{}, prs: map[string]matrixPR{}, deps: map[string][]string{}, attribution: map[string][]attributionRow{},
		authors: map[string]string{},
	}
	args := []any{org}
	matrixQuery(ctx, t, conn, `SELECT id, provider, is_active FROM teams FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var id, provider string
		var active uint8
		if err := rows.Scan(&id, &provider, &active); err != nil {
			return err
		}
		facts.teams[id] = matrixTeam{Provider: provider, Active: active == 1}
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT team_id, provider, ifNull(toString(repo_id), ''), repo_full_name, toString(source), valid_from, valid_to,
	is_primary, toInt64(specificity), toInt64(priority)
FROM team_repo_ownership FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var row ownershipRow
		var specificity, priority int64
		if err := rows.Scan(&row.TeamID, &row.Provider, &row.RepoID, &row.RepoName, &row.Source, &row.ValidFrom, &row.ValidTo, &row.IsPrimary, &specificity, &priority); err != nil {
			return err
		}
		row.Specificity, row.Priority = int(specificity), int(priority)
		facts.repoOwners = append(facts.repoOwners, row)
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT team_id, provider, project_id, toString(source), valid_from, valid_to,
	is_primary, toInt64(specificity), toInt64(priority)
FROM team_project_ownership FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var row ownershipRow
		var specificity, priority int64
		if err := rows.Scan(&row.TeamID, &row.Provider, &row.ProjectID, &row.Source, &row.ValidFrom, &row.ValidTo, &row.IsPrimary, &specificity, &priority); err != nil {
			return err
		}
		row.Specificity, row.Priority = int(specificity), int(priority)
		facts.projOwners = append(facts.projOwners, row)
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT team_id, provider, member_id, toString(source), valid_from, valid_to
FROM team_memberships FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var row ownershipRow
		if err := rows.Scan(&row.TeamID, &row.Provider, &row.RepoName, &row.Source, &row.ValidFrom, &row.ValidTo); err != nil {
			return err
		}
		facts.memberships = append(facts.memberships, row)
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT toString(id), repo, provider FROM repos FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var id string
		var repo matrixRepo
		if err := rows.Scan(&id, &repo.Name, &repo.Provider); err != nil {
			return err
		}
		facts.repos[id] = repo
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT id, provider FROM projects FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var id, provider string
		if err := rows.Scan(&id, &provider); err != nil {
			return err
		}
		facts.projects[id] = provider
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT work_item_id, provider, type, status, project_id, toString(repo_id), native_team_key, reporter, assignees, created_at, completed_at
FROM work_items FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var item matrixItem
		if err := rows.Scan(&item.ID, &item.Provider, &item.Type, &item.Status, &item.ProjectID, &item.RepoID, &item.NativeTeamKey,
			&item.Reporter, &item.Assignees, &item.CreatedAt, &item.CompletedAt); err != nil {
			return err
		}
		facts.items[item.ID] = item
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT toString(repo_id), number, created_at, merged_at FROM git_pull_requests FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var pr matrixPR
		if err := rows.Scan(&pr.RepoID, &pr.Number, &pr.CreatedAt, &pr.MergedAt); err != nil {
			return err
		}
		facts.prs[fmt.Sprintf("%s#%d", pr.RepoID, pr.Number)] = pr
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT w.work_item_id, p.author_name FROM work_items AS w FINAL
INNER JOIN git_pull_requests AS p FINAL ON p.org_id = w.org_id AND p.repo_id = w.repo_id
  AND toString(p.number) = splitByRegexp('[#!]', w.work_item_id)[-1]
WHERE w.org_id = ? AND w.type IN ('pr', 'pull_request', 'merge_request')`, args, func(rows driver.Rows) error {
		var item, author string
		if err := rows.Scan(&item, &author); err != nil {
			return err
		}
		facts.authors[item] = author
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT work_item_id, toString(repo_id), toUInt32(pr_number), provenance FROM work_graph_issue_pr FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var link matrixLink
		if err := rows.Scan(&link.WorkItemID, &link.RepoID, &link.PRNumber, &link.Provenance); err != nil {
			return err
		}
		facts.links = append(facts.links, link)
		return nil
	})
	matrixQuery(ctx, t, conn, `SELECT source_work_item_id, target_work_item_id, relationship_type FROM work_item_dependencies FINAL WHERE org_id = ?`, args, func(rows driver.Rows) error {
		var source, target, relationship string
		if err := rows.Scan(&source, &target, &relationship); err != nil {
			return err
		}
		facts.deps[source] = append(facts.deps[source], target+"|"+relationship)
		return nil
	})
	// The attribution of an item is its rows of the newest computation (the fence every reader of the
	// table applies: the newest computed_at of the item).
	matrixQuery(ctx, t, conn, `SELECT w.work_item_id, ifNull(w.team_id, ''), toString(w.source), w.is_primary, w.evidence
FROM work_item_team_attributions AS w FINAL
INNER JOIN (SELECT work_item_id, max(computed_at) AS newest FROM work_item_team_attributions FINAL WHERE org_id = ? GROUP BY work_item_id) AS l
  ON l.work_item_id = w.work_item_id AND l.newest = w.computed_at
WHERE w.org_id = ?`, []any{org, org}, func(rows driver.Rows) error {
		var item string
		var row attributionRow
		if err := rows.Scan(&item, &row.TeamID, &row.Source, &row.IsPrimary, &row.Evidence); err != nil {
			return err
		}
		facts.attribution[item] = append(facts.attribution[item], row)
		return nil
	})
	return facts
}

// ---- the contract's ownership rule, written once, independent of every reader ----

// itemDay is the day whose facts give an item its team (team-attribution.md §0.4j): the start of the
// completion day of a done item, else the start of the current day.
func (facts matrixFacts) itemDay(item matrixItem) time.Time {
	day := facts.now.UTC().Truncate(24 * time.Hour)
	if item.Status == "done" && item.CompletedAt != nil && !item.CompletedAt.After(facts.now) {
		day = item.CompletedAt.UTC().Truncate(24 * time.Hour)
	}
	return day
}

// ownersAt is the contract's owner set of the rows of one subject at an instant: the rows valid then, of
// active teams; before the subject's first recorded ownership, its FIRST recorded owners apply backward
// (team-attribution.md §0.4j, superseded rule, D5987 option C).
func (facts matrixFacts) ownersAt(rows []ownershipRow, at time.Time) map[string]bool {
	owners := map[string]bool{}
	var first time.Time
	for _, row := range rows {
		if first.IsZero() || row.ValidFrom.Before(first) {
			first = row.ValidFrom
		}
	}
	for _, row := range rows {
		team, known := facts.teams[row.TeamID]
		if row.TeamID == "" || (known && !team.Active) {
			continue
		}
		if row.validAt(at) || (at.Before(first) && row.ValidFrom.Equal(first)) {
			owners[row.TeamID] = true
		}
	}
	return owners
}

// isTeamKeyProject is the project a Linear team key stands for ("<org>:linear:<key>", the team-key
// ownership kind): the team's own key, not a project of the projects catalog.
func (facts matrixFacts) isTeamKeyProject(projectID string) bool {
	return strings.HasPrefix(projectID, facts.org+":")
}

func (facts matrixFacts) repoOwnerRows(repoID string) []ownershipRow {
	var out []ownershipRow
	name := facts.repos[repoID].Name
	for _, row := range facts.repoOwners {
		if (row.RepoID != "" && row.RepoID == repoID) || (row.RepoID == "" && name != "" && strings.EqualFold(row.RepoName, name)) {
			out = append(out, row)
		}
	}
	return out
}

func (facts matrixFacts) projectOwnerRows(projectID string) []ownershipRow {
	var out []ownershipRow
	for _, row := range facts.projOwners {
		if projectID != "" && row.ProjectID == projectID {
			out = append(out, row)
		}
	}
	return out
}

func (facts matrixFacts) primary(itemID string) (attributionRow, bool) {
	for _, row := range facts.attribution[itemID] {
		if row.IsPrimary == 1 {
			return row, true
		}
	}
	return attributionRow{}, false
}

// donorSources are the sources by which an issue holds a team of its own, the only teams a pull request
// may inherit from it (team-attribution.md §0: a donor's team is a native team or an ownership, never an
// inherited, membership, fallback or unassigned row).
var donorSources = map[string]bool{"native_team": true, "issue_project": true, "project_ownership": true, "repo_ownership": true}

// ownPrimary is an item's primary row when the item holds a resolved team on its own: what a donor gives.
func (facts matrixFacts) ownPrimary(itemID string) (attributionRow, bool) {
	row, ok := facts.primary(itemID)
	if !ok || row.TeamID == "" || !donorSources[row.Source] {
		return attributionRow{}, false
	}
	return row, true
}

// donorTeamsOnDay is the team a donor issue holds on its own on a day, by the contract's rule: its native
// team; else, of the owners of its project (or repository) valid that day (ownersAt), its main owner -
// one main owner for the whole history (team-attribution.md §0.4d), the team of its own primary row -
// when that team owned it that day, with every owner tied with it that day (tiedOwnersAt), and otherwise
// every owner of that day.
func (facts matrixFacts) donorTeamsOnDay(donor string, day time.Time) map[string]bool {
	own, ok := facts.ownPrimary(donor)
	if !ok {
		return map[string]bool{}
	}
	if own.Source == "native_team" {
		return map[string]bool{own.TeamID: true}
	}
	item := facts.items[donor]
	var rows []ownershipRow
	if own.Source == "repo_ownership" {
		rows = facts.repoOwnerRows(item.RepoID)
	} else {
		rows = facts.projectOwnerRows(item.ProjectID)
	}
	owners := facts.ownersAt(rows, day)
	if owners[own.TeamID] {
		return facts.tiedOwnersAt(rows, day, own.TeamID)
	}
	return owners
}

// tiedOwnersAt is the team with every owner tied with it at a time: owners of equal rank (specificity,
// then priority) at the top when no row of that time is a main row (is_primary 1). Like a full tie of
// main owners (D5990), every tied owner is an owner; the cascade writes the first by its deterministic
// order as the item's primary and the others as co-owner rows (team-attribution.md §0.4d), so any tied
// owner is the donor's team.
func (facts matrixFacts) tiedOwnersAt(rows []ownershipRow, at time.Time, team string) map[string]bool {
	out := map[string]bool{team: true}
	var first time.Time
	for _, row := range rows {
		if first.IsZero() || row.ValidFrom.Before(first) {
			first = row.ValidFrom
		}
	}
	var valid []ownershipRow
	for _, row := range rows {
		if known, ok := facts.teams[row.TeamID]; row.TeamID == "" || (ok && !known.Active) {
			continue
		}
		if row.validAt(at) || (at.Before(first) && row.ValidFrom.Equal(first)) {
			if row.IsPrimary == 1 {
				return out
			}
			valid = append(valid, row)
		}
	}
	outranks := func(a, b ownershipRow) bool {
		return a.Specificity > b.Specificity || (a.Specificity == b.Specificity && a.Priority < b.Priority)
	}
	var top []ownershipRow
	for _, row := range valid {
		switch {
		case len(top) == 0 || outranks(row, top[0]):
			top = []ownershipRow{row}
		case !outranks(top[0], row):
			top = append(top, row)
		}
	}
	tied := map[string]bool{}
	for _, row := range top {
		tied[row.TeamID] = true
	}
	if !tied[team] {
		return out
	}
	return tied
}

// donorRelationships are the dependency types a pull request inherits an issue's team through
// (team-attribution.md §0: PR/MR attribution comes from an actual linked issue row). The list is the
// contract's, written here, not read from the cascade.
var donorRelationships = map[string]bool{"relates_to": true, "relates": true, "duplicates": true, "external_issue_key": true}

// itemByKey is the one Jira or Linear item of an issue key, or "" when none or several hold it.
func (facts matrixFacts) itemByKey(key string) string {
	found := ""
	for id, item := range facts.items {
		_, suffix, ok := strings.Cut(id, ":")
		if !ok || (item.Provider != "jira" && item.Provider != "linear") || !strings.EqualFold(strings.TrimSpace(suffix), strings.TrimSpace(key)) {
			continue
		}
		if found != "" {
			return ""
		}
		found = id
	}
	return found
}

// linkedDonors are the issues a pull request is actually linked to: the targets of its recorded
// relations (an extkey relation resolves only to the one existing item of that key, never to a key
// prefix) and the issues of its links of record (work_graph_issue_pr, any tier). A pull request is no
// donor.
func (facts matrixFacts) linkedDonors(prID string) map[string]bool {
	donors := map[string]bool{}
	add := func(id string) {
		if item, ok := facts.items[id]; ok && !isPullRequestItem(item) {
			donors[id] = true
		}
	}
	for _, dep := range facts.deps[prID] {
		target, relationship, _ := strings.Cut(dep, "|")
		if !donorRelationships[relationship] {
			continue
		}
		if key, ok := strings.CutPrefix(target, "extkey:"); ok {
			if id := facts.itemByKey(key); id != "" {
				add(id)
			}
			continue
		}
		add(target)
	}
	pr := facts.items[prID]
	number := prID[strings.LastIndexAny(prID, "#!")+1:]
	for _, link := range facts.links {
		if link.RepoID == pr.RepoID && fmt.Sprint(link.PRNumber) == number {
			add(link.WorkItemID)
		}
	}
	return donors
}

// subjectProvider is the provider a check of an item counts under: a pull request's own provider; an
// issue's provider.
func isPullRequestItem(item matrixItem) bool {
	return item.Type == "pr" || item.Type == "pull_request" || item.Type == "merge_request"
}

func sortedTeamSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ---- the pipeline ----

// runAttributionVerbs runs the production verbs in production order over one organization: the inferred
// repository-ownership derivation (the sync.team_repo_ownership_derivation job body), the daily
// work_item_attribution family for every day from the first day of the worlds to today, then the backstop.
func runAttributionVerbs(ctx context.Context, t *testing.T, conn driver.Conn, org string, first, today time.Time) {
	t.Helper()
	derivation := providersync.TeamRepoOwnershipDerivationService{Conn: conn}
	if _, _, _, _, _, err := derivation.DeriveWithStats(ctx, org); err != nil {
		t.Fatalf("derive %s: %v", org, err)
	}
	discoverer, err := daily.NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	repos, err := discoverer.RepositoryIDs(ctx, org)
	if err != nil {
		t.Fatal(err)
	}
	family, err := daily.NewWorkItemAttributionExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	for day := first; !day.After(today); day = day.AddDate(0, 0, 1) {
		run := daily.Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true, DiscoveredRepoIDs: repos}
		if _, err := family.ComputeFamily(ctx, run, daily.Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: repos}); err != nil {
			t.Fatalf("daily work_item_attribution %s %s: %v", org, day.Format(time.DateOnly), err)
		}
	}
	writer, err := remaining.NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	backstop, err := remaining.NewWorkItemAttributionExecutor(ctx, conn, writer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backstop.ComputeOrg(ctx, org); err != nil {
		t.Fatalf("backstop %s: %v", org, err)
	}
}

// loadProviderWorlds loads every provider world for its own organization, moved so it ends today.
func loadProviderWorlds(ctx context.Context, t *testing.T, conn driver.Conn, now time.Time) map[string]string {
	t.Helper()
	orgs := map[string]string{}
	for _, world := range providerWorlds {
		frozen, err := LoadFrozenWorld(world.Params)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := LoadWorld(ctx, conn, frozen, world.OrgID, now); err != nil {
			t.Fatalf("load %s: %v", world.Params.Provider, err)
		}
		orgs[world.Params.Provider] = world.OrgID
	}
	return orgs
}

// openMatrixClickHouse is a fresh ClickHouse at the migration head with the provider worlds loaded.
func openMatrixClickHouse(ctx context.Context, t *testing.T) (driver.Conn, map[string]string, time.Time) {
	t.Helper()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	now := time.Now().UTC()
	return conn, loadProviderWorlds(ctx, t, conn, now), now
}

// ---- the checks ----

// checkResult is what one check found for one provider: how many subjects it judged, and the violations.
type checkResult struct {
	Subjects   int
	Violations []string
}

type matrixCheck struct {
	Entity string
	Eval   func(facts matrixFacts, provider string, readers ownerReaders) checkResult
}

// ownerReaders are the shared ownership readers whose answers the consumers check compares with the
// contract's rule (ownersAt). A known defect is planted by replacing one.
type ownerReaders struct {
	OwnersByRepo  func(at time.Time) (map[string][]string, error)
	Authoritative func(at time.Time) (map[string][]string, error)
}

var matrixChecks = map[string]matrixCheck{
	// teams
	"provider_prefixed_ids": {"teams", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, team := range f.teams {
			if team.Provider != p {
				continue
			}
			r.Subjects++
			if !strings.HasPrefix(id, providerTeamPrefix[p]+":") {
				r.Violations = append(r.Violations, id)
			}
		}
		return r
	}},
	"ownerless_team_takes_no_work": {"teams", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		owning := map[string]bool{}
		for _, row := range append(append([]ownershipRow{}, f.repoOwners...), f.projOwners...) {
			if !f.isTeamKeyProject(row.ProjectID) {
				owning[row.TeamID] = true
			}
		}
		ownerless := map[string]bool{}
		for id, team := range f.teams {
			if team.Provider == p && !owning[id] {
				ownerless[id] = true
				r.Subjects++
			}
		}
		for item, rows := range f.attribution {
			for _, row := range rows {
				if ownerless[row.TeamID] && row.IsPrimary >= 1 {
					r.Violations = append(r.Violations, item+" -> "+row.TeamID+" ("+row.Source+")")
				}
			}
		}
		return r
	}},
	"retired_team_owns_nothing_now": {"teams", func(f matrixFacts, p string, readers ownerReaders) (r checkResult) {
		retired := map[string]bool{}
		for id, team := range f.teams {
			if team.Provider != p {
				continue
			}
			rows, open := 0, 0
			for _, row := range append(append([]ownershipRow{}, f.repoOwners...), f.projOwners...) {
				if row.TeamID == id {
					rows++
					if row.ValidTo == nil || row.ValidTo.After(f.now) {
						open++
					}
				}
			}
			if !team.Active || (rows > 0 && open == 0) {
				retired[id] = true
				r.Subjects++
			}
		}
		owners, err := readers.OwnersByRepo(f.now)
		if err != nil {
			r.Violations = append(r.Violations, "OwnersByRepo: "+err.Error())
			return r
		}
		for repo, teams := range owners {
			for _, team := range teams {
				if retired[team] {
					r.Violations = append(r.Violations, "OwnersByRepo "+repo+" -> "+team)
				}
			}
		}
		for item, rows := range f.attribution {
			it := f.items[item]
			for _, row := range rows {
				if retired[row.TeamID] && row.IsPrimary >= 1 && !f.itemDay(it).Before(f.now.Truncate(24*time.Hour)) {
					r.Violations = append(r.Violations, "open item "+item+" -> "+row.TeamID)
				}
			}
		}
		return r
	}},
	// members
	"membership_never_decides_the_team": {"members", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, row := range f.memberships {
			if row.Provider == p {
				r.Subjects++
			}
		}
		for item, rows := range f.attribution {
			if f.items[item].Provider != p {
				continue
			}
			for _, row := range rows {
				if strings.HasSuffix(row.Source, "membership") && row.IsPrimary >= 1 {
					r.Violations = append(r.Violations, item+" -> "+row.TeamID+" ("+row.Source+")")
				}
			}
		}
		return r
	}},
	"membership_starts_at_its_evidence": {"members", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, row := range f.memberships {
			if row.Provider != p {
				continue
			}
			member := strings.ToLower(row.RepoName[strings.Index(row.RepoName, ":")+1:])
			var first time.Time
			for _, item := range f.items {
				people := append([]string{item.Reporter}, item.Assignees...)
				for _, person := range people {
					person = strings.ToLower(person)
					if person != "" && (strings.HasSuffix(person, ":"+member) || strings.HasPrefix(person, member+"@") || person == member) {
						if first.IsZero() || item.CreatedAt.Before(first) {
							first = item.CreatedAt
						}
					}
				}
			}
			if first.IsZero() {
				continue
			}
			r.Subjects++
			if row.ValidFrom.After(first) {
				r.Violations = append(r.Violations, fmt.Sprintf("%s %s from %s, first work %s", row.TeamID, row.RepoName, row.ValidFrom.Format(time.DateOnly), first.Format(time.DateOnly)))
			}
		}
		return r
	}},
	// repositories
	"pull_request_repository_resolves": {"repositories", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for key, pr := range f.prs {
			repo, ok := f.repos[pr.RepoID]
			if ok && repo.Provider != p {
				continue
			}
			r.Subjects++
			if !ok {
				r.Violations = append(r.Violations, key)
			}
		}
		return r
	}},
	"readers_agree_with_the_ownership_rule": {"repositories", func(f matrixFacts, p string, readers ownerReaders) checkResult {
		return f.judgeReaders(p, readers, false)
	}},
	"readers_apply_first_owners_backward": {"repositories", func(f matrixFacts, p string, readers ownerReaders) checkResult {
		return f.judgeReaders(p, readers, true)
	}},
	"repository_ownership_starts_at_its_evidence": {"repositories", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, repo := range f.repos {
			if repo.Provider != p {
				continue
			}
			var first time.Time
			for _, pr := range f.prs {
				if pr.RepoID == id && (first.IsZero() || pr.CreatedAt.Before(first)) {
					first = pr.CreatedAt
				}
			}
			rows := f.repoOwnerRows(id)
			if first.IsZero() || len(rows) == 0 {
				continue
			}
			r.Subjects++
			var earliest time.Time
			for _, row := range rows {
				if earliest.IsZero() || row.ValidFrom.Before(earliest) {
					earliest = row.ValidFrom
				}
			}
			if earliest.After(first) {
				r.Violations = append(r.Violations, fmt.Sprintf("%s owned from %s, first pull request %s", repo.Name, earliest.Format(time.DateOnly), first.Format(time.DateOnly)))
			}
		}
		return r
	}},
	// pull requests: a link's provider is its pull request's (repositories) or its issue's
	"issue_links_resolve": {"pull_requests", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, link := range f.links {
			item, itemOK := f.items[link.WorkItemID]
			repo := f.repos[link.RepoID]
			if repo.Provider != p && item.Provider != p {
				continue
			}
			r.Subjects++
			if !itemOK {
				r.Violations = append(r.Violations, "no work item "+link.WorkItemID)
			}
			if _, ok := f.prs[fmt.Sprintf("%s#%d", link.RepoID, link.PRNumber)]; !ok {
				r.Violations = append(r.Violations, fmt.Sprintf("no pull request %s#%d", repo.Name, link.PRNumber))
			}
		}
		return r
	}},
	"native_link_has_a_linked_row": {"pull_requests", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, link := range f.links {
			if link.Provenance != "native" {
				continue
			}
			item := f.items[link.WorkItemID]
			repo := f.repos[link.RepoID]
			if repo.Provider != p && item.Provider != p {
				continue
			}
			r.Subjects++
			found := false
			for prID, candidate := range f.items {
				if !isPullRequestItem(candidate) || !strings.HasSuffix(prID, fmt.Sprintf("%d", link.PRNumber)) || candidate.RepoID != link.RepoID {
					continue
				}
				for _, dep := range append(f.deps[prID], f.deps[link.WorkItemID]...) {
					target, relationship, _ := strings.Cut(dep, "|")
					if relationship != "external_issue_key" && (target == link.WorkItemID || target == prID) {
						found = true
					}
				}
			}
			if !found {
				r.Violations = append(r.Violations, fmt.Sprintf("%s <- %s#%d has no linked row", link.WorkItemID, repo.Name, link.PRNumber))
			}
		}
		return r
	}},
	"pull_request_inherits_only_from_a_linked_issue": {"pull_requests", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		// A pull request's linked_issue row names the team an issue it is actually linked to holds on its
		// own ON THE PULL REQUEST'S DAY (donorTeamsOnDay; following the donor's later changes is
		// CHAOS-9478). A pull request counts under its own provider and under the provider of each issue
		// it is linked to.
		for id, item := range f.items {
			if !isPullRequestItem(item) {
				continue
			}
			donors := f.linkedDonors(id)
			counts := item.Provider == p
			for donor := range donors {
				counts = counts || f.items[donor].Provider == p
			}
			if !counts {
				continue
			}
			for _, row := range f.attribution[id] {
				if row.Source != "linked_issue" {
					continue
				}
				r.Subjects++
				day := f.itemDay(item)
				given := false
				for donor := range donors {
					if row.TeamID != "" && f.donorTeamsOnDay(donor, day)[row.TeamID] {
						given = true
					}
				}
				if !given {
					var gave []string
					for _, donor := range sortedTeamSet(donors) {
						gave = append(gave, fmt.Sprintf("%s=%v", donor, sortedTeamSet(f.donorTeamsOnDay(donor, day))))
					}
					r.Violations = append(r.Violations, fmt.Sprintf("%s (%s) -> %s by linked_issue; linked issues' teams that day %v", id, day.Format(time.DateOnly), row.TeamID, gave))
				}
			}
		}
		return r
	}},
	"pull_request_team_is_not_a_membership": {"pull_requests", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		linkedProvider := map[string]bool{}
		for _, link := range f.links {
			if issue, ok := f.items[link.WorkItemID]; ok && issue.Provider == p {
				linkedProvider[fmt.Sprintf("%s#%d", link.RepoID, link.PRNumber)] = true
			}
		}
		for id, item := range f.items {
			if !isPullRequestItem(item) {
				continue
			}
			number := id[strings.LastIndexAny(id, "#!")+1:]
			if item.Provider != p && !linkedProvider[item.RepoID+"#"+number] {
				continue
			}
			r.Subjects++
			if row, ok := f.primary(id); ok && strings.HasSuffix(row.Source, "membership") {
				r.Violations = append(r.Violations, id+" -> "+row.TeamID+" ("+row.Source+")")
			}
		}
		return r
	}},
	// issues
	"issue_reaches_a_repository_only_through_its_links": {"issues", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, link := range f.links {
			item, ok := f.items[link.WorkItemID]
			if !ok || item.Provider != p || isPullRequestItem(item) {
				continue
			}
			r.Subjects++
			if _, ok := f.repos[link.RepoID]; !ok {
				r.Violations = append(r.Violations, link.WorkItemID+" -> unknown repository "+link.RepoID)
			}
		}
		return r
	}},
	"primary_owner_holds_on_the_item_day": {"issues", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, item := range f.items {
			if item.Provider != p || isPullRequestItem(item) {
				continue
			}
			row, ok := f.primary(id)
			if !ok {
				continue
			}
			var rows []ownershipRow
			switch row.Source {
			case "native_team":
				r.Subjects++
				team, known := f.teams[row.TeamID]
				if item.NativeTeamKey == "" || row.TeamID != providerTeamPrefix[p]+":"+item.NativeTeamKey || !known || !team.Active {
					r.Violations = append(r.Violations, fmt.Sprintf("%s -> %s by native_team; native key %q", id, row.TeamID, item.NativeTeamKey))
				}
				continue
			case "issue_project", "project_ownership":
				rows = f.projectOwnerRows(item.ProjectID)
			case "repo_ownership":
				rows = f.repoOwnerRows(item.RepoID)
			default:
				continue
			}
			r.Subjects++
			owners := f.ownersAt(rows, f.itemDay(item))
			if !owners[row.TeamID] {
				r.Violations = append(r.Violations, fmt.Sprintf("%s (%s) -> %s by %s; owners that day %v", id, f.itemDay(item).Format(time.DateOnly), row.TeamID, row.Source, sortedTeamSet(owners)))
			}
		}
		return r
	}},
	"owned_history_is_attributed": {"issues", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, item := range f.items {
			if item.Provider != p || isPullRequestItem(item) || item.Status != "done" {
				continue
			}
			owners := f.ownersAt(append(f.projectOwnerRows(item.ProjectID), f.repoOwnerRows(item.RepoID)...), f.itemDay(item))
			if len(owners) == 0 {
				continue
			}
			r.Subjects++
			if row, ok := f.primary(id); !ok || row.Source == "unassigned" {
				r.Violations = append(r.Violations, fmt.Sprintf("%s (%s) unassigned; owners that day %v", id, f.itemDay(item).Format(time.DateOnly), sortedTeamSet(owners)))
			}
		}
		return r
	}},
	"every_active_owner_takes_the_item": {"issues", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, item := range f.items {
			if item.Provider != p || isPullRequestItem(item) {
				continue
			}
			rows := f.projectOwnerRows(item.ProjectID)
			if len(rows) == 0 {
				rows = f.repoOwnerRows(item.RepoID)
			}
			owners := f.ownersAt(rows, f.itemDay(item))
			// The item's own native team wins (team-attribution.md 0.4d, D5206): no co-owner then.
			if primary, ok := f.primary(id); len(owners) < 2 || (ok && primary.Source == "native_team") {
				continue
			}
			r.Subjects++
			// An owner takes the item as its primary (1) or as a co-owner (2); a non-primary row (0) is
			// a candidate, not an attribution.
			got := map[string]bool{}
			for _, row := range f.attribution[id] {
				if row.TeamID != "" && (row.IsPrimary == 1 || row.IsPrimary == 2) {
					got[row.TeamID] = true
				}
			}
			for owner := range owners {
				if !got[owner] {
					r.Violations = append(r.Violations, fmt.Sprintf("%s misses owner %s", id, owner))
				}
			}
		}
		return r
	}},
	"attribution_carries_provenance": {"issues", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for id, rows := range f.attribution {
			if f.items[id].Provider != p {
				continue
			}
			for _, row := range rows {
				r.Subjects++
				if strings.TrimSpace(row.Evidence) == "" || row.Source == "" {
					r.Violations = append(r.Violations, id+" "+row.Source)
				}
			}
		}
		return r
	}},
	// projects
	"project_owner_is_of_the_project_provider": {"projects", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, row := range f.projOwners {
			if row.Provider != p {
				continue
			}
			r.Subjects++
			team, ok := f.teams[row.TeamID]
			if ok && team.Provider != "" && team.Provider != p {
				r.Violations = append(r.Violations, row.TeamID+" owns "+row.ProjectID)
			}
		}
		return r
	}},
	"project_ownership_names_the_catalog_project": {"projects", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		for _, row := range f.projOwners {
			if row.Provider != p || f.isTeamKeyProject(row.ProjectID) {
				continue
			}
			r.Subjects++
			if _, ok := f.projects[row.ProjectID]; !ok {
				r.Violations = append(r.Violations, row.TeamID+" owns "+row.ProjectID+", which the projects catalog does not hold")
			}
		}
		return r
	}},
	"project_ownership_starts_at_its_evidence": {"projects", func(f matrixFacts, p string, _ ownerReaders) (r checkResult) {
		seen := map[string]bool{}
		for _, row := range f.projOwners {
			if row.Provider != p || seen[row.ProjectID] || f.isTeamKeyProject(row.ProjectID) {
				continue
			}
			var first time.Time
			for _, item := range f.items {
				if item.ProjectID == row.ProjectID && (first.IsZero() || item.CreatedAt.Before(first)) {
					first = item.CreatedAt
				}
			}
			if first.IsZero() {
				continue
			}
			seen[row.ProjectID] = true
			r.Subjects++
			var earliest time.Time
			for _, candidate := range f.projectOwnerRows(row.ProjectID) {
				if earliest.IsZero() || candidate.ValidFrom.Before(earliest) {
					earliest = candidate.ValidFrom
				}
			}
			if earliest.After(first) {
				r.Violations = append(r.Violations, fmt.Sprintf("%s owned from %s, first item %s", row.ProjectID, earliest.Format(time.DateOnly), first.Format(time.DateOnly)))
			}
		}
		return r
	}},
}

// judgeReaders compares the shared readers with the contract's rule (ownersAt) for every repository of
// the provider, at now and on the day of every item of its repositories. backward=false judges the
// instants at or after a repository's first recorded ownership (the rows valid then, closures included);
// backward=true judges the instants before it, where the first recorded owners apply backward (D5987,
// option C).
func (f matrixFacts) judgeReaders(p string, readers ownerReaders, backward bool) (r checkResult) {
	instants := map[time.Time]bool{f.now: true}
	for _, item := range f.items {
		if repo, ok := f.repos[item.RepoID]; ok && repo.Provider == p {
			instants[f.itemDay(item)] = true
		}
	}
	ordered := make([]time.Time, 0, len(instants))
	for at := range instants {
		ordered = append(ordered, at)
	}
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].Before(ordered[right]) })
	for _, at := range ordered {
		var shared map[string][]string
		var authoritative map[string][]string
		asked := false
		day := at.Format(time.DateOnly)
		for id, repo := range f.repos {
			if repo.Provider != p {
				continue
			}
			rows := f.repoOwnerRows(id)
			var first time.Time
			for _, row := range rows {
				if first.IsZero() || row.ValidFrom.Before(first) {
					first = row.ValidFrom
				}
			}
			if before := len(rows) > 0 && at.Before(first); before != backward {
				continue
			}
			if !asked {
				var err error
				if shared, err = readers.OwnersByRepo(at); err != nil {
					r.Violations = append(r.Violations, "OwnersByRepo: "+err.Error())
					return r
				}
				if authoritative, err = readers.Authoritative(at); err != nil {
					r.Violations = append(r.Violations, "AuthoritativeOwnersByRepo: "+err.Error())
					return r
				}
				asked = true
			}
			want := f.ownersAt(rows, at)
			r.Subjects++
			got := map[string]bool{}
			for _, team := range shared[id] {
				got[team] = true
			}
			if strings.Join(sortedTeamSet(got), ",") != strings.Join(sortedTeamSet(want), ",") {
				r.Violations = append(r.Violations, fmt.Sprintf("OwnersByRepo %s at %s = %v, rule %v", repo.Name, day, sortedTeamSet(got), sortedTeamSet(want)))
			}
			if owners, ok := authoritative[id]; ok && len(owners) > 0 {
				for _, owner := range owners {
					if !want[owner] {
						r.Violations = append(r.Violations, fmt.Sprintf("AuthoritativeOwnersByRepo %s at %s names %s, not an owner", repo.Name, day, owner))
					}
				}
			} else if len(want) > 0 {
				r.Violations = append(r.Violations, fmt.Sprintf("AuthoritativeOwnersByRepo %s at %s has no owner, rule %v", repo.Name, day, sortedTeamSet(want)))
			}
		}
	}
	return r
}

func productionReaders(ctx context.Context, conn driver.Conn, org string) ownerReaders {
	return ownerReaders{
		OwnersByRepo: func(at time.Time) (map[string][]string, error) { return teamownership.OwnersByRepo(ctx, conn, org, at) },
		Authoritative: func(at time.Time) (map[string][]string, error) {
			return teamownership.AuthoritativeOwnersByRepo(ctx, conn, org, at)
		},
	}
}

// evaluateCell runs one cell's check over the organization of its provider's world.
func evaluateCell(cell matrixCell, facts map[string]matrixFacts, readers map[string]ownerReaders) (checkResult, error) {
	check, ok := matrixChecks[cell.Check]
	if !ok {
		return checkResult{}, fmt.Errorf("no check %q", cell.Check)
	}
	if check.Entity != cell.Entity {
		return checkResult{}, fmt.Errorf("check %q judges %s, not %s", cell.Check, check.Entity, cell.Entity)
	}
	return check.Eval(facts[cell.Provider], cell.Provider, readers[cell.Provider]), nil
}

// TestCrossProviderAttributionMatrix evaluates every cell of the contract on what the production verbs
// write over the provider worlds.
func TestCrossProviderAttributionMatrix(t *testing.T) {
	contract := loadMatrixContract(t)
	if contract.Version != 1 {
		t.Fatalf("matrix.json version %d", contract.Version)
	}
	covered := map[string]bool{}
	for _, cell := range contract.Cells {
		covered[cell.Provider+"/"+cell.Entity] = true
	}
	for _, entry := range contract.NotApplicable {
		if covered[entry.Provider+"/"+entry.Entity] {
			t.Errorf("%s/%s is not_applicable and has a cell", entry.Provider, entry.Entity)
		}
		if strings.TrimSpace(entry.Rule) == "" {
			t.Errorf("%s/%s is not_applicable without the rule that makes it so", entry.Provider, entry.Entity)
		}
		covered[entry.Provider+"/"+entry.Entity] = true
	}
	for _, provider := range attributionProviders {
		for _, entity := range attributionEntities {
			if !covered[provider+"/"+entity] {
				t.Errorf("%s/%s has no cell and no not_applicable entry", provider, entity)
			}
		}
	}
	// Within a covered entity, every check of the entity is a cell or an omission with its rule.
	judged := map[string]bool{}
	for _, cell := range contract.Cells {
		judged[cell.Provider+"/"+cell.Entity+"/"+cell.Check] = true
	}
	for _, entry := range contract.Omitted {
		key := entry.Provider + "/" + entry.Entity + "/" + entry.Check
		if judged[key] || strings.TrimSpace(entry.Rule) == "" {
			t.Errorf("omitted %s: it has a cell, or no rule", key)
		}
		judged[key] = true
	}
	notApplicable := map[string]bool{}
	for _, entry := range contract.NotApplicable {
		notApplicable[entry.Provider+"/"+entry.Entity] = true
	}
	for name, check := range matrixChecks {
		for _, provider := range attributionProviders {
			if !notApplicable[provider+"/"+check.Entity] && !judged[provider+"/"+check.Entity+"/"+name] {
				t.Errorf("%s/%s/%s is neither a cell nor omitted with its rule", provider, check.Entity, name)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	conn, orgs, facts, readers := producedMatrixState(ctx, t)
	for _, cell := range contract.Cells {
		name := cell.Provider + "/" + cell.Entity + "/" + cell.Check
		t.Run(name, func(t *testing.T) {
			result, err := evaluateCell(cell, facts, readers)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case result.Subjects == 0:
				t.Fatalf("fixture_gap: no world exercises %s", name)
			case cell.State == "pass":
				if len(result.Violations) > 0 {
					t.Errorf("%d of %d subject(s) violate %s:\n  %s", len(result.Violations), result.Subjects, cell.Check, strings.Join(result.Violations, "\n  "))
				}
			case cell.State == "known_red":
				if cell.Ticket == "" {
					t.Fatalf("a known_red cell without its ticket")
				}
				if len(result.Violations) == 0 {
					t.Errorf("known_red (%s) turned green over %d subject(s): move it to pass", cell.Ticket, result.Subjects)
				} else {
					t.Logf("known_red (%s): %d of %d subject(s):\n  %s", cell.Ticket, len(result.Violations), result.Subjects, strings.Join(result.Violations, "\n  "))
				}
			default:
				t.Fatalf("unknown state %q", cell.State)
			}
		})
	}
	// The acceptance gate judges the same produced state the cells judge.
	t.Run("rediscovers_known_defects", func(t *testing.T) {
		rediscoverKnownDefects(ctx, t, conn, orgs, facts, readers)
	})
}

// producedMatrixState is the state every cell and the acceptance gate judge: a fresh ClickHouse at the
// migration head with the provider worlds loaded, the production attribution verbs run over each
// organization, and the facts and the shared readers of each. An organization the verbs gave no
// attribution row fails here: no cell may judge a state the verbs never reached.
func producedMatrixState(ctx context.Context, t *testing.T) (driver.Conn, map[string]string, map[string]matrixFacts, map[string]ownerReaders) {
	t.Helper()
	conn, orgs, now := openMatrixClickHouse(ctx, t)
	today := now.Truncate(24 * time.Hour)
	facts := map[string]matrixFacts{}
	readers := map[string]ownerReaders{}
	for provider, org := range orgs {
		var first time.Time
		matrixQuery(ctx, t, conn, `SELECT min(created_at) FROM work_items WHERE org_id = ?`, []any{org}, func(rows driver.Rows) error {
			return rows.Scan(&first)
		})
		runAttributionVerbs(ctx, t, conn, org, first.UTC().Truncate(24*time.Hour), today)
		facts[provider] = loadMatrixFacts(ctx, t, conn, org, now)
		if len(facts[provider].attribution) == 0 {
			t.Fatalf("%s: the production verbs wrote no work_item_team_attributions row", provider)
		}
		readers[provider] = productionReaders(ctx, conn, org)
	}
	return conn, orgs, facts, readers
}

// ---- the acceptance gate: known defects the matrix must rediscover ----

// localOwnershipSQL is a reader with its own ownership SQL, as the readers the shared one replaced had:
// every grant ever written counts, closed or not (no valid_to, no active-team rule).
func localOwnershipSQL(ctx context.Context, conn driver.Conn, org string) func(time.Time) (map[string][]string, error) {
	return func(time.Time) (map[string][]string, error) {
		out := map[string][]string{}
		rows, err := conn.Query(ctx, `SELECT DISTINCT toString(r.id), o.team_id FROM team_repo_ownership AS o FINAL
INNER JOIN repos AS r FINAL ON r.org_id = o.org_id AND (toString(r.id) = ifNull(toString(o.repo_id), '') OR lower(r.repo) = lower(o.repo_full_name))
WHERE o.org_id = ? AND o.team_id != '' ORDER BY 2`, org)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var repo, team string
			if err := rows.Scan(&repo, &team); err != nil {
				return nil, err
			}
			out[repo] = append(out[repo], team)
		}
		return out, rows.Err()
	}
}

// membershipGrouping is the landscape's old team of a row: the teams its author is a member of, not the
// teams that own its repository (the rule CHAOS-9395 replaced).
func membershipGrouping(facts matrixFacts) func(time.Time) (map[string][]string, error) {
	return func(at time.Time) (map[string][]string, error) {
		out := map[string][]string{}
		for id, item := range facts.items {
			if !isPullRequestItem(item) {
				continue
			}
			teams := map[string]bool{}
			for _, row := range facts.memberships {
				member := strings.ToLower(row.RepoName[strings.Index(row.RepoName, ":")+1:])
				if row.validAt(at) && strings.Contains(strings.ToLower(facts.prAuthor(id)), member) && member != "" {
					teams[row.TeamID] = true
				}
			}
			out[item.RepoID] = append(out[item.RepoID], sortedTeamSet(teams)...)
		}
		return out, nil
	}
}

func (facts matrixFacts) prAuthor(workItemID string) string {
	return facts.authors[workItemID]
}

// rediscoverKnownDefects (TestCrossProviderAttributionMatrix/rediscovers_known_defects) is the acceptance
// gate: it plants each known defect the matrix exists to catch into the state the production verbs
// wrote, and requires the cell that judges it to turn red on the planted state, and to be green (or red
// for the reason it is known red) on the produced one. A check that cannot turn red
// on its defect is no check.
func rediscoverKnownDefects(ctx context.Context, t *testing.T, conn driver.Conn, orgs map[string]string, facts map[string]matrixFacts, readers map[string]ownerReaders) {
	judge := func(provider, check string, f map[string]matrixFacts, r map[string]ownerReaders) checkResult {
		t.Helper()
		result, err := evaluateCell(matrixCell{Provider: provider, Entity: matrixChecks[check].Entity, Check: check}, f, r)
		if err != nil {
			t.Fatal(err)
		}
		if result.Subjects == 0 {
			t.Fatalf("%s/%s judges nothing: the gate cannot run", provider, check)
		}
		return result
	}
	withFacts := func(provider string, plant func(*matrixFacts)) map[string]matrixFacts {
		copied := map[string]matrixFacts{}
		for key, value := range facts {
			copied[key] = value
		}
		planted := facts[provider]
		plant(&planted)
		copied[provider] = planted
		return copied
	}
	withReader := func(provider string, reader func(time.Time) (map[string][]string, error)) map[string]ownerReaders {
		copied := map[string]ownerReaders{}
		for key, value := range readers {
			copied[key] = value
		}
		planted := readers[provider]
		planted.OwnersByRepo = reader
		copied[provider] = planted
		return copied
	}

	t.Run("sync-time valid_from on a catalog writer", func(t *testing.T) {
		// As the producers write it: the ownership starts at the sync (known red, CHAOS-9404).
		if result := judge("jira", "project_ownership_starts_at_its_evidence", facts, readers); len(result.Violations) == 0 {
			t.Fatal("the sync-time start of the Jira project ownership is not found")
		}
		// The defect removed: the rows start at the first item of their project. The cell turns green.
		fixed := withFacts("jira", func(f *matrixFacts) {
			rows := append([]ownershipRow{}, f.projOwners...)
			for index := range rows {
				var first time.Time
				for _, item := range f.items {
					if item.ProjectID == rows[index].ProjectID && (first.IsZero() || item.CreatedAt.Before(first)) {
						first = item.CreatedAt
					}
				}
				if !first.IsZero() {
					rows[index].ValidFrom = first
				}
			}
			f.projOwners = rows
		})
		if result := judge("jira", "project_ownership_starts_at_its_evidence", fixed, readers); len(result.Violations) > 0 {
			t.Fatalf("the check stays red on ownership that starts at its evidence: %v", result.Violations)
		}
	})

	t.Run("landscape grouped by membership", func(t *testing.T) {
		if result := judge("github", "readers_agree_with_the_ownership_rule", facts, readers); len(result.Violations) > 0 {
			t.Fatalf("the shared readers disagree with the rule before the plant: %v", result.Violations)
		}
		planted := withReader("github", membershipGrouping(facts["github"]))
		if result := judge("github", "readers_agree_with_the_ownership_rule", facts, planted); len(result.Violations) == 0 {
			t.Fatal("a reader that groups a repository by its authors' memberships agrees with the ownership rule")
		}
	})

	t.Run("issue-key prefix taken as a link", func(t *testing.T) {
		if result := judge("jira", "native_link_has_a_linked_row", facts, readers); len(result.Violations) > 0 {
			t.Fatalf("a native link without a linked row before the plant: %v", result.Violations)
		}
		// The pull request that only names PAY-2 in its title carries an extkey dependency, not a link:
		// a writer that made it a native link from the key prefix is the defect.
		planted := withFacts("jira", func(f *matrixFacts) {
			for _, link := range f.links {
				if link.WorkItemID == "jira:PAY-2" {
					f.links = append(append([]matrixLink{}, f.links...), matrixLink{WorkItemID: link.WorkItemID, RepoID: link.RepoID, PRNumber: link.PRNumber, Provenance: "native"})
					return
				}
			}
			t.Fatal("the jira world has no link of PAY-2 to plant from")
		})
		if result := judge("jira", "native_link_has_a_linked_row", planted, readers); len(result.Violations) == 0 {
			t.Fatal("a native link made from an issue-key prefix is not found")
		}
	})

	t.Run("pull request team inherited without a linked issue", func(t *testing.T) {
		// A writer that gives a pull request a team by an issue-key prefix, with no linked issue row.
		planted := withFacts("github", func(f *matrixFacts) {
			ids := make([]string, 0, len(f.items))
			for id := range f.items {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				if !isPullRequestItem(f.items[id]) || len(f.linkedDonors(id)) > 0 {
					continue
				}
				attribution := map[string][]attributionRow{}
				for key, rows := range f.attribution {
					attribution[key] = rows
				}
				attribution[id] = append(append([]attributionRow{}, attribution[id]...), attributionRow{TeamID: "gh:payments", Source: "linked_issue", Evidence: "issue_key_prefix=PAY-2", IsPrimary: 1})
				f.attribution = attribution
				return
			}
			t.Fatal("the github world has no pull request without a linked issue")
		})
		if result := judge("github", "pull_request_inherits_only_from_a_linked_issue", planted, readers); len(result.Violations) == 0 {
			t.Fatal("a pull request team inherited without a linked issue is not found")
		}
	})

	t.Run("pull request team that is not its linked issue's", func(t *testing.T) {
		// A writer that names a team other than the linked issue's own on a linked_issue row.
		planted := withFacts("jira", func(f *matrixFacts) {
			ids := make([]string, 0, len(f.attribution))
			for id := range f.attribution {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				for index, row := range f.attribution[id] {
					if row.Source != "linked_issue" {
						continue
					}
					rows := append([]attributionRow{}, f.attribution[id]...)
					rows[index].TeamID = "jira:not-the-donor"
					attribution := map[string][]attributionRow{}
					for key, value := range f.attribution {
						attribution[key] = value
					}
					attribution[id] = rows
					f.attribution = attribution
					return
				}
			}
			t.Fatal("the jira world has no linked_issue row to plant on")
		})
		if result := judge("jira", "pull_request_inherits_only_from_a_linked_issue", facts, readers); len(result.Violations) > 0 {
			t.Fatalf("a linked_issue row without its donor before the plant: %v", result.Violations)
		}
		if result := judge("jira", "pull_request_inherits_only_from_a_linked_issue", planted, readers); len(result.Violations) == 0 {
			t.Fatal("a linked_issue row naming another team than its linked issue's is not found")
		}
	})

	t.Run("a co-owner written as a candidate row", func(t *testing.T) {
		if result := judge("jira", "every_active_owner_takes_the_item", facts, readers); len(result.Violations) > 0 {
			t.Fatalf("an active owner misses an item before the plant: %v", result.Violations)
		}
		// A writer that keeps a co-owner as a non-primary candidate (is_primary 0) instead of 2.
		planted := withFacts("jira", func(f *matrixFacts) {
			attribution := map[string][]attributionRow{}
			planted := false
			for id, rows := range f.attribution {
				copied := append([]attributionRow{}, rows...)
				for index := range copied {
					if copied[index].IsPrimary == 2 {
						copied[index].IsPrimary = 0
						planted = true
					}
				}
				attribution[id] = copied
			}
			if !planted {
				t.Fatal("the jira world has no co-owner row to plant on")
			}
			f.attribution = attribution
		})
		if result := judge("jira", "every_active_owner_takes_the_item", planted, readers); len(result.Violations) == 0 {
			t.Fatal("a co-owner written as a candidate row is not found")
		}
	})

	t.Run("readers that do not apply the first owners backward", func(t *testing.T) {
		// As the readers are: no owner before the first record (known red, CHAOS-9404).
		if result := judge("github", "readers_apply_first_owners_backward", facts, readers); len(result.Violations) == 0 {
			t.Fatal("the readers' missing backward owners are not found")
		}
		// The defect removed: a reader that answers an instant before a repository's first record with
		// the shared readers' answer at that first record. The cell turns green.
		first := map[string]time.Time{}
		for id := range facts["github"].repos {
			for _, row := range facts["github"].repoOwnerRows(id) {
				if first[id].IsZero() || row.ValidFrom.Before(first[id]) {
					first[id] = row.ValidFrom
				}
			}
		}
		production := readers["github"]
		backward := ownerReaders{
			OwnersByRepo: func(at time.Time) (map[string][]string, error) {
				out, err := production.OwnersByRepo(at)
				if err != nil {
					return nil, err
				}
				for id, start := range first {
					if at.Before(start) {
						later, err := production.OwnersByRepo(start)
						if err != nil {
							return nil, err
						}
						out[id] = later[id]
					}
				}
				return out, nil
			},
			Authoritative: func(at time.Time) (map[string][]string, error) {
				out, err := production.Authoritative(at)
				if err != nil {
					return nil, err
				}
				for id, start := range first {
					if at.Before(start) {
						later, err := production.Authoritative(start)
						if err != nil {
							return nil, err
						}
						if owners, ok := later[id]; ok {
							out[id] = owners
						}
					}
				}
				return out, nil
			},
		}
		fixed := map[string]ownerReaders{}
		for key, value := range readers {
			fixed[key] = value
		}
		fixed["github"] = backward
		if result := judge("github", "readers_apply_first_owners_backward", facts, fixed); len(result.Violations) > 0 {
			t.Fatalf("the check stays red on readers that apply the first owners backward: %v", result.Violations)
		}
	})

	t.Run("pull request team inherited from a donor without a team of its own", func(t *testing.T) {
		// A writer that takes the donor's membership row (or an unresolved row) as the team to inherit.
		for _, plant := range []func(row *attributionRow){
			func(row *attributionRow) { row.Source = "author_membership" },
			func(row *attributionRow) { row.Source = "unassigned" },
		} {
			planted := withFacts("jira", func(f *matrixFacts) {
				attribution := map[string][]attributionRow{}
				for id, rows := range f.attribution {
					attribution[id] = rows
				}
				for id, item := range f.items {
					if !isPullRequestItem(item) {
						continue
					}
					for donor := range f.linkedDonors(id) {
						rows := append([]attributionRow{}, attribution[donor]...)
						for index := range rows {
							if rows[index].IsPrimary == 1 {
								plant(&rows[index])
							}
						}
						attribution[donor] = rows
					}
				}
				f.attribution = attribution
			})
			if result := judge("jira", "pull_request_inherits_only_from_a_linked_issue", planted, readers); len(result.Violations) == 0 {
				t.Fatal("a pull request team inherited from a donor without a team of its own is not found")
			}
		}
		// A linked_issue row with no resolved team.
		planted := withFacts("jira", func(f *matrixFacts) {
			attribution := map[string][]attributionRow{}
			for id, rows := range f.attribution {
				copied := append([]attributionRow{}, rows...)
				for index := range copied {
					if copied[index].Source == "linked_issue" {
						copied[index].TeamID = ""
					}
				}
				attribution[id] = copied
			}
			f.attribution = attribution
		})
		if result := judge("jira", "pull_request_inherits_only_from_a_linked_issue", planted, readers); len(result.Violations) == 0 {
			t.Fatal("a linked_issue row with no resolved team is not found")
		}
	})

	t.Run("a reader with its own ownership SQL", func(t *testing.T) {
		planted := withReader("github", localOwnershipSQL(ctx, conn, orgs["github"]))
		if result := judge("github", "readers_agree_with_the_ownership_rule", facts, planted); len(result.Violations) == 0 {
			t.Fatal("a reader that counts a closed grant agrees with the ownership rule")
		}
		if result := judge("github", "retired_team_owns_nothing_now", facts, planted); len(result.Violations) == 0 {
			t.Fatal("a reader that still names a gone team as an owner is not found")
		}
	})
}
