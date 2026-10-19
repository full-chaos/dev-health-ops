//go:build integration

package fixturescli

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The full-tie cell of the attribution matrix (CHAOS-9533, D5990): a full tie is a repository that two or
// more teams reach with equal link counts at every tier. Every tied team is a main owner of it (a row with
// is_primary 1, equal shares), and every shared ownership reader names every one of them.
//
// None of the provider worlds holds such a repository: their inferred owners are one main owner, or one main
// owner and a co-owner, and the github world's only tie is a pair of provider grants (no inference, no main
// row). The matrix therefore adds one to each world before the production verbs run: provider-shaped source
// rows only -- a repository, two pull requests, two linked issues of two different teams, and the project
// ownership those issues need -- cloned from rows the world already holds. The derivation writes the
// ownership itself; nothing here writes a team_repo_ownership row.

// matrixFullTie is the full tie added to one provider world.
type matrixFullTie struct {
	RepoID, RepoName string
	// Teams are every team of the tie.
	Teams []string
	// Providers are the providers whose cells count the tie: the repository's, and the issues'.
	Providers []string
}

func (tie matrixFullTie) counts(provider string) bool {
	for _, candidate := range tie.Providers {
		if candidate == provider {
			return true
		}
	}
	return false
}

// fullTieLeg is one side of a tie: a team, the issue that gives it one link to the repository, and the
// project ownership that makes the team the issue's.
type fullTieLeg struct {
	Team string
	// Project is the issue's project. When NewProject, the matrix adds the project's ownership by Team (and
	// its catalog row when ProjectCatalogClone names the project to clone); otherwise the issue keeps the
	// project it is cloned with (a Linear issue resolves its team through its native team key).
	Project, ProjectKey, ProjectName string
	NewProject                       bool
	ProjectCatalogClone              string
	// CatalogNativeID, for a GitLab leg, is the new project's numeric id: its
	// ownership row is keyed by the catalog id {org}:gitlab:{id} with the path
	// as project_key, and a catalog row is added, as the GitLab writer does.
	// The issue's project_id is the catalog id and its project_key the path.
	CatalogNativeID   string
	Issue, IssueClone string
	NativeTeamKey     string
}

// fullTieSeed is the tie added to one provider world.
type fullTieSeed struct {
	RepoID, RepoName, RepoClone string
	RepoProvider                string
	// IssuesInRepo puts the issues in the tie repository (a repository host's issues live in a repository).
	IssuesInRepo bool
	// Ownership is the shape of the project ownership rows the provider writes.
	OwnershipSource      string
	OwnershipPrimary     int
	OwnershipSpecificity int
	OwnershipPriority    int
	Legs                 [2]fullTieLeg
}

var fullTieSeeds = map[string]fullTieSeed{
	"github": {
		RepoID: "f1c7e000-0000-4000-8000-00000000f101", RepoName: "chaos-fixtures/tie-core", RepoClone: "chaos-fixtures/platform-core", RepoProvider: "github",
		IssuesInRepo:    true,
		OwnershipSource: "native", OwnershipPrimary: 1, OwnershipSpecificity: 110, OwnershipPriority: 10,
		Legs: [2]fullTieLeg{
			{Team: "gh:payments", Project: "chaos-fixtures/tie-payments", NewProject: true, Issue: "gh:chaos-fixtures/tie-core#1", IssueClone: "gh:chaos-fixtures/payments-api#2"},
			{Team: "gh:platform", Project: "chaos-fixtures/tie-platform", NewProject: true, Issue: "gh:chaos-fixtures/tie-core#2", IssueClone: "gh:chaos-fixtures/payments-api#2"},
		},
	},
	"gitlab": {
		RepoID: "f1c7e000-0000-4000-8000-00000000f102", RepoName: "chaos-gl/tie-core", RepoClone: "chaos-gl/billing-service", RepoProvider: "gitlab",
		IssuesInRepo:    true,
		OwnershipSource: "provider_access", OwnershipPrimary: 0, OwnershipSpecificity: 110, OwnershipPriority: 300,
		Legs: [2]fullTieLeg{
			{Team: "gl:chaos-gl/checkout", Project: "chaos-gl/tie-checkout", NewProject: true, CatalogNativeID: "91001", Issue: "gitlab:chaos-gl/tie-core#1", IssueClone: "gitlab:chaos-gl/billing-service#2"},
			{Team: "gl:chaos-gl", Project: "chaos-gl/tie-group", NewProject: true, CatalogNativeID: "91002", Issue: "gitlab:chaos-gl/tie-core#2", IssueClone: "gitlab:chaos-gl/billing-service#2"},
		},
	},
	"jira": {
		RepoID: "f1c7e000-0000-4000-8000-00000000f103", RepoName: "chaos-fixtures/tie-ledger", RepoClone: "chaos-fixtures/ledger", RepoProvider: "github",
		OwnershipSource: "native", OwnershipPrimary: 1, OwnershipSpecificity: 110, OwnershipPriority: 10,
		Legs: [2]fullTieLeg{
			{Team: "jira:9a1f0c3e-0000-4000-8000-0000000a0001", Project: "10003", ProjectKey: "FIN", ProjectName: "Finance", NewProject: true, ProjectCatalogClone: "10002", Issue: "jira:FIN-1", IssueClone: "jira:PAY-1"},
			// OPS is owned by the second team alone: the existing project and its ownership are the leg's.
			{Team: "jira:9a1f0c3e-0000-4000-8000-0000000a0002", Issue: "jira:OPS-9", IssueClone: "jira:OPS-1"},
		},
	},
	"linear": {
		RepoID: "f1c7e000-0000-4000-8000-00000000f104", RepoName: "chaos-fixtures/tie-ledger", RepoClone: "chaos-fixtures/ledger", RepoProvider: "github",
		Legs: [2]fullTieLeg{
			{Team: "linear:ENG", Issue: "linear:ENG-9", IssueClone: "linear:ENG-1", NativeTeamKey: "ENG"},
			{Team: "linear:MOB", Issue: "linear:MOB-9", IssueClone: "linear:MOB-1", NativeTeamKey: "MOB"},
		},
	},
}

func sqlQuote(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

func sqlTime(at time.Time, scale int) string {
	return fmt.Sprintf("toDateTime64('%s', %d, 'UTC')", at.UTC().Format("2006-01-02 15:04:05"), scale)
}

// seedFullTie adds the provider's tie to its loaded world, before the production verbs run, and returns
// what it added. It fails the test when a row it clones is missing: a world that changed under it.
func seedFullTie(ctx context.Context, t *testing.T, conn driver.Conn, provider, org string, now time.Time) matrixFullTie {
	t.Helper()
	seed, ok := fullTieSeeds[provider]
	if !ok {
		t.Fatalf("no full tie is seeded for %s", provider)
	}
	day := now.UTC().Truncate(24 * time.Hour)
	ownedFrom := day.AddDate(0, 0, -90)
	issueAt := day.AddDate(0, 0, -70).Add(9 * time.Hour)
	issueDone := issueAt.AddDate(0, 0, 5)
	prAt := [2]time.Time{day.AddDate(0, 0, -60).Add(9 * time.Hour), day.AddDate(0, 0, -50).Add(9 * time.Hour)}
	exec := func(what, statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("full tie of %s: %s: %v", provider, what, err)
		}
	}
	requireRows := func(what, query string) {
		t.Helper()
		var n uint64
		rows, err := conn.Query(ctx, query)
		if err != nil {
			t.Fatalf("full tie of %s: %s: %v", provider, what, err)
		}
		defer rows.Close()
		if rows.Next() {
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
		}
		if n == 0 {
			t.Fatalf("full tie of %s: %s: the query finds no row", provider, what)
		}
	}
	orgLit := sqlQuote(org)
	// A cloned row is read in a subquery: a REPLACE alias also names the column in the WHERE of the same
	// SELECT, so filtering on a replaced column there finds no row.

	requireRows("repository", fmt.Sprintf(`SELECT count() FROM repos FINAL WHERE org_id = %s AND repo = %s`, orgLit, sqlQuote(seed.RepoClone)))
	exec("repository", fmt.Sprintf(`INSERT INTO repos SELECT * REPLACE (toUUID(%s) AS id, %s AS repo,
	replaceAll(ifNull(settings, ''), %s, %s) AS settings)
FROM (SELECT * FROM repos FINAL WHERE org_id = %s AND repo = %s LIMIT 1)`,
		sqlQuote(seed.RepoID), sqlQuote(seed.RepoName),
		sqlQuote(seed.RepoClone[strings.LastIndex(seed.RepoClone, "/")+1:]), sqlQuote(seed.RepoName[strings.LastIndex(seed.RepoName, "/")+1:]),
		orgLit, sqlQuote(seed.RepoClone)))

	requireRows("pull request", fmt.Sprintf(`SELECT count() FROM git_pull_requests FINAL WHERE org_id = %s AND state = 'merged'`, orgLit))
	for index, at := range prAt {
		exec("pull request", fmt.Sprintf(`INSERT INTO git_pull_requests SELECT * REPLACE (toUUID(%s) AS repo_id, toUInt32(%d) AS number,
	%s AS created_at, %s AS merged_at, %s AS closed_at, 'merged' AS state,
	CAST(NULL AS Nullable(DateTime64(3, 'UTC'))) AS first_review_at, CAST(NULL AS Nullable(DateTime64(3, 'UTC'))) AS first_comment_at)
FROM (SELECT * FROM git_pull_requests FINAL WHERE org_id = %s AND state = 'merged' ORDER BY repo_id, number LIMIT 1)`,
			sqlQuote(seed.RepoID), index+1, sqlTime(at, 3), sqlTime(at.Add(48*time.Hour), 3), sqlTime(at.Add(48*time.Hour), 3), orgLit))
	}

	requireRows("added repository", fmt.Sprintf(`SELECT count() FROM repos FINAL WHERE org_id = %s AND id = toUUID(%s)`, orgLit, sqlQuote(seed.RepoID)))
	requireRows("added pull requests", fmt.Sprintf(`SELECT count() FROM git_pull_requests FINAL WHERE org_id = %s AND repo_id = toUUID(%s)`, orgLit, sqlQuote(seed.RepoID)))
	tie := matrixFullTie{RepoID: seed.RepoID, RepoName: seed.RepoName, Providers: []string{seed.RepoProvider, provider}}
	for index, leg := range seed.Legs {
		tie.Teams = append(tie.Teams, leg.Team)
		if leg.NewProject {
			ownershipProjectID := leg.Project
			if leg.CatalogNativeID != "" {
				ownershipProjectID = org + ":gitlab:" + leg.CatalogNativeID
				exec("catalog project", fmt.Sprintf(`INSERT INTO projects SELECT * REPLACE (%s AS id, %s AS project_key, %s AS name)
FROM (SELECT * FROM projects FINAL WHERE org_id = %s AND provider = 'gitlab' LIMIT 1)`,
					sqlQuote(ownershipProjectID), sqlQuote(leg.Project), sqlQuote(leg.Project), orgLit))
				requireRows("catalog project", fmt.Sprintf(`SELECT count() FROM projects FINAL WHERE org_id = %s AND id = %s`, orgLit, sqlQuote(ownershipProjectID)))
			}
			exec("project ownership", fmt.Sprintf(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at)
SELECT %s, %s, %s, %s, %s, %s, %d, %d, %d, %s, %s`,
				orgLit, sqlQuote(provider), sqlQuote(leg.Team), sqlQuote(ownershipProjectID), sqlQuote(firstNonEmpty(leg.ProjectKey, leg.Project)),
				sqlQuote(seed.OwnershipSource), seed.OwnershipPrimary, seed.OwnershipSpecificity, seed.OwnershipPriority, sqlTime(ownedFrom, 3), sqlTime(ownedFrom, 3)))
		}
		if leg.ProjectCatalogClone != "" {
			requireRows("project", fmt.Sprintf(`SELECT count() FROM projects FINAL WHERE org_id = %s AND id = %s`, orgLit, sqlQuote(leg.ProjectCatalogClone)))
			exec("project", fmt.Sprintf(`INSERT INTO projects SELECT * REPLACE (%s AS id, %s AS project_key, %s AS name)
FROM (SELECT * FROM projects FINAL WHERE org_id = %s AND id = %s LIMIT 1)`,
				sqlQuote(leg.Project), sqlQuote(leg.ProjectKey), sqlQuote(leg.ProjectName), orgLit, sqlQuote(leg.ProjectCatalogClone)))
		}
		requireRows("issue", fmt.Sprintf(`SELECT count() FROM work_items FINAL WHERE org_id = %s AND work_item_id = %s`, orgLit, sqlQuote(leg.IssueClone)))
		replace := []string{
			sqlQuote(leg.Issue) + " AS work_item_id",
			sqlTime(issueAt, 3) + " AS created_at",
			sqlTime(issueDone, 3) + " AS updated_at",
			"CAST(NULL AS Nullable(DateTime64(3))) AS started_at",
			fmt.Sprintf("if(isNull(completed_at), CAST(NULL AS Nullable(DateTime64(3))), %s) AS completed_at", sqlTime(issueDone, 3)),
			fmt.Sprintf("if(isNull(closed_at), CAST(NULL AS Nullable(DateTime64(3))), %s) AS closed_at", sqlTime(issueDone, 3)),
		}
		if seed.IssuesInRepo {
			replace = append(replace, fmt.Sprintf("toUUID(%s) AS repo_id", sqlQuote(seed.RepoID)))
		}
		if leg.NewProject {
			if leg.CatalogNativeID != "" {
				replace = append(replace, sqlQuote(org+":gitlab:"+leg.CatalogNativeID)+" AS project_id", sqlQuote(leg.Project)+" AS project_key")
			} else {
				replace = append(replace, sqlQuote(leg.Project)+" AS project_id")
			}
			if leg.ProjectKey != "" {
				replace = append(replace, sqlQuote(leg.ProjectKey)+" AS project_key", sqlQuote(leg.ProjectName)+" AS project_name")
			}
		}
		if leg.NativeTeamKey != "" {
			replace = append(replace, sqlQuote(leg.NativeTeamKey)+" AS native_team_key")
		}
		exec("issue", fmt.Sprintf(`INSERT INTO work_items SELECT * REPLACE (%s)
FROM (SELECT * FROM work_items FINAL WHERE org_id = %s AND work_item_id = %s LIMIT 1)`, strings.Join(replace, ", "), orgLit, sqlQuote(leg.IssueClone)))
		requireRows("added issue", fmt.Sprintf(`SELECT count() FROM work_items FINAL WHERE org_id = %s AND work_item_id = %s`, orgLit, sqlQuote(leg.Issue)))
		exec("issue to pull request link", fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
SELECT toUUID(%s), %s, %d, 0.9, 'explicit_text', 'full_tie_fixture', %s, %s`,
			sqlQuote(seed.RepoID), sqlQuote(leg.Issue), index+1, sqlTime(now, 3), orgLit))
	}
	t.Logf("attribution matrix: full tie added to the %s world: %s held by %s", provider, seed.RepoName, strings.Join(tie.Teams, ", "))
	return tie
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// checkFullTieOwners judges every full tie the provider's cells count: every tied team holds an open main
// row (is_primary 1) for the repository, and every shared reader returns every one of them (D5990).
func checkFullTieOwners(f matrixFacts, provider string, readers ownerReaders) (r checkResult) {
	for _, tie := range f.fullTies {
		if !tie.counts(provider) {
			continue
		}
		rows := f.repoOwnerRows(tie.RepoID)
		for _, team := range tie.Teams {
			r.Subjects++
			main := false
			for _, row := range rows {
				if row.TeamID == team && row.IsPrimary == 1 && row.validAt(f.now) {
					main = true
				}
			}
			if !main {
				r.Violations = append(r.Violations, fmt.Sprintf("%s holds no main row (is_primary 1) for the tied repository %s", team, tie.RepoName))
			}
		}
		readerAnswers := map[string]func(time.Time) (map[string][]string, error){
			"OwnersByRepo":              readers.OwnersByRepo,
			"AuthoritativeOwnersByRepo": readers.Authoritative,
		}
		names := make([]string, 0, len(readerAnswers))
		for name := range readerAnswers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			answer, err := readerAnswers[name](f.now)
			if err != nil {
				r.Violations = append(r.Violations, name+": "+err.Error())
				continue
			}
			got := map[string]bool{}
			for _, team := range answer[tie.RepoID] {
				got[team] = true
			}
			for _, team := range tie.Teams {
				r.Subjects++
				if !got[team] {
					r.Violations = append(r.Violations, fmt.Sprintf("%s for %s returns %v, not the tied team %s", name, tie.RepoName, sortedTeamSet(got), team))
				}
			}
		}
	}
	return r
}

// rediscoverFullTieDefects plants each defect the full-tie cell exists to catch into the produced state and
// requires the cell to turn red on it, and to be green on the state as produced.
func rediscoverFullTieDefects(t *testing.T, facts map[string]matrixFacts, readers map[string]ownerReaders) {
	t.Helper()
	const check = "full_tie_owners_are_all_main"
	judge := func(provider string, f map[string]matrixFacts, r map[string]ownerReaders) checkResult {
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
	for _, provider := range attributionProviders {
		provider := provider
		tie := facts[provider].fullTies[0]
		if result := judge(provider, facts, readers); len(result.Violations) > 0 {
			t.Fatalf("%s: a violation before any plant: %v", provider, result.Violations)
		}
		t.Run(provider+" tied team written as a co-owner", func(t *testing.T) {
			// A writer that gives only the first tied team the main row and keeps the others as co-owners (2).
			planted := map[string]matrixFacts{}
			for key, value := range facts {
				planted[key] = value
			}
			f := facts[provider]
			rows := append([]ownershipRow{}, f.repoOwners...)
			demoted := false
			for index := range rows {
				if rows[index].TeamID == tie.Teams[1] && (rows[index].RepoID == tie.RepoID || strings.EqualFold(rows[index].RepoName, tie.RepoName)) {
					rows[index].IsPrimary = 2
					demoted = true
				}
			}
			if !demoted {
				t.Fatalf("%s: no row of the tied team %s to plant on", provider, tie.Teams[1])
			}
			f.repoOwners = rows
			planted[provider] = f
			if result := judge(provider, planted, readers); len(result.Violations) == 0 {
				t.Fatal("a tied team written as a co-owner is not found")
			}
		})
		t.Run(provider+" readers that name one tied owner", func(t *testing.T) {
			// Readers that answer the first tied team only (the deterministic order of the cascade's primary).
			first := func(read func(time.Time) (map[string][]string, error)) func(time.Time) (map[string][]string, error) {
				return func(at time.Time) (map[string][]string, error) {
					out, err := read(at)
					if err != nil {
						return nil, err
					}
					if owners := out[tie.RepoID]; len(owners) > 1 {
						out[tie.RepoID] = owners[:1]
					}
					return out, nil
				}
			}
			for _, which := range []string{"OwnersByRepo", "AuthoritativeOwnersByRepo"} {
				planted := map[string]ownerReaders{}
				for key, value := range readers {
					planted[key] = value
				}
				reader := readers[provider]
				if which == "OwnersByRepo" {
					reader.OwnersByRepo = first(reader.OwnersByRepo)
				} else {
					reader.Authoritative = first(reader.Authoritative)
				}
				planted[provider] = reader
				if result := judge(provider, facts, planted); len(result.Violations) == 0 {
					t.Fatalf("a %s that names one tied owner is not found", which)
				}
			}
		})
	}
}
