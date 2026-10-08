//go:build integration

package remaining

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

var coOwnerProviders = []string{"jira", "gitlab", "github", "linear"}

type storedAttribution struct {
	teamID    string
	source    string
	isPrimary uint8
	evidence  string
}

// latestAttributions reads an item's rows the way the readers do: FINAL and
// the newest computed_at of the item.
func latestAttributions(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string) []storedAttribution {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT ifNull(team_id, ''), toString(source), is_primary, evidence
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND work_item_id = ?
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at) FROM work_item_team_attributions
      WHERE org_id = ? AND work_item_id = ? GROUP BY work_item_id)
ORDER BY ifNull(team_id, ''), source`, orgID, workItemID, orgID, workItemID)
	if err != nil {
		t.Fatalf("read attributions: %v", err)
	}
	defer rows.Close()
	var result []storedAttribution
	for rows.Next() {
		var row storedAttribution
		if err := rows.Scan(&row.teamID, &row.source, &row.isPrimary, &row.evidence); err != nil {
			t.Fatalf("scan attribution: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate attributions: %v", err)
	}
	return result
}

// teamsWith returns the teams holding the given is_primary value on source.
func teamsWith(rows []storedAttribution, source string, isPrimary uint8) []string {
	var result []string
	for _, row := range rows {
		if row.source == source && row.isPrimary == isPrimary {
			result = append(result, row.teamID)
		}
	}
	sort.Strings(result)
	return result
}

// An item of a project connected to two active teams and one inactive team,
// for every provider, through the real fact loaders, the real cascade and the
// real writer into a store built from the migration chain: team A has the one
// primary row (1), team B a co-owner row (2) from the same source with its own
// evidence, the inactive team C no row. A later run in which a new team ranks
// first moves A from 1 to 2: the newest row per (item, team, source) key wins,
// also while merges are stopped, and the item still has exactly one primary
// row.
func TestAnItemOfAProjectOfSeveralTeamsIsWrittenForEveryActiveTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES work_item_team_attributions"); err != nil {
		t.Fatalf("stop merges: %v", err)
	}
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	orgID := "org-coowner-" + uuid.NewString()
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertTeam := func(provider, teamID, key string, active uint8, at time.Time) {
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], ?, ?, ?, ?, ?, ?)`,
			teamID, uuid.New(), "Team "+teamID, []string{key}, active, at, at, orgID, provider, teamID); err != nil {
			t.Fatalf("insert team %s: %v", teamID, err)
		}
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, ?, ?, ?, ?, 'native', 1, 110, 10, ?, NULL, ?)`,
			orgID, provider, teamID, "proj-"+provider, key, opened, at); err != nil {
			t.Fatalf("insert ownership %s: %v", teamID, err)
		}
	}
	repoID := uuid.New()
	repoIDText := repoID.String()
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	affected := map[string]struct{}{}
	for _, provider := range coOwnerProviders {
		key := "KEY" + strings.ToUpper(provider)
		insertTeam(provider, "b-team-a-"+provider, key, 1, opened)
		insertTeam(provider, "b-team-b-"+provider, key, 1, opened)
		insertTeam(provider, "b-team-c-"+provider, key, 0, opened)
		projectID := "proj-" + provider
		projectKey := key
		id := provider + ":" + key + "-1"
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{
			WorkItemID: id, Provider: provider, RepoID: &repoIDText,
			ProjectKey: &projectKey, ProjectID: &projectID, OrgID: orgID,
		}
		affected[id] = struct{}{}
	}

	write := func(computedAt time.Time) {
		facts, err := LoadWorkItemDerivationFacts(ctx, conn, orgID, computedAt)
		if err != nil {
			t.Fatalf("load facts: %v", err)
		}
		derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
		rows := BuildWorkItemAttributionRows(orgID, computedAt, affected, subjects, derived)
		if _, err := writer.WriteAttributions(ctx, WorkItemAttributionProducer{
			Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString(),
		}, rows); err != nil {
			t.Fatalf("write attributions: %v", err)
		}
	}

	firstRun := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	write(firstRun)
	for _, provider := range coOwnerProviders {
		id := provider + ":KEY" + strings.ToUpper(provider) + "-1"
		rows := latestAttributions(t, ctx, conn, orgID, id)
		a, b, c := "b-team-a-"+provider, "b-team-b-"+provider, "b-team-c-"+provider
		if got := teamsWith(rows, "issue_project", 1); strings.Join(got, ",") != a {
			t.Errorf("%s: primary teams = %v, want [%s]", provider, got, a)
		}
		if got := teamsWith(rows, "issue_project", 2); strings.Join(got, ",") != b {
			t.Errorf("%s: co-owner teams = %v, want [%s]", provider, got, b)
		}
		for _, row := range rows {
			if row.teamID == c {
				t.Errorf("%s: inactive team has a row: %+v", provider, row)
			}
			if strings.TrimSpace(row.evidence) == "" || row.source == "" {
				t.Errorf("%s: row without provenance: %+v", provider, row)
			}
		}
	}

	// A new team that ranks first (lower id) joins every project: A moves
	// from 1 to 2 under the same sort key.
	secondRun := firstRun.Add(24 * time.Hour)
	for _, provider := range coOwnerProviders {
		insertTeam(provider, "a-team-first-"+provider, "KEY"+strings.ToUpper(provider), 1, secondRun)
	}
	write(secondRun)
	for _, provider := range coOwnerProviders {
		id := provider + ":KEY" + strings.ToUpper(provider) + "-1"
		rows := latestAttributions(t, ctx, conn, orgID, id)
		first, a, b := "a-team-first-"+provider, "b-team-a-"+provider, "b-team-b-"+provider
		if got := teamsWith(rows, "issue_project", 1); strings.Join(got, ",") != first {
			t.Errorf("%s run 2: primary teams = %v, want [%s]", provider, got, first)
		}
		if got := teamsWith(rows, "issue_project", 2); strings.Join(got, ",") != a+","+b {
			t.Errorf("%s run 2: co-owner teams = %v, want [%s %s]", provider, got, a, b)
		}
		var physical uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_team_attributions
			WHERE org_id = ? AND work_item_id = ? AND team_id = ? AND source = 'issue_project'`,
			orgID, id, a).Scan(&physical); err != nil {
			t.Fatalf("count physical rows: %v", err)
		}
		if physical < 2 {
			t.Fatalf("%s: %d physical rows of team A's key, want both versions stored (merges stopped)", provider, physical)
		}
	}
}
