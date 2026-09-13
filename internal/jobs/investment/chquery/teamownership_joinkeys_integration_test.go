//go:build integration

package chquery

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fanOutTeamRepoDonorsQuery is the donor query as it stood while its only
// DISTINCT ran after the attributions-to-ownership join. It is kept ONLY as
// the before/after oracle for the tests here and in the two-version parity
// test, and must never be reintroduced into the production query: on a team
// with many attributions and many ownership rows the join materializes the
// product of both multiplicities before the DISTINCT collapses it.
const fanOutTeamRepoDonorsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM (
        SELECT work_item_id, team_id, source, is_primary, computed_at,
               max(computed_at) OVER (PARTITION BY work_item_id) AS latest_computed_at
        FROM work_item_team_attributions FINAL
        WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
    )
    WHERE computed_at = latest_computed_at
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
),
ownership AS (
    SELECT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
),
team_repos AS (
    SELECT o.team_id AS team_id, r.id AS repo_id
    FROM ownership AS o
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id
    WHERE o.repo_id IS NOT NULL

    UNION ALL

    SELECT o.team_id AS team_id, r.id AS repo_id
    FROM ownership AS o
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
    WHERE o.repo_id IS NULL
)
SELECT DISTINCT a.work_item_id, assumeNotNull(a.team_id), tr.repo_id
FROM attributions AS a
INNER JOIN active_teams AS t ON t.id = a.team_id
INNER JOIN team_repos AS tr ON tr.team_id = a.team_id
WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND a.team_id IS NOT NULL AND a.team_id != ''
  AND tr.repo_id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY a.work_item_id, tr.repo_id, a.team_id`

type donorQueryCost struct {
	joinResultRows uint64
	resultRows     uint64
	readRows       uint64
	queryHash      uint64
	queryText      string
}

func taggedDonorContext(ctx context.Context, tag string) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": tag}))
}

// readDonorQueryCost reads the server's own accounting of one finished query.
// JoinResultRowCount is summed over every join in the plan, so it counts the
// rows each join emitted before any later DISTINCT or filter saw them.
func readDonorQueryCost(t *testing.T, ctx context.Context, conn driver.Conn, tag string) donorQueryCost {
	t.Helper()
	mustExec(t, ctx, conn, "SYSTEM FLUSH LOGS")
	var cost donorQueryCost
	if err := conn.QueryRow(ctx, `SELECT ProfileEvents['JoinResultRowCount'], result_rows, read_rows, normalized_query_hash, query
FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = ? ORDER BY event_time_microseconds DESC LIMIT 1`, tag).
		Scan(&cost.joinResultRows, &cost.resultRows, &cost.readRows, &cost.queryHash, &cost.queryText); err != nil {
		t.Fatalf("read query_log for %s: %v", tag, err)
	}
	return cost
}

// seedFanOutDonorCorpus writes the shape that makes the attributions-to-
// ownership join explode in production: few teams, each attributed many work
// items through several eligible primary sources, and each owning many
// repositories through several ownership rows (the id arm, the name arm under
// three sources and differing casings, and a second open validity window).
// Every duplicate here is legitimate evidence that collapses to one donor
// tuple, so the output is exactly teams x workItemsPerTeam x reposPerTeam.
func seedFanOutDonorCorpus(t *testing.T, ctx context.Context, conn driver.Conn, at time.Time, teams, reposPerTeam, workItemsPerTeam int) []string {
	t.Helper()
	repoID := "toUUID(concat(leftPad(lower(hex(number)), 8, '0'), '-0000-4000-8000-', leftPad(lower(hex(number)), 12, '0')))"
	teamOf := fmt.Sprintf("concat('team-', toString(number %% %d))", teams)

	mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id)
SELECT concat('team-', toString(number)), generateUUIDv4(), 'T', 'linear', 1, ?, ? FROM numbers(%d)`, teams), at, orgAlpha)
	mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO repos (id,repo,provider,org_id,last_synced)
SELECT %s, concat('acme/repo-', toString(number)), 'github', ?, ? FROM numbers(%d)`, repoID, teams*reposPerTeam), orgAlpha, at)
	mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at)
SELECT ?, 'github', %s, %s, concat('acme/repo-', toString(number)), 'exact', 'native', ?, NULL, ? FROM numbers(%d)`, teamOf, repoID, teams*reposPerTeam),
		orgAlpha, at.Add(-time.Hour), at)
	for _, row := range []struct {
		source, name string
		validFrom    time.Duration
	}{
		{"provider_access", "concat('acme/repo-', toString(number))", -time.Hour},
		{"provider_access", "concat('acme/repo-', toString(number))", -2 * time.Hour},
		{"inferred", "concat('Acme/Repo-', toString(number))", -time.Hour},
		{"jira_legacy", "concat('ACME/REPO-', toString(number))", -time.Hour},
	} {
		mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at)
SELECT ?, 'github', %s, NULL, %s, 'exact', ?, ?, NULL, ? FROM numbers(%d)`, teamOf, row.name, teams*reposPerTeam),
			orgAlpha, row.source, at.Add(row.validFrom), at)
	}
	// Production work item ids are long external keys; a hundred-odd bytes each
	// is what made the inline list the size of the query itself.
	prefix := "extkey:" + strings.Repeat("k", 112) + "-"
	mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at)
SELECT ?, toUUID('00000000-0000-0000-0000-000000000000'), concat(?, toString(number)), 'linear', %s, source, 1, 'high', 'e', ?
FROM numbers(%d) ARRAY JOIN ['native_team', 'project_ownership', 'repo_ownership'] AS source`, teamOf, teams*workItemsPerTeam),
		orgAlpha, prefix, at)
	ids := make([]string, teams*workItemsPerTeam)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return ids
}

// TestDonorJoinSidesAreReducedToDistinctKeys pins where the donor query's
// DISTINCT runs. Materialization needs one row per (work item, team,
// repository), and both sides of the team_id join carry legitimate duplicates:
// several eligible primary sources per work item and team, and several
// ownership rows per team and repository. Collapsing only after the join makes
// the join emit the product of the two multiplicities -- in production about
// 297 million rows for a result a few orders of magnitude smaller. Reduced to
// distinct keys first, each join can emit at most one row per row it was fed
// plus one per output row, which is the bound asserted here, independently of
// any oracle; the before/after ratio and byte-equal output are asserted too so
// the bound cannot be met by returning less.
func TestDonorJoinSidesAreReducedToDistinctKeys(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	const teams, reposPerTeam, workItemsPerTeam = 3, 150, 300
	ids := seedFanOutDonorCorpus(t, ctx, conn, at, teams, reposPerTeam, workItemsPerTeam)

	before := runDonorQuery(t, taggedDonorContext(ctx, "donor-join-before"), conn, fanOutTeamRepoDonorsQuery, ids, orgAlpha, at)
	after, err := reader.FetchTeamRepoDonors(taggedDonorContext(ctx, "donor-join-after"), ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	if want := teams * reposPerTeam * workItemsPerTeam; len(before) != want {
		t.Fatalf("oracle returned %d donors, want %d; the corpus is not reaching the join", len(before), want)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("reduced-key donor query diverged from the fan-out form: %d rows vs %d", len(after), len(before))
	}

	beforeCost := readDonorQueryCost(t, ctx, conn, "donor-join-before")
	afterCost := readDonorQueryCost(t, ctx, conn, "donor-join-after")
	t.Logf("JoinResultRowCount before=%d after=%d; result_rows=%d; read_rows before=%d after=%d; normalized_query_hash before=%d after=%d",
		beforeCost.joinResultRows, afterCost.joinResultRows, afterCost.resultRows, beforeCost.readRows, afterCost.readRows,
		beforeCost.queryHash, afterCost.queryHash)
	if afterCost.resultRows != uint64(len(after)) {
		t.Fatalf("query_log result_rows=%d, want %d; the tagged row is not this query", afterCost.resultRows, len(after))
	}
	if limit := afterCost.resultRows + afterCost.readRows; afterCost.joinResultRows > limit {
		t.Fatalf("joins emitted %d rows, more than the %d output rows plus the %d rows read; a join side still carries duplicate keys",
			afterCost.joinResultRows, afterCost.resultRows, afterCost.readRows)
	}
	if afterCost.joinResultRows*10 > beforeCost.joinResultRows {
		t.Fatalf("joins emitted %d rows against %d for the fan-out form; want at least a tenfold cut",
			afterCost.joinResultRows, beforeCost.joinResultRows)
	}
	// A parameter is substituted into the logged text, where hundreds of long
	// ids push the query past log_queries_cut_to_length and give every form of
	// it one normalized hash. The ids must stay out of the text.
	if strings.Contains(afterCost.queryText, ids[0]) || len(afterCost.queryText) > 8<<10 {
		t.Fatalf("the logged donor query carries the work item ids inline (%d bytes)", len(afterCost.queryText))
	}
	if afterCost.queryHash == beforeCost.queryHash {
		t.Fatalf("both donor query forms log normalized_query_hash %d; the logged text is not the query", afterCost.queryHash)
	}
}
