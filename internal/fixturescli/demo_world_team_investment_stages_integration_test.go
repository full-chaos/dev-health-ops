//go:build integration

package fixturescli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

var gitLabTeams = map[string]bool{"Data": true, "Delivery": true, "Web": true}

func TestDemoWorldEveryTeamHasInvestmentStages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	world, err := LoadFrozenWorld(demoWorld.Params)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := LoadWorld(ctx, conn, world, demoWorld.OrgID, now); err != nil {
		t.Fatal(err)
	}
	prCommitLoader, err := prcommit.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitWriter, err := prcommit.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	prCommitService, err := prcommit.NewService(prCommitLoader, prCommitWriter, conn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceLinks(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
		t.Fatal(err)
	}
	if _, err := prCommitService.ProduceEdges(ctx, demoWorld.OrgID, prcommit.Window{}); err != nil {
		t.Fatal(err)
	}
	reader, err := chquery.NewReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := investment.NewMaterializer(reader, writer, categorize.MockProvider{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := materializer.Run(ctx, investment.Config{
		OrgID: demoWorld.OrgID, FromTS: now.AddDate(0, 0, -demoDepthDays), ToTS: now,
		RunID: "demo-world-team-stages", ComputedAt: now, ProviderName: "mock",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("materializer: components %d, ownership fallback + repo attribution in stats: %+v", stats.Components, stats)

	type team struct{ id, name string }
	var teams []team
	rows, err := conn.Query(ctx, `SELECT id, name FROM teams FINAL WHERE org_id = ? ORDER BY name`, demoWorld.OrgID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var tm team
		if err := rows.Scan(&tm.id, &tm.name); err != nil {
			t.Fatal(err)
		}
		teams = append(teams, tm)
	}
	rows.Close()
	if len(teams) == 0 {
		t.Fatal("the demo world holds no team")
	}

	var table strings.Builder
	fmt.Fprintf(&table, "\nteam | owned_repos | edges_in_repos | units_via_repo_id | units_via_pr_ref | units_via_issue_ref | units_reached | units_with_distribution | units_effort_gt0 | units_dist_nonempty | status:metric:count\n")
	var missing []string
	for _, tm := range teams {
		cond, bindings := teamscope.RepoCondition(demoWorld.OrgID, "%s", []string{tm.id}, now)
		args := make([]any, 0, len(bindings)+1)
		for _, b := range bindings {
			args = append(args, clickhouse.Named(b.Name, b.Value))
		}
		args = append(args, clickhouse.Named("org_id", demoWorld.OrgID))
		count := func(statement string) uint64 {
			var n uint64
			if err := conn.QueryRow(ctx, statement, args...).Scan(&n); err != nil {
				t.Fatalf("%s: %v\n%s", tm.name, err, statement)
			}
			return n
		}
		owned := count(`SELECT count() FROM repos FINAL WHERE org_id = {org_id:String} AND ` + fmt.Sprintf(cond, "toString(id)"))
		edges := count(`SELECT count() FROM (SELECT edge_id FROM work_graph_edges WHERE org_id = {org_id:String} AND ` + fmt.Sprintf(cond, "toString(repo_id)") + ` GROUP BY edge_id)`)

		with := `WITH (SELECT groupArray(toString(id)) FROM repos FINAL WHERE org_id = {org_id:String} AND ` + fmt.Sprintf(cond, "toString(id)") + `) AS owned_ids,
  (SELECT groupArray((provider, lower(repo))) FROM repos FINAL WHERE org_id = {org_id:String} AND ` + fmt.Sprintf(cond, "toString(id)") + `) AS owned_keys `
		latest := `(SELECT work_unit_id,
    (argMax(tuple(repo_id), computed_at)).1 AS repo_id,
    argMax(theme_distribution_json, computed_at) AS theme_distribution_json,
    argMax(effort_value, computed_at) AS effort_value,
    argMax(structural_evidence_json, computed_at) AS structural_evidence_json
  FROM work_unit_investments WHERE org_id = {org_id:String} GROUP BY work_unit_id)`
		prRefRepo := `arrayMap(r -> splitByString('#pr', r)[1], arrayFilter(r -> match(r, '^[0-9a-fA-F-]{36}#pr[0-9]+$'), JSONExtract(structural_evidence_json, 'prs', 'Array(String)')))`
		issueRefRepo := `arrayMap(r -> (if(startsWith(r, 'ghpr:'), 'github', 'gitlab'), lower(if(startsWith(r, 'ghpr:'), substring(splitByChar('#', r)[1], 6), substring(splitByChar('!', r)[1], 8)))),
            arrayFilter(r -> match(r, '^ghpr:[^#]+#[0-9]+$') OR match(r, '^gitlab:[^!]+![0-9]+$'), JSONExtract(structural_evidence_json, 'issues', 'Array(String)')))`
		byRepoID := `(repo_id IS NOT NULL AND has(owned_ids, toString(repo_id)))`
		byPR := `arrayExists(r -> has(owned_ids, r), ` + prRefRepo + `)`
		byIssue := `arrayExists(k -> has(owned_keys, k), ` + issueRefRepo + `)`
		viaRepoID := count(with + `SELECT count() FROM ` + latest + ` WHERE ` + byRepoID)
		viaPR := count(with + `SELECT count() FROM ` + latest + ` WHERE ` + byPR)
		viaIssue := count(with + `SELECT count() FROM ` + latest + ` WHERE ` + byIssue)
		reachedSQL := `(` + byRepoID + ` OR ` + byPR + ` OR ` + byIssue + `)`
		reached := count(with + `SELECT count() FROM ` + latest + ` WHERE ` + reachedSQL)
		withDistribution := count(with + `SELECT count() FROM ` + latest + ` WHERE length(mapKeys(theme_distribution_json)) > 0 AND effort_value > 0 AND ` + reachedSQL)
		effortPositive := count(with + `SELECT count() FROM ` + latest + ` WHERE effort_value > 0 AND ` + reachedSQL)
		distNonEmpty := count(with + `SELECT count() FROM ` + latest + ` WHERE length(mapKeys(theme_distribution_json)) > 0 AND ` + reachedSQL)
		var breakdown []string
		groups, err := conn.Query(ctx, with+`SELECT status, metric, count() FROM (SELECT argMax(categorization_status, computed_at) AS status, argMax(effort_metric, computed_at) AS metric, argMax(tuple(repo_id), computed_at).1 AS repo_id, argMax(effort_value, computed_at) AS effort_value, argMax(structural_evidence_json, computed_at) AS structural_evidence_json, argMax(theme_distribution_json, computed_at) AS theme_distribution_json FROM work_unit_investments WHERE org_id = {org_id:String} GROUP BY work_unit_id) WHERE `+reachedSQL+` GROUP BY status, metric ORDER BY status, metric`, args...)
		if err != nil {
			t.Fatalf("%s: %v", tm.name, err)
		}
		for groups.Next() {
			var status, metric *string
			var n uint64
			if err := groups.Scan(&status, &metric, &n); err != nil {
				t.Fatal(err)
			}
			breakdown = append(breakdown, fmt.Sprintf("%s:%s:%d", deref(status), deref(metric), n))
		}
		groups.Close()
		fmt.Fprintf(&table, "%s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s\n", tm.name, owned, edges, viaRepoID, viaPR, viaIssue, reached, withDistribution, effortPositive, distNonEmpty, strings.Join(breakdown, ","))
		if withDistribution == 0 {
			missing = append(missing, tm.name)
		}
		if !gitLabTeams[tm.name] && withDistribution != reached {
			t.Errorf("%s (github-backed): %d of %d reached units carry effort and a distribution; adding GitLab links must not change them", tm.name, withDistribution, reached)
		}
	}
	for _, diag := range []struct{ name, sql string }{
		{"pull_requests_by_provider(total, with_line_counts)", `SELECT r.provider, count(), countIf(p.additions + p.deletions > 0) FROM (SELECT repo_id, number, argMax(additions, last_synced) AS additions, argMax(deletions, last_synced) AS deletions FROM git_pull_requests WHERE org_id = ? GROUP BY repo_id, number) AS p INNER JOIN (SELECT id, provider FROM repos FINAL WHERE org_id = ?) AS r ON r.id = p.repo_id GROUP BY r.provider ORDER BY r.provider`},
		{"commit_stats_by_provider(rows, nonzero)", `SELECT r.provider, count(), countIf(s.additions + s.deletions > 0) FROM git_commit_stats AS s INNER JOIN (SELECT id, provider FROM repos FINAL WHERE org_id = ?) AS r ON r.id = s.repo_id WHERE s.org_id = ? GROUP BY r.provider ORDER BY r.provider`},
		{"cycle_times_by_provider(rows, active_hours>0)", `SELECT w.provider, count(), countIf(c.active > 0) FROM (SELECT work_item_id, argMax(active_time_hours, computed_at) AS active FROM work_item_cycle_times WHERE org_id = ? GROUP BY work_item_id) AS c INNER JOIN (SELECT work_item_id, argMax(provider, last_synced) AS provider FROM work_items WHERE org_id = ? GROUP BY work_item_id) AS w ON w.work_item_id = c.work_item_id GROUP BY w.provider ORDER BY w.provider`},
	} {
		diagRows, err := conn.Query(ctx, diag.sql, demoWorld.OrgID, demoWorld.OrgID)
		if err != nil {
			t.Logf("%s: %v", diag.name, err)
			continue
		}
		var line []string
		for diagRows.Next() {
			var provider string
			var total, positive uint64
			if err := diagRows.Scan(&provider, &total, &positive); err != nil {
				t.Fatal(err)
			}
			line = append(line, fmt.Sprintf("%s=%d/%d", provider, total, positive))
		}
		diagRows.Close()
		t.Logf("%s: %s", diag.name, strings.Join(line, " "))
	}
	t.Log(table.String())
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("teams with no persisted investment distribution reachable through their owned repositories: %v", missing)
	}
}

func deref(s *string) string {
	if s == nil {
		return "null"
	}
	return *s
}
