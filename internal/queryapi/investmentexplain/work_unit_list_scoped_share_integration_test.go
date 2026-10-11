//go:build integration

package investmentexplain

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-9248: the work-unit LIST in a REPOSITORY scope carries the scope's repository and the unit's ALLOCATED share
// there, so the sum of the list rows in a repository scope is that scope's allocation total (the evidence table
// sums them). Team scope and org scope read the whole unit, as before (asserted, not assumed).
//
//	ux: effort 100 allocated 60 to repo A / 40 to repo B, own repo_id = A
//	u1: effort 10, own repo_id = A, no allocation row
func TestWorkUnitListInARepositoryScopeCarriesTheAllocatedShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	const org = "org-9248"
	const repoA, repoB = "92480001-9248-4248-8248-924892489248", "92480002-9248-4248-8248-924892489248"
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}} {
		exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, at, at)
	}
	for _, o := range [][3]string{{"team-a", repoA, "acme/a"}, {"team-b", repoB, "acme/b"}} {
		exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, ?, ?, 'exact', 'native', 1, 100, 0, ?, NULL, ?)`,
			org, o[0], o[1], o[2], at.AddDate(-1, 0, 0), at)
	}
	raw, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	unit := func(id, own string, effort float64) {
		exec(`INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`, id, at.Add(-24*time.Hour), at, own, effort,
			map[string]float64{"feature_delivery": 1}, map[string]float64{"feature_delivery.roadmap": 1}, string(raw), at.Add(time.Hour), org)
	}
	unit("ux", repoA, 100)
	for repo, v := range map[string]float64{repoA: 60, repoB: 40} {
		exec(`INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES ('ux', ?, 'churn_loc', ?, ?, 'pr_refs', 'run-1', ?, ?)`, repo, v, v/100, at.Add(time.Hour), org)
	}
	unit("u1", repoA, 10)

	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	read := func(filter WorkUnitInvestmentsFilter) map[string][2]any {
		t.Helper()
		filter.OrgID, filter.StartTS, filter.EndTS, filter.Limit = org, at.AddDate(0, 0, -3), at.AddDate(0, 0, 3), 100
		rows, err := reader.FetchWorkUnitInvestments(ctx, filter)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string][2]any{}
		for _, row := range rows {
			repo, effort := "", math.NaN()
			if row.RepoID != nil {
				repo = *row.RepoID
			}
			if row.EffortValue != nil {
				effort = *row.EffortValue
			}
			out[row.WorkUnitID+"@"+repo] = [2]any{repo, effort}
		}
		return out
	}
	sum := func(rows map[string][2]any) float64 {
		var total float64
		for _, v := range rows {
			total += v[1].(float64)
		}
		return total
	}
	want := func(label string, got map[string][2]any, key string, effort float64) {
		t.Helper()
		v, ok := got[key]
		if !ok {
			t.Errorf("%s: no row %s in %v", label, key, got)
			return
		}
		if math.Abs(v[1].(float64)-effort) > 1e-6 {
			t.Errorf("%s: row %s effort = %v, want %v", label, key, v[1], effort)
		}
	}

	// Repository B: the unit appears once, as repository B with its 40 (not repository A with 100).
	repoB_ := read(WorkUnitInvestmentsFilter{RepoIDs: []string{repoB}})
	want("repo B", repoB_, "ux@"+repoB, 40)
	if len(repoB_) != 1 || sum(repoB_) != 40 {
		t.Errorf("repo B list = %v, want one row summing to repository B's allocation total 40", repoB_)
	}
	// Repository A: ux's 60 plus u1's 10 = the allocation total of A.
	repoA_ := read(WorkUnitInvestmentsFilter{RepoIDs: []string{repoA}})
	want("repo A", repoA_, "ux@"+repoA, 60)
	want("repo A", repoA_, "u1@"+repoA, 10)
	if sum(repoA_) != 70 {
		t.Errorf("repo A list sums to %v, want repository A's allocation total 70", sum(repoA_))
	}
	// Org scope (no scope): unchanged, the whole unit once with its own repository.
	org_ := read(WorkUnitInvestmentsFilter{})
	want("org scope", org_, "ux@"+repoA, 100)
	want("org scope", org_, "u1@"+repoA, 10)
	if len(org_) != 2 {
		t.Errorf("org scope list = %v, want 2 whole units", org_)
	}
	// Team scope: unchanged, the whole unit (own repository, full effort) for every unit the team's repositories touch.
	teamB := read(WorkUnitInvestmentsFilter{
		TeamScopeCondition: "work_unit_investments.repo_id IN (SELECT toString(repo_id) FROM team_repo_ownership FINAL WHERE org_id = {org_id:String} AND team_id = 'team-b')",
	})
	want("team B scope", teamB, "ux@"+repoA, 100)
	if len(teamB) != 1 {
		t.Errorf("team B list = %v, want ux once, whole", teamB)
	}
}
