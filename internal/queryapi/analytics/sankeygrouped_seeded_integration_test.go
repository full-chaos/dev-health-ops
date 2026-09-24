//go:build integration

// Differential test for the grouped sankey query: on a real ClickHouse
// engine, CompileSankeyGrouped + executeSankeyGrouped must return what
// CompileSankey + ExecuteSankeyQueries (one UNION ALL nodes query plus
// one query per edge pair) return, for every request in an enumerated
// domain -- every ordered path of two and three dimensions, a set of
// four-dimension paths, both source planes (investment and
// investment_metrics_daily), two measures, limits that truncate and
// limits that do not, and with and without a theme filter. The seed
// holds ties, NULL team keys, unassigned teams and repos, multi-repo
// allocation and multi-subcategory units, so truncation order, NULL
// handling and allocation weights are all exercised.
package analytics

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const groupedSankeyOrg = "seeded-sankey-grouped"

func seedGroupedSankey(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	t.Helper()
	exec := func(what, sql string) {
		t.Helper()
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("seed %s: %v", what, err)
		}
	}
	const ts = "2026-01-02 00:00:00.000"
	type unit struct {
		id, workType, repo, issue string
		effort                    float64
		subcats                   string // ClickHouse map literal body
	}
	units := []unit{
		{"wu-01", "pr", seededCoverageRepo1, "linear:ALPHA-1", 100, "'feature_delivery.build', 0.6, 'quality.testing', 0.4"},
		{"wu-02", "issue", seededCoverageRepo2, "linear:BETA-1", 50, "'feature_delivery.build', 1.0"},
		{"wu-03", "pr", "", "linear:NOTEAM-1", 25, "'risk.security', 0.5, 'maintenance.debt', 0.5"},
		{"wu-04", "issue", seededCoverageRepo1, "linear:ALPHA-1", 0, "'quality.testing', 1.0"},
		{"wu-05", "epic", seededCoverageRepo3, "linear:GAMMA-1", 10, "'operational.on_call', 0.25, 'feature_delivery.roadmap', 0.75"},
		// Ties with wu-02 on every aggregate it shares, so key order
		// breaks the tie under truncation.
		{"wu-06", "issue", seededCoverageRepo2, "linear:BETA-1", 50, "'feature_delivery.build', 1.0"},
		{"wu-07", "pr", seededCoverageRepo3, "linear:BETA-2", 5, "'maintenance.debt', 1.0"},
		{"wu-08", "epic", "", "linear:NOTEAM-2", 7, "'risk.compliance', 1.0"},
	}
	rows := make([]string, 0, len(units))
	for _, u := range units {
		repo := "NULL"
		if u.repo != "" {
			repo = fmt.Sprintf("toUUID('%s')", u.repo)
		}
		rows = append(rows, fmt.Sprintf(
			"('%s', toDateTime64('%s', 3, 'UTC'), toDateTime64('2026-01-03 00:00:00.000', 3, 'UTC'), %s, 'github', 'churn_loc', %v, map(), map(%s), '{\"issues\":[\"%s\"],\"prs\":[]}', 0.5, 'moderate', 'ok', '', 'v1', 'h', 'run-1', toDateTime64('%s', 3, 'UTC'), '%s', 'seeded', '%s')",
			u.id, ts, repo, u.effort, u.subcats, u.issue, ts, u.workType, groupedSankeyOrg))
	}
	exec("work_unit_investments", "INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES "+strings.Join(rows, ", "))
	exec("work_unit_repo_effort", fmt.Sprintf(`INSERT INTO work_unit_repo_effort
        (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES
        ('wu-01', toUUID('%[1]s'), 'churn_loc', 60, 0.6, 'evidence', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s'),
        ('wu-01', toUUID('%[2]s'), 'churn_loc', 40, 0.4, 'evidence', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s'),
        ('wu-04', toUUID('%[1]s'), 'churn_loc', 0, 1.0, 'evidence', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s'),
        ('wu-04', toUUID('%[3]s'), 'churn_loc', 0, 1.0, 'evidence', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s'),
        ('wu-05', toUUID('%[3]s'), 'churn_loc', 10, 1.0, 'evidence', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s')`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageRepo3, ts, groupedSankeyOrg))
	exec("work_item_team_attributions", fmt.Sprintf(`INSERT INTO work_item_team_attributions
        (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
        ('%[4]s', toUUID('%[1]s'), 'linear:ALPHA-1', 'linear', 'ALPHA', 'Alpha', 'native_team', 1, 'high', '', toDateTime64('%[3]s', 3, 'UTC')),
        ('%[4]s', toUUID('%[2]s'), 'linear:BETA-1', 'linear', 'BETA', 'Beta', 'native_team', 1, 'high', '', toDateTime64('%[3]s', 3, 'UTC')),
        ('%[4]s', toUUID('%[2]s'), 'linear:BETA-2', 'linear', 'BETA', 'Beta', 'native_team', 1, 'high', '', toDateTime64('%[3]s', 3, 'UTC')),
        ('%[4]s', toUUID('%[1]s'), 'linear:GAMMA-1', 'linear', 'GAMMA', '', 'native_team', 1, 'high', '', toDateTime64('%[3]s', 3, 'UTC'))`,
		seededCoverageRepo1, seededCoverageRepo2, ts, groupedSankeyOrg))
	exec("repos", fmt.Sprintf(`INSERT INTO repos
        (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES
        (toUUID('%[1]s'), 'acme/one', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL),
        (toUUID('%[2]s'), 'acme/two', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL),
        (toUUID('%[3]s'), '', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL)`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageRepo3, ts, groupedSankeyOrg))

	// investment_metrics_daily: the non-investment plane. team_id is
	// Nullable -- one row carries a real NULL, one an empty string.
	exec("investment_metrics_daily", fmt.Sprintf(`INSERT INTO investment_metrics_daily
        (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id) VALUES
        (toUUID('%[1]s'), toDate('2026-01-05'), 't1', 'feature', 'ps1', 1, 5, 1, 10, 2.0, now(), '%[4]s'),
        (toUUID('%[2]s'), toDate('2026-01-05'), '', 'feature', 'ps2', 1, 7, 1, 30, 2.0, now(), '%[4]s'),
        (toUUID('%[3]s'), toDate('2026-01-05'), 't3', 'bug', 'ps3', 1, 3, 1, 10, 2.0, now(), '%[4]s'),
        (toUUID('%[1]s'), toDate('2026-01-06'), NULL, 'bug', 'ps1', 1, 3, 1, 20, 2.0, now(), '%[4]s'),
        (NULL, toDate('2026-01-06'), 't1', 'chore', 'ps2', 1, 5, 1, 10, 2.0, now(), '%[4]s')`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageRepo3, groupedSankeyOrg))
}

// groupedSankeyPaths enumerates every ordered path of two and three
// distinct dimensions over dims, plus the four-dimension paths the web
// client and a reversed/interleaved variant send.
func groupedSankeyPaths(dims []Dimension) [][]Dimension {
	var out [][]Dimension
	for _, a := range dims {
		for _, b := range dims {
			if a == b {
				continue
			}
			out = append(out, []Dimension{a, b})
			for _, c := range dims {
				if c == a || c == b {
					continue
				}
				out = append(out, []Dimension{a, b, c})
			}
		}
	}
	out = append(out,
		[]Dimension{DimensionTeam, DimensionTheme, DimensionSubcategory, DimensionRepo},
		[]Dimension{DimensionRepo, DimensionSubcategory, DimensionTheme, DimensionTeam},
		[]Dimension{DimensionWorkType, DimensionTeam, DimensionRepo, DimensionTheme},
		[]Dimension{DimensionTeam, DimensionTheme, DimensionSubcategory, DimensionRepo, DimensionWorkType},
	)
	return out
}

func floatPtrsEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	if math.IsNaN(*a) || math.IsNaN(*b) {
		return math.IsNaN(*a) && math.IsNaN(*b)
	}
	// Same rows, same aggregate; ClickHouse's parallel SUM may differ in
	// the last bits between any two runs of either form.
	return math.Abs(*a-*b) <= 1e-9*math.Max(1, math.Max(math.Abs(*a), math.Abs(*b)))
}

func sankeyNodesEqual(a, b []model.SankeyNode) string {
	if len(a) != len(b) {
		return fmt.Sprintf("node count %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Label != b[i].Label || a[i].Dimension != b[i].Dimension || !floatPtrsEqual(a[i].Value, b[i].Value) {
			return fmt.Sprintf("node[%d] %+v != %+v", i, a[i], b[i])
		}
	}
	return ""
}

func sankeyEdgesEqual(a, b []model.SankeyEdge) string {
	if len(a) != len(b) {
		return fmt.Sprintf("edge count %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Source != b[i].Source || a[i].Target != b[i].Target || !floatPtrsEqual(a[i].Value, b[i].Value) {
			return fmt.Sprintf("edge[%d] %+v != %+v", i, a[i], b[i])
		}
	}
	return ""
}

func TestSankeyGrouped_SeededRealClickHouse_EqualsPerDimensionQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL + investmentMetricsDailyDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}
	seedGroupedSankey(t, ctx, conn)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	dims := []Dimension{DimensionTeam, DimensionTheme, DimensionSubcategory, DimensionRepo, DimensionWorkType}
	limits := []struct{ nodes, edges int }{{3, 2}, {10, 3}, {100, 500}}
	themeFilter := &model.FilterInput{Why: &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}}}

	type diffCase struct {
		name          string
		req           SankeyRequest
		useInvestment bool
		filters       *model.FilterInput
	}
	var domain []diffCase
	for _, useInvestment := range []bool{true, false} {
		for _, path := range groupedSankeyPaths(dims) {
			for _, measure := range []Measure{MeasureCount, MeasureChurnLOC} {
				for _, lim := range limits {
					for _, filters := range []*model.FilterInput{nil, themeFilter} {
						domain = append(domain, diffCase{
							name: fmt.Sprintf("inv=%v path=%s measure=%s limits=%d/%d filtered=%v", useInvestment, pathLabel(path), measure, lim.nodes, lim.edges, filters != nil),
							req: SankeyRequest{
								Path: path, Measure: measure,
								StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-08"),
								MaxNodes: lim.nodes, MaxEdges: lim.edges,
							},
							useInvestment: useInvestment,
							filters:       filters,
						})
					}
				}
			}
		}
	}

	// Each worker writes only its own results slot.
	type result struct {
		failure             string
		rejected            bool
		nonEmpty, truncated bool
	}
	results := make([]result, len(domain))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				c := domain[idx]
				r := &results[idx]
				nodesQ, edgesQ, oldErr := CompileSankey(c.req, groupedSankeyOrg, 30, c.useInvestment, c.filters)
				grouped, newErr := CompileSankeyGrouped(c.req, groupedSankeyOrg, 30, c.useInvestment, c.filters)
				if (oldErr == nil) != (newErr == nil) || (oldErr != nil && oldErr.Error() != newErr.Error()) {
					r.failure = fmt.Sprintf("compile disagreement: per-dimension err=%v grouped err=%v", oldErr, newErr)
					continue
				}
				if oldErr != nil {
					r.rejected = true
					continue
				}
				wantNodes, wantEdges, wantErr := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQ}, edgesQ)
				gotNodes, gotEdges, gotErr := executeSankeyGrouped(ctx, client, grouped)
				if (wantErr == nil) != (gotErr == nil) {
					r.failure = fmt.Sprintf("execute disagreement: per-dimension err=%v grouped err=%v", wantErr, gotErr)
					continue
				}
				if wantErr != nil {
					r.rejected = true
					continue
				}
				if diff := sankeyNodesEqual(wantNodes, gotNodes); diff != "" {
					r.failure = fmt.Sprintf("nodes differ: %s\nwant %+v\ngot  %+v", diff, wantNodes, gotNodes)
					continue
				}
				if diff := sankeyEdgesEqual(wantEdges, gotEdges); diff != "" {
					r.failure = fmt.Sprintf("edges differ: %s\nwant %+v\ngot  %+v", diff, wantEdges, gotEdges)
					continue
				}
				n := len(c.req.Path)
				r.nonEmpty = len(wantNodes) > 0 && len(wantEdges) > 0
				r.truncated = len(wantNodes) == (c.req.MaxNodes/n)*n || len(wantEdges) == (c.req.MaxEdges/(n-1))*(n-1)
			}
		}()
	}
	for idx := range domain {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	var cases, compared, bothRejected, nonEmpty, truncated int
	for idx, r := range results {
		cases++
		if r.failure != "" {
			t.Errorf("%s: %s", domain[idx].name, r.failure)
			continue
		}
		if r.rejected {
			bothRejected++
			continue
		}
		compared++
		if r.nonEmpty {
			nonEmpty++
		}
		if r.truncated {
			truncated++
		}
	}
	t.Logf("cases=%d compared=%d both_rejected=%d non_empty=%d at_a_limit=%d", cases, compared, bothRejected, nonEmpty, truncated)
	// A domain that never reached real rows, a limit, or a rejection
	// proves nothing; fail instead of passing on an empty comparison.
	if compared == 0 || nonEmpty == 0 || truncated == 0 || bothRejected == 0 {
		t.Fatalf("differential domain did not exercise every class: compared=%d non_empty=%d at_a_limit=%d both_rejected=%d", compared, nonEmpty, truncated, bothRejected)
	}
}
