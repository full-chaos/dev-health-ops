//go:build integration

package analytics

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL. The catalog value read and the evidence-quality group read scan a
// Nullable key into a string; each WHERE drops the NULL key before the scan.
func TestFilteredGroupKeysNeverScanANull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, ddl := range []string{seededQualitySchemaDDL, workItemCompletionDailyDDL, seededCatalogExtraDDL} {
		for _, stmt := range splitSQLStatements(ddl) {
			if err := conn.Exec(ctx, stmt); err != nil {
				t.Fatalf("exec DDL %q: %v", stmt, err)
			}
		}
	}
	chschema.CreateTables(ctx, t, conn, "work_unit_investments")

	const orgID = "nullable-group-key-14218"
	seedQualityRows(t, ctx, conn, orgID, []seededQualityRow{
		{workUnitID: "wu-typed", evidence: 0.8, band: "high", themeMap: "map('feature_delivery', 1.0)", subcategoryMap: "map('feature_delivery.delivery', 1.0)", workUnitType: "pull_request"},
		{workUnitID: "wu-null", evidence: 0.2, band: "low", subcategoryMap: "map('maintenance.refactor', 1.0)"},
	})
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	t.Run("ResolveCatalog work type", func(t *testing.T) {
		dim := model.DimensionInputWorkType
		res, _, err := ResolveCatalog(ctx, client, orgID, &dim, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Values) != 1 || res.Values[0].Value != "pull_request" {
			t.Fatalf("values = %+v, want only pull_request: the NULL work type is not a value", res.Values)
		}
	})
	t.Run("executeEvidenceQualityByGroup theme", func(t *testing.T) {
		q, err := compileEvidenceQualityByGroup(orgID, mustGraphQLDate("2026-01-01"), mustGraphQLDate("2026-01-08"), DimensionTheme, evidenceQualityFilter{})
		if err != nil {
			t.Fatal(err)
		}
		groups, err := executeEvidenceQualityByGroup(ctx, client, q)
		if err != nil {
			t.Fatal(err)
		}
		if len(groups) != 1 {
			t.Fatalf("groups = %+v, want one: the unit with no theme has no group", groups)
		}
	})
}

// The pooled series bucket date_trunc(day), and day is toDate(started_at), a
// Nullable column: the day window drops a run with no start before the scan,
// for the team series and for the ORG series.
func TestPooledSeriesNeverBucketARunWithNoStart(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-14218-no-start"
	const repo = "14218000-0000-4000-8000-0000000001a1"
	e.repo(org, repo, "acme/a")
	e.owner(org, "team-a", repo, "acme/a", 1, openFrom(), nil)
	e.durations(org, repo, day1, 10, 20, 30)
	e.exec(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, finished_at, last_synced, org_id, duration_seconds) VALUES (?, 'no-start', 'success', NULL, NULL, ?, ?, 5)`,
		repo, ms(&poolSynced), org)
	e.write(org, day1, repo)
	teams, orgValues := e.dimSeries(org, model.DimensionInputTeam, model.MeasureInputPipelineDurationP95, model.BucketIntervalInputDay, nil)
	if len(teams["team-a"]) != 1 || len(orgValues) != 1 {
		t.Fatalf("team series = %v, org series = %v, want one bucket each", teams, values(orgValues))
	}
}

// The timeseries read scans its dimension value into a string. On the daily
// path the repo dimension is work_item_completion_daily.repo_id, a
// Nullable(UUID): a row with no repository reads as the empty value (v2.49).
func TestTimeseriesRepoDimensionReadsANullRepoAsEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range splitSQLStatements(workItemCompletionDailyDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}
	const orgID = "timeseries-null-repo-14218"
	seedWorkItemCompletionRows(t, ctx, conn, orgID, "2026-01-02", []workItemCompletionSeedRow{{repoID: "14218000-0000-4000-8000-0000000002a1", teamID: "team-a", workItemsDone: 2}})
	if err := conn.Exec(ctx, `INSERT INTO work_item_completion_daily (repo_id, day, team_id, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (NULL, toDate('2026-01-02'), 'team-a', 1, 3, 1, 10, 2.0, now(), ?)`, orgID); err != nil {
		t.Fatal(err)
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	input := tsInput(model.DimensionInputRepo, model.MeasureInputThroughput)
	input.Interval = model.BucketIntervalInputDay
	got, err := resolveOneTimeseries(ctx, client, orgID, input, false, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	dims := map[string]bool{}
	for _, s := range got {
		dims[s.DimensionValue] = true
	}
	if !dims[""] || len(dims) != 2 {
		t.Fatalf("dimension values = %v, want the repo and the empty value of the row with no repository", dims)
	}
}

// The sankey node/edge reads and the two timeseries reads scan a value that can
// be NULL (toString of a Nullable dimension, date_trunc of a Nullable day) into
// a nillable local and read NULL as the v2.49 zero value: "" or the zero date.
func TestSankeyAndTimeseriesReadsKeepTheZeroValueOfANull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	t.Run("queryNodes", func(t *testing.T) {
		nodes, err := queryNodes(ctx, client, compiledQuery{sql: "SELECT 'repo' AS dimension, toString(CAST(NULL AS Nullable(UUID))) AS node_id, toFloat64(1) AS value"})
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 1 || nodes[0].ID != "repo:" {
			t.Fatalf("nodes = %+v, want one node with the empty id", nodes)
		}
	})
	t.Run("queryEdges", func(t *testing.T) {
		edges, err := queryEdges(ctx, client, compiledQuery{sql: "SELECT 'repo' AS source_dim, 'team' AS target_dim, toString(CAST(NULL AS Nullable(UUID))) AS source, 'team-a' AS target, toFloat64(1) AS value"})
		if err != nil {
			t.Fatal(err)
		}
		if len(edges) != 1 || edges[0].Source != "repo:" || edges[0].Target != "team:team-a" {
			t.Fatalf("edges = %+v, want one edge from the empty repo id", edges)
		}
	})
	t.Run("ExecuteTimeseries", func(t *testing.T) {
		got, err := ExecuteTimeseries(ctx, client, compiledQuery{sql: "SELECT date_trunc('day', CAST(NULL AS Nullable(Date))) AS bucket, CAST(NULL AS Nullable(String)) AS dimension_value, toFloat64(1) AS value"}, "repo", "throughput")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].DimensionValue != "" || len(got[0].Buckets) != 1 || !got[0].Buckets[0].Date.Time().IsZero() {
			t.Fatalf("series = %+v, want one series with the empty value and a zero bucket", got)
		}
	})
	t.Run("ExecuteOrgTimeseries", func(t *testing.T) {
		got, err := ExecuteOrgTimeseries(ctx, client, compiledQuery{sql: "SELECT date_trunc('day', CAST(NULL AS Nullable(Date))) AS bucket, toFloat64(1) AS numerator, toFloat64(2) AS denominator"}, "org", "pipeline_duration_p95", MeasurePipelineDurationP95)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Buckets) != 1 || !got.Buckets[0].Date.Time().IsZero() {
			t.Fatalf("series = %+v, want one zero bucket", got)
		}
	})
}
