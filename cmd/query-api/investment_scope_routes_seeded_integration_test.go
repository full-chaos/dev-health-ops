//go:build integration

package main

// Every investment route reader, against the real migration chain, across
// the membership gate's input domain: each reader's answer for an
// organisation in a given marker × investment state must equal its answer
// for a reference organisation whose stored rows are exactly the work units
// that state should expose.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investment"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentflow"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/sankey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const scopeRoutesBase = "2026-01-06 00:00:00"

// scopeRoutesRepoID is the one repository every seeded unit belongs to.
const scopeRoutesRepoID = "00000000-0000-0000-0000-0000000000a1"

type scopeUnit struct {
	id          string
	effort      float64
	theme       string
	subcategory string
	computedAt  string
}

type scopeMarker struct {
	runID       string
	completedAt string
	units       []string
}

type scopeCell struct {
	name        string
	lagging     bool // the projection trails the newest investment row
	units       []scopeUnit
	markers     []scopeMarker
	otherOrgRun bool // a complete run exists, but only for a different organisation
	reference   string
}

func scopeRoutesInsertUnit(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, unit scopeUnit) {
	t.Helper()
	stmt := fmt.Sprintf(`INSERT INTO work_unit_investments
		(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
		 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
		 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
		 categorization_errors_json, categorization_model_version, categorization_input_hash,
		 categorization_run_id, computed_at, org_id)
		VALUES ('%s', 'issue', 'Unit %s', toDateTime64('2026-01-02 00:00:00',3), toDateTime64('2026-01-04 00:00:00',3),
		 toUUID('%s'), 'github', 'churn_loc', %g, map('%s', 1.0), map('%s', 1.0), '{}',
		 0.5, 'moderate', 'ok', '', 'v1', 'hash', 'cat-run', toDateTime64('%s',3), '%s')`,
		unit.id, unit.id, scopeRoutesRepoID, unit.effort, unit.theme, unit.subcategory, unit.computedAt, orgID)
	if err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("seed work_unit_investments %s/%s: %v", orgID, unit.id, err)
	}
}

func scopeRoutesInsertRepo(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string) {
	t.Helper()
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES ('%s', 'acme/scope-repo', toDateTime64('%s',3), toDateTime64('%s',3), '%s', 'github')`,
		scopeRoutesRepoID, scopeRoutesBase, scopeRoutesBase, orgID)); err != nil {
		t.Fatalf("seed repos %s: %v", orgID, err)
	}
}

func scopeRoutesInsertMarker(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, marker scopeMarker) {
	t.Helper()
	for i, unitID := range marker.units {
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_unit_membership
			(org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant,
			 categorization_status, computed_at, run_id)
			VALUES ('%s', 'issue', 'node-%s-%d', '%s', 'theme', 'feature_delivery', 1.0, 1, 'ok', toDateTime64('%s',3), '%s')`,
			orgID, marker.runID, i, unitID, marker.completedAt, marker.runID)); err != nil {
			t.Fatalf("seed work_unit_membership %s: %v", orgID, err)
		}
	}
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
		VALUES ('%s', '%s', toDateTime64('%s',3))`, orgID, marker.runID, marker.completedAt)); err != nil {
		t.Fatalf("seed work_unit_membership_runs %s: %v", orgID, err)
	}
}

func scopeRoutesAt(offset time.Duration) string {
	base, _ := time.Parse("2006-01-02 15:04:05", scopeRoutesBase)
	return base.Add(offset).Format("2006-01-02 15:04:05.000")
}

// scopeLagWarnings counts the stale-projection warn lines per organisation.
type scopeLagWarnings struct {
	mu    sync.Mutex
	byOrg map[string]int
}

func (w *scopeLagWarnings) Enabled(context.Context, slog.Level) bool { return true }
func (w *scopeLagWarnings) WithAttrs([]slog.Attr) slog.Handler       { return w }
func (w *scopeLagWarnings) WithGroup(string) slog.Handler            { return w }
func (w *scopeLagWarnings) Handle(_ context.Context, record slog.Record) error {
	if record.Level != slog.LevelWarn || record.Message != "investment membership projection trails the latest investment computation; reads stay scoped to the latest complete membership run" {
		return nil
	}
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "org_id" {
			w.mu.Lock()
			w.byOrg[attr.Value.String()]++
			w.mu.Unlock()
		}
		return true
	})
	return nil
}

func (w *scopeLagWarnings) take(orgID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.byOrg[orgID]
	delete(w.byOrg, orgID)
	return n
}

type scopeRoute struct {
	name string
	read func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error)
}

func scopeRoutes(t *testing.T) []scopeRoute {
	t.Helper()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	date := func(s string) graphqldate.Date {
		d, err := graphqldate.Parse(s)
		if err != nil {
			t.Fatalf("parse date: %v", err)
		}
		return d
	}
	useInvestment := true
	dateRange := &model.DateRangeInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-08")}
	return []scopeRoute{
		{"/api/v1/investment", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			reader, err := investment.NewReader(client)
			if err != nil {
				return nil, err
			}
			return investment.BuildResponse(ctx, reader, orgID, investment.Params{OrgID: orgID, StartTS: start, EndTS: end, ScopeLevel: "org"})
		}},
		{"/api/v1/investment/sunburst", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			reader, err := investment.NewReader(client)
			if err != nil {
				return nil, err
			}
			return investment.BuildSunburstResponse(ctx, reader, orgID, investment.SunburstParams{OrgID: orgID, StartTS: start, EndTS: end, ScopeLevel: "org", Limit: 50})
		}},
		{"/api/v1/investment/flow", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			return investmentflow.BuildFlowResponse(ctx, client, investmentflow.Params{OrgID: orgID, StartTS: start, EndTS: end, ScopeLevel: "org", TopNRepos: 12})
		}},
		{"/api/v1/investment/flow (team_category_repo)", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			flowMode := "team_category_repo"
			return investmentflow.BuildFlowResponse(ctx, client, investmentflow.Params{OrgID: orgID, StartTS: start, EndTS: end, ScopeLevel: "org", TopNRepos: 12, FlowMode: &flowMode})
		}},
		{"/api/v1/investment/flow/repo-team", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			return investmentflow.BuildRepoTeamFlowResponse(ctx, client, investmentflow.RepoTeamParams{OrgID: orgID, StartTS: start, EndTS: end, ScopeLevel: "org"})
		}},
		{"/api/v1/sankey (investment)", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			return sankey.BuildResponse(ctx, client, orgID, sankey.Params{Mode: "investment", ScopeLevel: "org", StartDay: start, EndDay: end})
		}},
		{"/api/v1/investment/explain (breakdown read)", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				return nil, err
			}
			return reader.FetchInvestmentBreakdown(ctx, investmentexplain.BreakdownFilters{OrgID: orgID, StartTS: start, EndTS: end})
		}},
		{"/api/v1/work-units", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				return nil, err
			}
			units, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{OrgID: orgID, StartTS: start, EndTS: end, Limit: 200})
			if err != nil {
				return nil, err
			}
			ids := make([]string, 0, len(units))
			for _, unit := range units {
				ids = append(ids, unit.WorkUnitID)
			}
			return ids, nil
		}},
		{"/query analytics (breakdown, timeseries, sankey, flowMatrix)", func(ctx context.Context, client analytics.QueryClient, orgID string) (any, error) {
			return analytics.Resolve(ctx, client, orgID, model.AnalyticsRequestInput{
				Breakdowns:    []model.BreakdownRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dateRange, TopN: 10}},
				Timeseries:    []model.TimeseriesRequestInput{{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, Interval: model.BucketIntervalInputDay, DateRange: dateRange}},
				Sankey:        &model.SankeyRequestInput{Path: []model.DimensionInput{model.DimensionInputTheme, model.DimensionInputSubcategory}, Measure: model.MeasureInputCount, DateRange: dateRange, MaxNodes: 20, MaxEdges: 20},
				FlowMatrix:    &model.FlowMatrixRequestInput{Dimension: model.DimensionInputTheme, Measure: model.MeasureInputCount, DateRange: dateRange, MaxNodes: 20, MaxEdges: 20},
				UseInvestment: &useInvestment,
			})
		}},
	}
}

func TestInvestmentRoutesFollowTheMembershipGateAcrossItsDomain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	chschema.Apply(ctx, t, instance)

	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	defer func() { _ = conn.Close() }()
	rawClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(instance.URI))
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = rawClient.Close() }()
	pinned := analytics.PinInvestmentMembershipScope(rawClient)
	warnings := &scopeLagWarnings{byOrg: map[string]int{}}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(warnings))
	defer slog.SetDefault(previousLogger)

	in := scopeUnit{id: "wu-in", effort: 3, theme: "feature_delivery", subcategory: "feature_delivery.roadmap", computedAt: scopeRoutesAt(0)}
	stale := scopeUnit{id: "wu-stale", effort: 7, theme: "maintenance", subcategory: "maintenance.debt", computedAt: scopeRoutesAt(0)}
	fresh := func(offset time.Duration) scopeUnit {
		return scopeUnit{id: "wu-fresh", effort: 11, theme: "operational", subcategory: "operational.on_call", computedAt: scopeRoutesAt(offset)}
	}
	run := func(runID string, offset time.Duration, units ...string) scopeMarker {
		return scopeMarker{runID: runID, completedAt: scopeRoutesAt(offset), units: units}
	}

	// References: an organisation whose stored rows ARE the exposed set.
	references := map[string][]scopeUnit{
		"ref-empty":    nil,
		"ref-in":       {in},
		"ref-in-stale": {in, stale},
	}
	cells := []scopeCell{
		{name: "no marker, no investments", reference: "ref-empty"},
		{name: "complete marker, no investments", markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in")}, reference: "ref-empty"},
		{name: "no marker, investments", units: []scopeUnit{in, stale}, reference: "ref-in-stale"},
		{name: "marker newer than investments", units: []scopeUnit{in, stale}, markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in")}, reference: "ref-in"},
		{name: "marker equal to newest investment", units: []scopeUnit{in, stale}, markers: []scopeMarker{run("run-1", 0, "wu-in")}, reference: "ref-in"},
		{name: "marker lagging by 1ms", lagging: true, units: []scopeUnit{in, stale, fresh(5*time.Second + time.Millisecond)}, markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in")}, reference: "ref-in"},
		{name: "marker lagging by seconds", lagging: true, units: []scopeUnit{in, stale, fresh(9 * time.Second)}, markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in")}, reference: "ref-in"},
		{name: "marker lagging by hours", lagging: true, units: []scopeUnit{in, stale, fresh(3 * time.Hour)}, markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in")}, reference: "ref-in"},
		{name: "older run superseded by newer run", units: []scopeUnit{in, stale}, markers: []scopeMarker{run("run-old", 2*time.Second, "wu-in", "wu-stale"), run("run-new", 5*time.Second, "wu-in")}, reference: "ref-in"},
		{name: "duplicate marker rows for one run", units: []scopeUnit{in, stale}, markers: []scopeMarker{run("run-1", 5*time.Second, "wu-in"), {runID: "run-1", completedAt: scopeRoutesAt(5 * time.Second)}}, reference: "ref-in"},
		{name: "marker with an empty run id", units: []scopeUnit{in, stale}, markers: []scopeMarker{{runID: "", completedAt: scopeRoutesAt(5 * time.Second)}}, reference: "ref-in-stale"},
		{name: "complete run only for another organisation", units: []scopeUnit{in, stale}, otherOrgRun: true, reference: "ref-in-stale"},
	}

	for orgID, units := range references {
		scopeRoutesInsertRepo(t, ctx, conn, orgID)
		for _, unit := range units {
			scopeRoutesInsertUnit(t, ctx, conn, orgID, unit)
		}
		// A reference organisation's set is published by its own marker, so
		// its reads are scoped to exactly its rows.
		if len(units) > 0 {
			ids := make([]string, 0, len(units))
			for _, unit := range units {
				ids = append(ids, unit.id)
			}
			scopeRoutesInsertMarker(t, ctx, conn, orgID, run("run-ref", time.Hour*24, ids...))
		}
	}
	for i, cell := range cells {
		orgID := fmt.Sprintf("cell-%02d", i)
		scopeRoutesInsertRepo(t, ctx, conn, orgID)
		for _, unit := range cell.units {
			scopeRoutesInsertUnit(t, ctx, conn, orgID, unit)
		}
		for _, marker := range cell.markers {
			scopeRoutesInsertMarker(t, ctx, conn, orgID, marker)
		}
		if cell.otherOrgRun {
			scopeRoutesInsertMarker(t, ctx, conn, "cell-other-org", run("run-other", 5*time.Second, "wu-in"))
		}
	}

	render := func(route scopeRoute, ctx context.Context, client analytics.QueryClient, orgID string) string {
		t.Helper()
		got, err := route.read(ctx, client, orgID)
		if err != nil {
			t.Fatalf("%s for %s: %v", route.name, orgID, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("%s for %s: marshal: %v", route.name, orgID, err)
		}
		return string(encoded)
	}

	for _, route := range scopeRoutes(t) {
		// The reader must tell the references apart, or no cell below can fail.
		if refIn := render(route, ctx, rawClient, "ref-in"); refIn == render(route, ctx, rawClient, "ref-in-stale") {
			t.Fatalf("%s answers the same for {wu-in} and {wu-in, wu-stale}; it cannot observe the gate: %s", route.name, refIn)
		}
		for i, cell := range cells {
			orgID := fmt.Sprintf("cell-%02d", i)
			want := render(route, ctx, rawClient, cell.reference)
			for _, clientName := range []string{"raw", "pinned"} {
				client := map[string]analytics.QueryClient{"raw": rawClient, "pinned": pinned}[clientName]
				warnings.take(orgID)
				requestCtx := analytics.WithInvestmentMembershipScopeRequest(ctx)
				if got := render(route, requestCtx, client, orgID); got != want {
					t.Errorf("%s, %s, %s client:\n got  %s\n want %s (%s)", route.name, cell.name, clientName, got, want, cell.reference)
				}
				if clientName != "pinned" {
					continue
				}
				// A pinned request reports a lagging projection exactly once.
				wantWarnings := 0
				if cell.lagging {
					wantWarnings = 1
				}
				if got := warnings.take(orgID); got != wantWarnings {
					t.Errorf("%s, %s: stale-projection warn lines = %d, want %d", route.name, cell.name, got, wantWarnings)
				}
			}
		}
	}
}
