//go:build integration

// CHAOS-4723's durable real-engine regression coverage for
// resolveEvidenceQualityStats -- the seeded-Testcontainers counterpart to
// investmentquality_live_test.go's live-data evidence. Unlike that file
// (real org data, drifts, opt-in via DEV_HEALTH_REQUIRE_LIVE, not enrolled in
// any CI shard), THIS file spins up its OWN isolated ClickHouse via
// internal/testsupport/containers.StartClickHouse -- the exact reusable
// harness cmd/query-api/internal/hotspots' own real-engine regression guard
// (hotspots_argmax_tiebreak_integration_test.go) already uses -- seeds known
// rows, and asserts EXACT numbers computed by hand from the seed data. It is
// enrolled in ci/go_integration_shards.tsv like every other integration
// package, runs in CI, and never drifts: this is what proves the SQL this
// package generates is syntactically and semantically correct against a real
// ClickHouse engine (aggregate NULL/NaN handling, argMax semantics, the
// investment-membership-scope join fragments) -- exactly what
// investmentquality_test.go's routingFakeClient-based unit tests CANNOT
// prove, since a fake row-scanner double never parses or executes SQL at
// all.
//
// SCHEMA: only the three tables compileInvestmentQualityStats's generated
// SQL actually references (verified by reading investment.go/
// investmentmembershipscope.go's Source functions, not guessed) --
// work_unit_investments (017_investment_materialize_tables.sql +
// 019_work_unit_investment_labels.sql's work_unit_type/work_unit_name +
// 024_add_org_id.sql's org_id), work_unit_membership_runs
// (047_work_unit_membership_run_id.sql), and work_unit_membership
// (046_work_unit_membership.sql + 047's run_id column) -- no `repos` table,
// since this test never exercises repo-scope filtering.
//
// MEMBERSHIP SCOPE LEFT EMPTY, DELIBERATELY: work_unit_membership_runs and
// work_unit_membership are created but never seeded. latestCompleteMembershipRunSource's
// marker_count is then 0 for every org, so investmentMembershipScopeStateSource's
// scope_enabled evaluates to 0 (scope_mode="unscoped_no_marker") and
// investmentMembershipScopeFilter's `(SELECT scope_enabled ...) = 0 OR ...`
// passes every row through regardless of membership_scoped_work_unit_ids'
// (empty) contents -- the query still touches all three tables (ClickHouse
// resolves every referenced table at parse time even on the short-circuited
// side of an OR), so they must exist, but seeding rows into the two
// membership tables is unnecessary to prove this file's claim: the
// evidence-quality aggregate SQL itself is correct.
package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// splitSQLStatements splits a `;`-terminated multi-statement DDL block into
// individual statements, skipping blank ones -- the raw ClickHouse driver
// connection (unlike dev-health-go's Client) executes one statement per
// Exec call, same discipline query_route_integration_test.go's own
// multi-statement seed helpers use.
func splitSQLStatements(sql string) []string {
	var out []string
	for _, stmt := range strings.Split(sql, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

const seededQualitySchemaDDL = `
CREATE TABLE work_unit_investments (
    work_unit_id String,
    from_ts DateTime64(3, 'UTC'),
    to_ts DateTime64(3, 'UTC'),
    repo_id Nullable(UUID),
    provider Nullable(String),
    effort_metric String,
    effort_value Float64,
    theme_distribution_json Map(String, Float64),
    subcategory_distribution_json Map(String, Float64),
    structural_evidence_json String,
    evidence_quality Float64,
    evidence_quality_band String,
    categorization_status String,
    categorization_errors_json String,
    categorization_model_version String,
    categorization_input_hash String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    work_unit_type Nullable(String),
    work_unit_name Nullable(String),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (work_unit_id);

CREATE TABLE work_unit_membership_runs (
    org_id       String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, run_id);

CREATE TABLE work_unit_membership (
    org_id String,
    node_type String,
    node_id String,
    work_unit_id String,
    category_kind String,
    category String,
    weight Float64,
    is_dominant UInt8,
    categorization_status String,
    computed_at DateTime64(3, 'UTC'),
    run_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, node_type, node_id, category_kind, category);
`

// seededQualityRow is one seeded work_unit_investments row -- only the
// columns compileInvestmentQualityStats's SELECT actually reads carry
// meaningful values; every other NOT NULL column gets an arbitrary but
// valid filler so the INSERT satisfies the schema.
type seededQualityRow struct {
	workUnitID string
	evidence   float64
	band       string // "" is the "unknown" band, per the query's own countIf(evidence_quality IS NULL OR evidence_quality_band = '')
}

func seedQualityRows(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, rows []seededQualityRow) {
	t.Helper()
	const fromTS = "2026-01-01 00:00:00"
	const toTS = "2026-01-05 00:00:00"
	const computedAt = "2026-01-06 00:00:00"
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf(
			`('%s', toDateTime64('%s',3), toDateTime64('%s',3), NULL, NULL, 'fte_days', 1.0, map(), map(), '', %g, '%s', 'ok', '', 'v1', 'hash', 'run', toDateTime64('%s',3), NULL, NULL, '%s')`,
			r.workUnitID, fromTS, toTS, r.evidence, r.band, computedAt, orgID,
		)
	}
	insert := fmt.Sprintf(
		"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES %s",
		values,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_investments: %v", err)
	}
}

// TestResolveEvidenceQualityStats_SeededRealClickHouse_ExactAggregate is
// CHAOS-4723's durable, CI-enrolled real-engine regression test: six known
// work units, one per band (two "moderate" to prove countIf groups
// correctly, not just distinguishes present/absent), fed through
// resolveEvidenceQualityStats -- the SAME function Phase 4 calls -- against a
// REAL ClickHouse engine, hand-computed expected aggregate.
func TestResolveEvidenceQualityStats_SeededRealClickHouse_ExactAggregate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "chaos-4723-seeded-quality"
	seedQualityRows(t, ctx, conn, orgID, []seededQualityRow{
		{workUnitID: "wu-high", evidence: 0.90, band: "high"},
		{workUnitID: "wu-moderate-1", evidence: 0.50, band: "moderate"},
		{workUnitID: "wu-moderate-2", evidence: 0.51, band: "moderate"},
		{workUnitID: "wu-low", evidence: 0.20, band: "low"},
		{workUnitID: "wu-very-low", evidence: 0.05, band: "very_low"},
		{workUnitID: "wu-unknown", evidence: 0.00, band: ""}, // evidence_quality is NOT NULL per DDL, so "unknown" here comes from the empty band, not a NULL value -- exactly what CHAOS-4723's own SQL's countIf(evidence_quality IS NULL OR evidence_quality_band = '') expresses.
	})

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	batch := model.AnalyticsRequestInput{
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputRepo,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{
				StartDate: mustGraphQLDate("2026-01-01"),
				EndDate:   mustGraphQLDate("2026-01-08"),
			},
			TopN: 10,
		}},
		UseInvestment: boolPtr(true),
	}

	got, err := resolveEvidenceQualityStats(ctx, client, orgID, batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats: %v", err)
	}
	if got == nil {
		t.Fatal("expected a populated EvidenceQualityStats for the seeded org/window, got nil")
	}

	if got.Total != 6 {
		t.Errorf("Total = %d, want 6", got.Total)
	}

	var bands map[string]int
	if err := json.Unmarshal(got.BandCounts, &bands); err != nil {
		t.Fatalf("BandCounts did not unmarshal: %v (%s)", err, got.BandCounts)
	}
	wantBands := map[string]int{"high": 1, "moderate": 2, "low": 1, "very_low": 1, "unknown": 1}
	for band, want := range wantBands {
		if bands[band] != want {
			t.Errorf("bands[%s] = %d, want %d (bands=%+v)", band, bands[band], want, bands)
		}
	}

	// Hand-computed from the seed: evidence_quality is a non-nullable
	// column, so quality_known_count == total == 6 (every seeded row,
	// including the "unknown"-band one, has a real, non-NULL evidence_quality
	// value -- "unknown" comes from the empty band string, never from a NULL
	// evidence score). mean/stddev are population statistics over all six
	// values: 0.90, 0.50, 0.51, 0.20, 0.05, 0.00.
	values := []float64{0.90, 0.50, 0.51, 0.20, 0.05, 0.00}
	var sum float64
	for _, v := range values {
		sum += v
	}
	wantMean := sum / float64(len(values))
	var sqDiffSum float64
	for _, v := range values {
		d := v - wantMean
		sqDiffSum += d * d
	}
	wantStddev := math.Sqrt(sqDiffSum / float64(len(values)))

	if got.Mean == nil {
		t.Fatal("expected a non-nil mean")
	}
	if math.Abs(*got.Mean-wantMean) > 1e-9 {
		t.Errorf("Mean = %v, want %v", *got.Mean, wantMean)
	}
	if got.Stddev == nil {
		t.Fatal("expected a non-nil stddev")
	}
	if math.Abs(*got.Stddev-wantStddev) > 1e-9 {
		t.Errorf("Stddev = %v, want %v", *got.Stddev, wantStddev)
	}
}
