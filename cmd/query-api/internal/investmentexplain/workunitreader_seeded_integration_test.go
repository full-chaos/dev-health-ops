//go:build integration

// CHAOS-4977 step 7's recurrence guard: a REAL ClickHouse engine
// (internal/testsupport/containers.StartClickHouse, the same harness
// cmd/query-api/internal/analytics' own seeded integration tests use --
// investmentquality_seeded_integration_test.go,
// breakdown_seeded_integration_test.go), never the fake RowScanner every
// other test in this package uses.
//
// This is the exact class of gap that shipped undetected: every prior
// test double for FetchWorkUnitInvestments handed back whatever Go value
// its author declared for theme_distribution_json/subcategory_
// distribution_json, so none of them ever exercised the real
// clickhouse-go driver's type-conversion path against the real column
// type. The real columns are Map(String, Float64)
// (migrations/clickhouse/017_investment_materialize_tables.sql:11-12) --
// scanning them into *string (this package's first draft) fails outright
// ("converting Map(String, Float64) to **string is unsupported"), a
// defect that blocked EVERY live request and was only found by CHAOS-4977
// step 7's live-ClickHouse differential, never by any fixture-based test.
// This file exists so that class of gap cannot recur silently: it runs
// the real, unmodified FetchWorkUnitInvestments against a real, migrated
// schema, not a double.
//
// SCHEMA: work_unit_investments (017 + 019's work_unit_type/work_unit_name
// + 024's org_id), plus the three tables
// LatestWorkUnitInvestmentsSource's generated SQL references but this
// test deliberately leaves EMPTY (same reasoning as
// investmentquality_seeded_integration_test.go's own DDL: an empty
// work_unit_membership_runs makes scope_enabled evaluate to 0, so the
// membership-scope OR short-circuits and every seeded row passes
// through; an empty work_unit_supersessions makes the NOT IN filter
// vacuously true). ClickHouse still resolves every referenced table at
// parse time, so they must exist even though this file never seeds rows
// into them.
package investmentexplain

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func splitWorkUnitReaderDDL(sql string) []string {
	var out []string
	for _, stmt := range strings.Split(sql, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

const workUnitReaderSeededSchemaDDL = `
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

CREATE TABLE work_unit_supersessions (
    org_id String,
    superseded_work_unit_id String,
    superseded_by_run_id String,
    superseded_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(superseded_at)
ORDER BY (org_id, superseded_work_unit_id);
`

// TestFetchWorkUnitInvestments_SeededRealClickHouse_ScansMapColumns is
// CHAOS-4977 step 7's recurrence guard. RED on the pre-fix code (the
// original *string scan destinations): fails immediately with
// "converting Map(String, Float64) to **string is unsupported" --
// verified live against a real ClickHouse 26.7 container during step 7
// itself. GREEN on the mapKeys()/mapValues() fix (workunitreader.go +
// attribution.go's zipDistributionOrdered/zipDistribution).
func TestFetchWorkUnitInvestments_SeededRealClickHouse_ScansMapColumns(t *testing.T) {
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

	for _, stmt := range splitWorkUnitReaderDDL(workUnitReaderSeededSchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "chaos-4977-step7-recurrence-guard"
	const fromTS = "2026-01-01 00:00:00"
	const toTS = "2026-01-05 00:00:00"
	const computedAt = "2026-01-06 00:00:00"

	insert := fmt.Sprintf(
		`INSERT INTO work_unit_investments
			(work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value,
			 theme_distribution_json, subcategory_distribution_json, structural_evidence_json,
			 evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json,
			 categorization_model_version, categorization_input_hash, categorization_run_id,
			 computed_at, work_unit_type, work_unit_name, org_id)
		VALUES
			('wu-1', toDateTime64('%s',3), toDateTime64('%s',3), NULL, NULL, 'fte_days', 1.0,
			 map('velocity', 40.0, 'quality', 10.0), map('velocity.feature', 40.0, 'quality.bugfix', 10.0), '{}',
			 0.8, 'high', 'ok', '', 'v1', 'hash', 'run-1',
			 toDateTime64('%s',3), 'issue', 'Ship the new thing', '%s')`,
		fromTS, toTS, computedAt, orgID,
	)
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_investments: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	rows, err := reader.FetchWorkUnitInvestments(ctx, WorkUnitInvestmentsFilter{
		OrgID:   orgID,
		StartTS: time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC),
		EndTS:   time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("FetchWorkUnitInvestments: %v (this is the CHAOS-4977 step 7 defect if the message "+
			"contains \"converting Map\" / \"unsupported\")", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1 -- got %+v", len(rows), rows)
	}

	row := rows[0]
	gotThemes := zipDistribution(row.ThemeDistributionKeys, row.ThemeDistributionValues)
	wantThemes := map[string]float64{"velocity": 40.0, "quality": 10.0}
	for k, want := range wantThemes {
		if got := gotThemes[k]; got != want {
			t.Errorf("theme_distribution[%q] = %v, want %v (full map: %+v)", k, got, want, gotThemes)
		}
	}
	if len(gotThemes) != len(wantThemes) {
		t.Errorf("theme_distribution has %d keys, want %d -- got %+v", len(gotThemes), len(wantThemes), gotThemes)
	}

	gotSubcategories := zipDistribution(row.SubcategoryDistributionKeys, row.SubcategoryDistributionValues)
	wantSubcategories := map[string]float64{"velocity.feature": 40.0, "quality.bugfix": 10.0}
	for k, want := range wantSubcategories {
		if got := gotSubcategories[k]; got != want {
			t.Errorf("subcategory_distribution[%q] = %v, want %v (full map: %+v)", k, got, want, gotSubcategories)
		}
	}
	if len(gotSubcategories) != len(wantSubcategories) {
		t.Errorf("subcategory_distribution has %d keys, want %d -- got %+v", len(gotSubcategories), len(wantSubcategories), gotSubcategories)
	}
}
