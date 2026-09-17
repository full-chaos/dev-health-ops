//go:build integration

// Recurrence guard for the UInt64-aggregate-vs-Go-scan-type class of
// defect: countDistinctIf() always returns UInt64 in ClickHouse
// regardless of the counted column's type, and every fixture-backed test
// in this package (fixtures_test.go's fixtureRowScanner) answers a Scan
// call by reflecting on the Go destination type it is handed rather than
// replaying the real clickhouse-go driver's own conversion rules -- so a
// scan destination narrower than the aggregate's actual return type
// (originally *int64 here) can pass every fixture test in this package
// while failing every real request. Same gap class, same fix shape, as
// cmd/query-api/internal/investmentexplain's own
// workunitreader_seeded_integration_test.go -- this file exists so this
// specific expression shape (an aggregate-producing UInt64 scanned by
// this package) has a real-driver guard the same way that one already
// does for a real-driver Map(String, Float64) scan.
//
// SCHEMA: work_unit_investments (as investmentexplain's own seeded DDL),
// plus every table fetchInvestmentUnassignedCounts' composed SQL touches
// through repoAllocatedWorkUnitInvestmentsSource / analytics.
// LatestWorkUnitInvestmentsSource / analytics.LatestWorkUnitRepoEffortSource
// / analytics.BuildUnitTeamSubquery: work_unit_supersessions and
// work_unit_membership_runs/work_unit_membership stay EMPTY (same trick
// investmentexplain's own DDL doc comment uses -- an empty
// work_unit_membership_runs makes scope_enabled evaluate to 0, so the
// membership-scope OR short-circuits and the seeded row passes through;
// an empty work_unit_supersessions makes the NOT IN filter vacuously
// true); work_unit_repo_effort, repos and work_item_team_attributions
// also stay empty -- the seeded row has no PR/issue evidence refs and no
// repo_effort row, so it naturally counts as BOTH missing_repo and
// missing_team without needing any row in those three tables, only their
// existence for ClickHouse to resolve the query against.
package investmentflow

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

func splitUnassignedCountsDDL(sql string) []string {
	var out []string
	for _, stmt := range strings.Split(sql, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

const unassignedCountsSeededSchemaDDL = `
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

CREATE TABLE work_unit_repo_effort (
    work_unit_id String,
    repo_id Nullable(UUID),
    effort_metric String,
    effort_value Float64,
    allocation_weight Float64,
    allocation_source String,
    categorization_run_id String,
    repo_source Nullable(String),
    computed_at DateTime64(3, 'UTC'),
    org_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id, ifNull(toString(repo_id), ''));

CREATE TABLE repos (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default',
    provider String DEFAULT 'unknown'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (id);

CREATE TABLE work_item_team_attributions (
    org_id String,
    repo_id UUID,
    work_item_id String,
    provider String,
    team_id Nullable(String),
    team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6),
    is_primary UInt8,
    confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3),
    evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source);
`

// TestFetchInvestmentUnassignedCounts_SeededRealClickHouse_ScansCountDistinctIf
// is this class's recurrence guard. RED on the pre-fix code (unassignedCounts'
// original MissingTeam/MissingRepo int64 fields): fails with "clickhouse
// [ScanRow]: (missing_repo) converting UInt64 to *int64 is unsupported" --
// the exact error this defect's own production telemetry captured. GREEN on
// the uint64 fix.
func TestFetchInvestmentUnassignedCounts_SeededRealClickHouse_ScansCountDistinctIf(t *testing.T) {
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

	for _, stmt := range splitUnassignedCountsDDL(unassignedCountsSeededSchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "chaos-5866-unassigned-counts-recurrence-guard"
	const fromTS = "2026-01-01 00:00:00"
	const toTS = "2026-01-05 00:00:00"
	const computedAt = "2026-01-06 00:00:00"

	// No repo_id (NULL, no work_unit_repo_effort row either -- has_allocation
	// stays 0) and no PR/issue evidence refs in structural_evidence_json (so
	// the unit_team ARRAY JOIN drops this row entirely, leaving it
	// team-unmatched) -- this row counts toward BOTH missing_repo and
	// missing_team, exercising both countDistinctIf() columns the query
	// projects.
	insert := fmt.Sprintf(
		`INSERT INTO work_unit_investments
			(work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value,
			 theme_distribution_json, subcategory_distribution_json, structural_evidence_json,
			 evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json,
			 categorization_model_version, categorization_input_hash, categorization_run_id,
			 computed_at, work_unit_type, work_unit_name, org_id)
		VALUES
			('wu-1', toDateTime64('%s',3), toDateTime64('%s',3), NULL, NULL, 'fte_days', 1.0,
			 map(), map(), '{}',
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

	startTS := time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
	endTS := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)

	got, err := fetchInvestmentUnassignedCounts(ctx, client, startTS, endTS, "", nil, orgID, nil, nil)
	if err != nil {
		t.Fatalf("fetchInvestmentUnassignedCounts: %v (this is the UInt64-scan defect if the message "+
			"contains \"converting UInt64\" / \"unsupported\")", err)
	}
	if got.MissingRepo != 1 {
		t.Errorf("MissingRepo = %d, want 1", got.MissingRepo)
	}
	if got.MissingTeam != 1 {
		t.Errorf("MissingTeam = %d, want 1", got.MissingTeam)
	}
}
