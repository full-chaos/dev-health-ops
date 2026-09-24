//go:build integration

// Recurrence guard for the UInt64-aggregate-vs-Go-scan-type class of
// defect: sum()/countIf() always return UInt64 in ClickHouse regardless
// of the summed/counted column's own width (confirmed against PROD
// system.columns for every source column below: new_items_count,
// new_bugs_count and items_touched are UInt32; churn is UInt32) -- and
// every fixture-backed test in this package (fixtures_test.go's
// fixtureRowScanner) answers a Scan call by reflecting on the Go
// destination type it is handed rather than replaying the real
// clickhouse-go driver's own conversion rules, so a narrower/mismatched
// scan destination can pass every fixture test in this package while
// failing every real request -- exactly what shipped and degraded four
// live routes to 503. Same gap class, same fix shape, as
// internal/queryapi/investmentexplain's own
// workunitreader_seeded_integration_test.go and this package's sibling
// internal/queryapi/investmentflow/
// unassignedcounts_seeded_integration_test.go.
//
// SCHEMA: minimal single-row seeds of work_item_metrics_daily,
// work_item_cycle_times, work_item_state_durations_daily,
// file_metrics_daily and repos -- exactly the tables/columns
// fetchExpenseCounts, fetchExpenseAbandoned, fetchStateStatusCounts and
// fetchHotspotRows read, column types matching PROD's system.columns
// (see this package's queries.go doc comments) rather than this file's
// own guess.
//
// ENGINE NOTE: src/dev_health_ops/migrations/clickhouse/001_metrics_v2.sql
// creates work_item_metrics_daily, work_item_state_durations_daily and
// file_metrics_daily as plain MergeTree, and no later migration in this
// repo changes that -- plain MergeTree does not support FINAL at all
// (ClickHouse error 181), confirmed live while drafting this file. This
// package's own queries.go reads all three FINAL, and this defect's own
// production telemetry shows those exact reads progressing past FINAL to
// the ScanRow stage with no engine error anywhere in the captured
// output, so this file's engine/ORDER BY for all three tables is taken
// from PROD's own system.tables (confirmed drift record:
// _records/schema-drift-prod-engines.tsv -- all three are
// ReplacingMergeTree(computed_at), the same shape two sibling tables'
// own later migrations already moved to), not from this repo's migration
// files: the migration chain does not describe production here.
package sankey

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

func splitAggregateScanDDL(sql string) []string {
	var out []string
	for _, stmt := range strings.Split(sql, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

const aggregateScanSeededSchemaDDL = `
CREATE TABLE work_item_metrics_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  items_started UInt32,
  items_completed UInt32,
  items_started_unassigned UInt32,
  items_completed_unassigned UInt32,
  wip_count_end_of_day UInt32,
  wip_unassigned_end_of_day UInt32,
  bug_completed_ratio Float64,
  story_points_completed Float64,
  new_bugs_count UInt32 DEFAULT 0,
  new_items_count UInt32 DEFAULT 0,
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, day, work_scope_id, team_id);

CREATE TABLE work_item_cycle_times (
  work_item_id String,
  provider LowCardinality(String),
  day Date,
  work_scope_id LowCardinality(String),
  team_id Nullable(String),
  team_name Nullable(String),
  assignee Nullable(String),
  type LowCardinality(String),
  status LowCardinality(String),
  created_at DateTime('UTC'),
  completed_at Nullable(DateTime('UTC')),
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (provider, work_item_id);

CREATE TABLE work_item_state_durations_daily (
  day Date,
  provider LowCardinality(String),
  work_scope_id LowCardinality(String),
  team_id LowCardinality(String),
  team_name String,
  status LowCardinality(String),
  duration_hours Float64,
  items_touched UInt32,
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, provider, work_scope_id, team_id, status, day);

CREATE TABLE file_metrics_daily (
  repo_id UUID,
  day Date,
  path String,
  churn UInt32,
  contributors UInt32,
  commits_count UInt32,
  hotspot_score Float64,
  computed_at DateTime('UTC'),
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(day)
ORDER BY (org_id, repo_id, day, path);

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
`

// TestSankeyAggregateReads_SeededRealClickHouse_ScanFloat64 is this class's
// recurrence guard. RED on the pre-fix code (new_items/new_bugs/
// items_touched/churn scanned straight off sum() with no CAST, and
// canceled_items scanned into *int64 off countIf() with no CAST): every
// one of the four calls below fails with "clickhouse [ScanRow]: ...
// converting UInt64 to *float64 is unsupported" (or *int64 for the
// abandoned-count path), the exact class of error this defect's own
// production telemetry captured. GREEN on the CAST(... AS Float64) fix.
func TestSankeyAggregateReads_SeededRealClickHouse_ScanFloat64(t *testing.T) {
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

	for _, stmt := range splitAggregateScanDDL(aggregateScanSeededSchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "chaos-5866-aggregate-scan-recurrence-guard"
	const day1 = "2026-01-02"
	const computedAt = "2026-01-02 12:00:00"

	// fetchExpenseCounts + fetchExpenseAbandoned (buildExpenseFlow):
	// new_bugs_count=8 (unplanned), items_completed*bug_completed_ratio=
	// 10*0.5=5 (bug_completed), two canceled cycle-time rows (abandoned).
	metricsInsert := fmt.Sprintf(
		`INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
			 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
			 wip_unassigned_end_of_day, bug_completed_ratio, story_points_completed,
			 new_bugs_count, new_items_count, computed_at, org_id)
		VALUES
			('%s', 'github', 'scope-1', '', '', 20, 10, 0, 0, 0, 0, 0.5, 0.0, 8, 10, toDateTime('%s'), '%s')`,
		day1, computedAt, orgID,
	)
	if err := conn.Exec(ctx, metricsInsert); err != nil {
		t.Fatalf("seed work_item_metrics_daily: %v", err)
	}

	for i := 0; i < 2; i++ {
		cycleInsert := fmt.Sprintf(
			`INSERT INTO work_item_cycle_times
				(work_item_id, provider, day, work_scope_id, type, status, created_at, computed_at, org_id)
			VALUES
				('wi-%d', 'github', '%s', 'scope-1', 'bug', 'canceled', toDateTime('%s'), toDateTime('%s'), '%s')`,
			i, day1, computedAt, computedAt, orgID,
		)
		if err := conn.Exec(ctx, cycleInsert); err != nil {
			t.Fatalf("seed work_item_cycle_times row %d: %v", i, err)
		}
	}

	// fetchStateStatusCounts (buildStateFlow): backlog=6, todo=4 so
	// flow_backlog = min(backlog, todo) = 4 produces one Link.
	stateRows := []struct {
		status string
		count  int
	}{
		{"backlog", 6},
		{"todo", 4},
	}
	for _, sr := range stateRows {
		stateInsert := fmt.Sprintf(
			`INSERT INTO work_item_state_durations_daily
				(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
			VALUES
				('%s', 'github', 'scope-1', '', '', '%s', 1.0, %d, toDateTime('%s'), '%s')`,
			day1, sr.status, sr.count, computedAt, orgID,
		)
		if err := conn.Exec(ctx, stateInsert); err != nil {
			t.Fatalf("seed work_item_state_durations_daily (%s): %v", sr.status, err)
		}
	}

	// fetchHotspotRows (buildHotspotFlow): one repo, one file, churn=15.
	const repoID = "11111111-1111-1111-1111-111111111111"
	repoInsert := fmt.Sprintf(
		`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES ('%s', 'acme/widgets', toDateTime64('%s',3), toDateTime64('%s',3), '%s', 'github')`,
		repoID, computedAt, computedAt, orgID,
	)
	if err := conn.Exec(ctx, repoInsert); err != nil {
		t.Fatalf("seed repos: %v", err)
	}
	fileInsert := fmt.Sprintf(
		`INSERT INTO file_metrics_daily (repo_id, day, path, churn, contributors, commits_count, hotspot_score, computed_at, org_id)
		VALUES ('%s', '%s', 'src/widget.go', 15, 1, 1, 0.0, toDateTime('%s'), '%s')`,
		repoID, day1, computedAt, orgID,
	)
	if err := conn.Exec(ctx, fileInsert); err != nil {
		t.Fatalf("seed file_metrics_daily: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	startDay := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	endDay := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	t.Run("buildExpenseFlow", func(t *testing.T) {
		_, links, err := buildExpenseFlow(ctx, client, startDay, endDay, "", nil, nil, orgID)
		if err != nil {
			t.Fatalf("buildExpenseFlow: %v (this is the UInt64-scan defect if the message contains "+
				"\"converting UInt64\" / \"unsupported\")", err)
		}
		if len(links) == 0 {
			t.Fatalf("buildExpenseFlow: got 0 links, want at least 1 (Planned work -> Unplanned work)")
		}
		if got, want := links[0].Value, 8.0; got != want {
			t.Errorf("first link value = %v, want %v (unplanned = new_bugs)", got, want)
		}
	})

	t.Run("buildStateFlow", func(t *testing.T) {
		_, links, err := buildStateFlow(ctx, client, startDay, endDay, "", nil, nil, orgID)
		if err != nil {
			t.Fatalf("buildStateFlow: %v (this is the UInt64-scan defect if the message contains "+
				"\"converting UInt64\" / \"unsupported\")", err)
		}
		if len(links) == 0 {
			t.Fatalf("buildStateFlow: got 0 links, want at least 1 (Backlog -> Todo)")
		}
		if got, want := links[0].Value, 4.0; got != want {
			t.Errorf("first link value = %v, want %v (min(backlog=6, todo=4))", got, want)
		}
	})

	t.Run("buildHotspotFlow", func(t *testing.T) {
		_, links, err := buildHotspotFlow(ctx, client, startDay, endDay, "", nil, nil, orgID, teamScopeAsOf)
		if err != nil {
			t.Fatalf("buildHotspotFlow: %v (this is the UInt64-scan defect if the message contains "+
				"\"converting UInt64\" / \"unsupported\")", err)
		}
		if len(links) == 0 {
			t.Fatalf("buildHotspotFlow: got 0 links, want at least 1 (repo -> directory)")
		}
		if got, want := links[0].Value, 15.0; got != want {
			t.Errorf("first link value = %v, want %v (churn)", got, want)
		}
	})
}
