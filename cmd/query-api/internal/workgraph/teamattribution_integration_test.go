//go:build integration

package workgraph

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// newTeamAttributionTestClickHouse starts a real testcontainers ClickHouse,
// migrates it to the real chain's head (chschema.Apply -- see that
// package's doc comment for why hand-typed DDL is not used here: the
// work_item_team_attributions enum has grown source/confidence values
// across several migrations, and only the real chain has all of them), and
// returns both an admin connection (for seeding) and a
// workgraph.QueryClient (for exercising resolveWorkUnitTeamAttributions
// exactly as schema.resolvers.go does).
func newTeamAttributionTestClickHouse(ctx context.Context, t *testing.T) (admin stdclickhouse.Conn, client QueryClient) {
	t.Helper()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err = stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	queryClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = queryClient.Close() })

	return admin, queryClient
}

// seedRunMarker writes the work_unit_membership_runs completion marker --
// required, same write-protocol-order discipline
// membership_integration_test.go documents: a run is visible to readers
// only once its marker row exists.
func seedRunMarker(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, runID string, completedAt time.Time) {
	t.Helper()
	if err := admin.Exec(ctx, `
        INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
        VALUES (?, ?, ?)
    `, orgID, runID, completedAt); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}
}

// membershipSeed is one work_unit_membership row: a (node_id) that belongs
// to workUnitID.
type membershipSeed struct {
	nodeType, nodeID, workUnitID string
}

func seedMembership(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID, runID string, computedAt time.Time, rows []membershipSeed) {
	t.Helper()
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_unit_membership (
            org_id, node_type, node_id, work_unit_id, category_kind, category,
            weight, is_dominant, categorization_status, computed_at, run_id
        )
    `)
	if err != nil {
		t.Fatalf("prepare membership batch: %v", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			orgID, row.nodeType, row.nodeID, row.workUnitID, "theme", "x",
			1.0, uint8(1), "ok", computedAt, runID,
		); err != nil {
			t.Fatalf("append membership row %+v: %v", row, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send membership batch: %v", err)
	}
}

// attributionSeed is one work_item_team_attributions row.
type attributionSeed struct {
	workItemID         string
	teamID, teamName   string // "" -> written as SQL NULL, not bound (matches teamownership_integration_test.go's convention)
	source, confidence string
}

func seedAttributions(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, orgID string, computedAt time.Time, rows []attributionSeed) {
	t.Helper()
	for _, row := range rows {
		teamIDSQL := "?"
		teamNameSQL := "?"
		args := []any{orgID, row.workItemID}
		if row.teamID == "" {
			teamIDSQL = "NULL"
		} else {
			args = append(args, row.teamID)
		}
		if row.teamName == "" {
			teamNameSQL = "NULL"
		} else {
			args = append(args, row.teamName)
		}
		args = append(args, row.source, row.confidence, computedAt)
		stmt := `INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES (` +
			`?, toUUID('00000000-0000-0000-0000-000000000000'), ?, 'linear', ` + teamIDSQL + `, ` + teamNameSQL + `, ?, 1, ?, 'seed', ?)`
		if err := admin.Exec(ctx, stmt, args...); err != nil {
			t.Fatalf("seed work_item_team_attributions row %+v: %v", row, err)
		}
	}
}

// TestResolveWorkUnitTeamAttributions_SourcePrecedenceAndShapeRealEngine is
// the red-first proof this port's whole point rests on: against a REAL
// ClickHouse engine (not a fake), the winning team per work unit must be
// chosen by SOURCE PRECEDENCE (teamAttributionSourceRankSQL's rank order),
// not by which team has more member work items -- team_attribution.py's
// `(min(src_rank), -toInt64(count()), team_id)` sort_key. Also pins
// end-to-end row shape: member_count scoped to the WINNING team only,
// evidence text, and empty-string(NULL)-team mapping to a nil *string.
func TestResolveWorkUnitTeamAttributions_SourcePrecedenceAndShapeRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin, client := newTeamAttributionTestClickHouse(ctx, t)

	const orgID = "org-3969-precedence"
	const runID = "run-3969-precedence"
	now := time.Now().UTC()
	seedRunMarker(ctx, t, admin, orgID, runID, now)

	// wu-native: one item attributed via native_team (rank 0, team T1), one
	// via project_ownership (rank 2, team T2) -- T1 must win despite both
	// teams having exactly one member item each.
	//
	// wu-tiebreak: two items attributed via native_team (rank 0) to team
	// T1, one item attributed via native_team to team T3 -- same rank, so
	// the -count(DESC) tiebreak must pick T1 (2 members) over T3 (1
	// member).
	//
	// wu-unassigned: one item with NULL team_id/team_name, source
	// 'unassigned' -- must map to nil *string team fields.
	seedMembership(ctx, t, admin, orgID, runID, now, []membershipSeed{
		{"issue", "wi-native", "wu-native"},
		{"issue", "wi-project", "wu-native"},
		{"issue", "wi-tie-1", "wu-tiebreak"},
		{"issue", "wi-tie-2", "wu-tiebreak"},
		{"issue", "wi-tie-3", "wu-tiebreak"},
		{"issue", "wi-unassigned", "wu-unassigned"},
	})
	seedAttributions(ctx, t, admin, orgID, now, []attributionSeed{
		{workItemID: "wi-native", teamID: "team-1", teamName: "Team Alpha", source: "native_team", confidence: "high"},
		{workItemID: "wi-project", teamID: "team-2", teamName: "Team Beta", source: "project_ownership", confidence: "medium"},
		{workItemID: "wi-tie-1", teamID: "team-1", teamName: "Team Alpha", source: "native_team", confidence: "high"},
		{workItemID: "wi-tie-2", teamID: "team-1", teamName: "Team Alpha", source: "native_team", confidence: "high"},
		{workItemID: "wi-tie-3", teamID: "team-3", teamName: "Team Gamma", source: "native_team", confidence: "high"},
		{workItemID: "wi-unassigned", teamID: "", teamName: "", source: "unassigned", confidence: "none"},
	})

	got, err := resolveWorkUnitTeamAttributions(ctx, client, orgID, nil, nil, workUnitTeamAttributionsMaxRows)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions: %v", err)
	}

	byWorkUnit := map[string]int{}
	for i, r := range got {
		byWorkUnit[r.WorkUnitID] = i
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want 3: %+v", len(got), got)
	}

	native := got[byWorkUnit["wu-native"]]
	if native.TeamID == nil || *native.TeamID != "team-1" {
		t.Fatalf("wu-native: source precedence not applied, got team %+v, want team-1 (native_team must beat project_ownership)", native.TeamID)
	}
	if native.Source != "NATIVE_TEAM" || native.Confidence != "HIGH" || native.MemberCount != 1 {
		t.Fatalf("wu-native shape: %+v", native)
	}
	wantEvidence := "1 member work item(s) attributed to Team Alpha via native_team"
	if native.Evidence != wantEvidence {
		t.Fatalf("wu-native evidence = %q, want %q", native.Evidence, wantEvidence)
	}

	tiebreak := got[byWorkUnit["wu-tiebreak"]]
	if tiebreak.TeamID == nil || *tiebreak.TeamID != "team-1" || tiebreak.MemberCount != 2 {
		t.Fatalf("wu-tiebreak: -count(DESC) tiebreak not applied, got %+v, want team-1 with 2 members", tiebreak)
	}

	unassigned := got[byWorkUnit["wu-unassigned"]]
	if unassigned.TeamID != nil || unassigned.TeamName != nil {
		t.Fatalf("wu-unassigned: NULL team columns must map to nil *string, got %+v", unassigned)
	}
	if unassigned.Source != "UNASSIGNED" || unassigned.Confidence != "NONE" {
		t.Fatalf("wu-unassigned enums: %+v", unassigned)
	}
}

// TestResolveWorkUnitTeamAttributions_FiltersRealEngine pins the
// workUnitIds/teamId filters against a real engine.
func TestResolveWorkUnitTeamAttributions_FiltersRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin, client := newTeamAttributionTestClickHouse(ctx, t)

	const orgID = "org-3969-filters"
	const runID = "run-3969-filters"
	now := time.Now().UTC()
	seedRunMarker(ctx, t, admin, orgID, runID, now)
	seedMembership(ctx, t, admin, orgID, runID, now, []membershipSeed{
		{"issue", "wi-a", "wu-a"},
		{"issue", "wi-b", "wu-b"},
	})
	seedAttributions(ctx, t, admin, orgID, now, []attributionSeed{
		{workItemID: "wi-a", teamID: "team-a", teamName: "Team A", source: "native_team", confidence: "high"},
		{workItemID: "wi-b", teamID: "team-b", teamName: "Team B", source: "native_team", confidence: "high"},
	})

	// workUnitIds filter.
	got, err := resolveWorkUnitTeamAttributions(ctx, client, orgID, []string{"wu-a"}, nil, workUnitTeamAttributionsMaxRows)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions (work_unit_ids filter): %v", err)
	}
	if len(got) != 1 || got[0].WorkUnitID != "wu-a" {
		t.Fatalf("work_unit_ids filter: got %+v, want exactly wu-a", got)
	}

	// teamId filter.
	teamB := "team-b"
	got, err = resolveWorkUnitTeamAttributions(ctx, client, orgID, nil, &teamB, workUnitTeamAttributionsMaxRows)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions (team_id filter): %v", err)
	}
	if len(got) != 1 || got[0].WorkUnitID != "wu-b" {
		t.Fatalf("team_id filter: got %+v, want exactly wu-b", got)
	}
}

// TestResolveWorkUnitTeamAttributions_TruncationSignalRealEngine is the
// real-engine half of CHAOS-3969's truncation-signal proof (the
// fake-client half is
// TestResolveWorkUnitTeamAttributions_TruncationSignalFiresWhenProbeRowReturned
// / TestResolveWorkUnitTeamAttributions_NoTruncationSignalWhenExactlyAtLimit
// in teamattribution_test.go): seeds MORE qualifying work units than a
// small test-local limit override, exercises the real query end to end,
// and asserts BOTH that the result is capped at `limit` AND that
// recordWorkUnitTeamAttributionsTruncation's slog line actually fires --
// captured via a real slog.JSONHandler + buffer, same convention
// principal/rejection_telemetry_test.go and
// routeswitch/postgres_switch_integration_test.go already use in this
// binary, not a mocked logger.
func TestResolveWorkUnitTeamAttributions_TruncationSignalRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	admin, client := newTeamAttributionTestClickHouse(ctx, t)

	const orgID = "org-3969-truncation"
	const runID = "run-3969-truncation"
	now := time.Now().UTC()
	seedRunMarker(ctx, t, admin, orgID, runID, now)
	seedMembership(ctx, t, admin, orgID, runID, now, []membershipSeed{
		{"issue", "wi-1", "wu-1"},
		{"issue", "wi-2", "wu-2"},
		{"issue", "wi-3", "wu-3"},
	})
	seedAttributions(ctx, t, admin, orgID, now, []attributionSeed{
		{workItemID: "wi-1", teamID: "team-1", teamName: "Team 1", source: "native_team", confidence: "high"},
		{workItemID: "wi-2", teamID: "team-2", teamName: "Team 2", source: "native_team", confidence: "high"},
		{workItemID: "wi-3", teamID: "team-3", teamName: "Team 3", source: "native_team", confidence: "high"},
	})

	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, nil)))
	defer slog.SetDefault(prevLogger)

	const smallLimit = 2
	got, err := resolveWorkUnitTeamAttributions(ctx, client, orgID, nil, nil, smallLimit)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions: %v", err)
	}
	if len(got) != smallLimit {
		t.Fatalf("got %d results, want exactly the %d-row cap", len(got), smallLimit)
	}

	found := false
	for _, line := range strings.Split(strings.TrimSpace(logBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not valid JSON: %s: %v", line, err)
		}
		if record["msg"] == "query_api.workgraph.work_unit_team_attributions.truncated" {
			found = true
			if record["org_id"] != orgID {
				t.Fatalf("truncation log org_id = %v, want %q", record["org_id"], orgID)
			}
			if record["limit"] != float64(smallLimit) {
				t.Fatalf("truncation log limit = %v, want %d", record["limit"], smallLimit)
			}
		}
	}
	if !found {
		t.Fatalf("truncation warning never logged; captured log output: %s", logBuf.String())
	}

	// CHAOS-3969 round 2 (team-lead review): the log line alone is not a
	// sufficient signal -- a COUNTER must fire too, so an operator has
	// something to alert on. This reads the REAL counter (this package's
	// shared realMeterReader, main_test.go) after the SAME truncating
	// call above -- the log-line proof and the counter proof are the
	// identical call, not two separate setups -- through the unswapped
	// recordWorkUnitTeamAttributionsTruncation/
	// incrementWorkUnitTeamAttributionsTruncationCounter default chain
	// (every OTHER test in this package that exercises truncation swaps
	// those vars to a spy and restores them via t.Cleanup, so this is the
	// only place in the suite that increments the real instrument).
	var rm metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	sawCounter := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "devhealth_query_api_workgraph_truncation_total" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok || len(data.DataPoints) != 1 {
				t.Fatalf("counter data shape = %+v, want one int64 sum data point", m.Data)
			}
			dp := data.DataPoints[0]
			sawCounter = true
			if dp.Value != 1 {
				t.Errorf("counter value = %d, want 1", dp.Value)
			}
			if got, ok := dp.Attributes.Value("family"); !ok || got.AsString() != "team_attribution" {
				t.Errorf("counter family attribute = %v (present=%v), want team_attribution", got, ok)
			}
			if got, ok := dp.Attributes.Value("op"); !ok || got.AsString() != "work_unit_team_attributions" {
				t.Errorf("counter op attribute = %v (present=%v), want work_unit_team_attributions", got, ok)
			}
		}
	}
	if !sawCounter {
		t.Fatalf("devhealth_query_api_workgraph_truncation_total was never recorded")
	}

	// Negative case: a limit above the real row count must NOT log.
	logBuf.Reset()
	got, err = resolveWorkUnitTeamAttributions(ctx, client, orgID, nil, nil, workUnitTeamAttributionsMaxRows)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions (unbounded): %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want all 3 (no truncation)", len(got))
	}
	if strings.Contains(logBuf.String(), "work_unit_team_attributions.truncated") {
		t.Fatalf("truncation warning logged when result was below limit: %s", logBuf.String())
	}

	// TRUNC-1 real-engine negative case (codex round chaos-3969-r1, P2,
	// team-lead-ruled fix-before-open): a limit EXACTLY equal to the real
	// row count (3 seeded work units, limit=3) must fire NEITHER the log
	// NOR the counter -- this is the exact false positive codex found
	// against a real engine, not just a fake one. The counter is a
	// cumulative Sum, so this asserts the value is UNCHANGED from what it
	// was after the genuine-truncation call above (still 1), not merely
	// present.
	var beforeRM metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &beforeRM); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	counterValueBefore := truncationCounterValue(t, beforeRM)

	logBuf.Reset()
	got, err = resolveWorkUnitTeamAttributions(ctx, client, orgID, nil, nil, 3)
	if err != nil {
		t.Fatalf("resolveWorkUnitTeamAttributions (limit == true count): %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want all 3", len(got))
	}
	if strings.Contains(logBuf.String(), "work_unit_team_attributions.truncated") {
		t.Fatalf("truncation warning logged for a limit exactly equal to the real row count (false positive, TRUNC-1): %s", logBuf.String())
	}

	var afterRM metricdata.ResourceMetrics
	if err := realMeterReader.Collect(ctx, &afterRM); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	if got := truncationCounterValue(t, afterRM); got != counterValueBefore {
		t.Fatalf("truncation counter incremented (%d -> %d) for a limit exactly equal to the real row count (false positive, TRUNC-1)", counterValueBefore, got)
	}
}

// truncationCounterValue returns the current value of the
// devhealth_query_api_workgraph_truncation_total counter's one data point
// (family=team_attribution, op=work_unit_team_attributions is this
// package's only series on it today), or 0 if it has never been recorded
// yet.
func truncationCounterValue(t *testing.T, rm metricdata.ResourceMetrics) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "devhealth_query_api_workgraph_truncation_total" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok || len(data.DataPoints) != 1 {
				t.Fatalf("counter data shape = %+v, want one int64 sum data point", m.Data)
			}
			return data.DataPoints[0].Value
		}
	}
	return 0
}
