//go:build integration

// Executed-SQL proof against a REAL ClickHouse container, schema applied
// through the canonical migration chain (chschema.Apply) -- the dedup
// mechanism this package's query shares with
// internal/queryapi/home's fetchRecommendationSignals (same table, same
// two-stage argMax shape) is already proven there; this test proves
// THIS package's own filter/order variant end to end: one team, no
// LIMIT, ordered by window_end DESC then rule_id.
package recommendations

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestResolve_SeededRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client, err := chquery.NewProductionClient(inst.URI)
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const orgID = "recommendations-seeded-it"
	const tOld = "2026-04-08 03:00:00"
	const tNew = "2026-04-08 04:00:00"

	exec := func(stmt string) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	// Same (org_id, team_id, rule_id, window_end) key, two computed_at
	// versions: old is a fired=false tombstone (must be superseded), new
	// is fired=true. Proves the INNER argMax(..., computed_at) picks the
	// latest run, matching TestHomeReaders_SeededRealClickHouse's own
	// recommendations_daily case.
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-a', '` + orgID + `', 'rule-1', '2026-04-01', '2026-04-07', false, 'warning', 'stale', 'stale', 'stale', '[]', toDateTime64('` + tOld + `',3))`)
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-a', '` + orgID + `', 'rule-1', '2026-04-01', '2026-04-07', true, 'critical', 'fresh title', 'fresh rationale', 'fresh criterion', '[{"team_id":"team-a","metric_table":"work_item_metrics_daily","field":"wip_count","window_start":"2026-04-01","window_end":"2026-04-07","value":9.0}]', toDateTime64('` + tNew + `',3))`)

	// A different team in the SAME org must never leak into team-a's
	// result -- proves the WHERE team_id = {team_id:String} filter, the
	// one clause this query's variant changes vs. queries_signals.go's
	// team_id IN (...) shape.
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-b', '` + orgID + `', 'rule-2', '2026-04-01', '2026-04-07', true, 'warning', 'other team', 'other team', 'other team', '[]', toDateTime64('` + tNew + `',3))`)

	// A row OUTSIDE the read window (window_end before window_start) must
	// never surface -- proves the window_end >= / <= bounds.
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-a', '` + orgID + `', 'rule-old', '2025-01-01', '2025-01-08', true, 'warning', 'too old', 'too old', 'too old', '[]', toDateTime64('` + tNew + `',3))`)

	// Same team, DIFFERENT org: must never surface for orgID -- proves the
	// outer/inner org_id predicate (the P3 org-scope guard: removing it
	// makes this row appear).
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-a', 'other-org-it', 'rule-foreign', '2026-04-01', '2026-04-07', true, 'critical', 'foreign org', 'foreign org', 'foreign org', '[]', toDateTime64('` + tNew + `',3))`)

	// Ordering guard: ORDER BY latest_window_end DESC, rule_id. rule-3 ties
	// rule-1 on window_end (tie broken by rule_id, so after rule-1); rule-0
	// has an earlier window_end (so LAST although its id sorts first).
	exec(`INSERT INTO recommendations_daily
		(team_id, org_id, rule_id, window_start, window_end, fired, severity, title, rationale, success_criterion, evidence_json, computed_at) VALUES
		('team-a', '` + orgID + `', 'rule-3', '2026-04-01', '2026-04-07', true, 'warning', 'tie', 'tie', 'tie', '[]', toDateTime64('` + tNew + `',3)),
		('team-a', '` + orgID + `', 'rule-0', '2026-03-25', '2026-04-03', true, 'warning', 'earlier', 'earlier', 'earlier', '[]', toDateTime64('` + tNew + `',3))`)

	now := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	got := Resolve(ctx, client, orgID, "team-a", model.WindowInput{Value: 4, Unit: model.WindowUnitWeek}, now)

	if len(got) != 3 {
		t.Fatalf("Resolve returned %d recommendations, want 3 (rule-1, rule-3, rule-0): %+v", len(got), got)
	}
	if got[0].RuleID != "rule-1" || got[1].RuleID != "rule-3" || got[2].RuleID != "rule-0" {
		t.Errorf("order = %s,%s,%s; want rule-1,rule-3,rule-0 (window_end DESC, then rule_id)", got[0].RuleID, got[1].RuleID, got[2].RuleID)
	}
	rec := got[0]
	if rec.RuleID != "rule-1" {
		t.Errorf("RuleID = %q, want rule-1", rec.RuleID)
	}
	if rec.TeamID != "team-a" {
		t.Errorf("TeamID = %q, want team-a (team-b must not leak in)", rec.TeamID)
	}
	if rec.Severity != model.SeverityCritical {
		t.Errorf("Severity = %q, want CRITICAL (the fresh row, not the stale tombstone)", rec.Severity)
	}
	if rec.Title != "fresh title" {
		t.Errorf("Title = %q, want the fresh row's title, not the stale tombstone's", rec.Title)
	}
	if len(rec.Evidence) != 1 || rec.Evidence[0].Field != "wip_count" {
		t.Errorf("Evidence = %+v, want one wip_count entry", rec.Evidence)
	}
}
