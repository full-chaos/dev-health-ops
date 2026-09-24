//go:build integration

package testopsrisk

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var ddls = []string{
	`CREATE TABLE testops_release_confidence (repo_id UUID, day Date, confidence_score Float64, factors_json String DEFAULT '{}',
      org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	`CREATE TABLE testops_quality_drag (repo_id UUID, day Date, drag_hours Float64, failure_rework_hours Float64, flake_investigation_hours Float64,
      queue_wait_hours Float64, retry_overhead_hours Float64, org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC'))
      ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	`CREATE TABLE testops_pipeline_stability (repo_id UUID, day Date, stability_index Float64, org_id LowCardinality(String) DEFAULT '',
      computed_at DateTime('UTC')) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	`CREATE TABLE repos (id UUID, repo String, org_id String DEFAULT 'default', created_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'))
      ENGINE = ReplacingMergeTree(last_synced) ORDER BY id`,
}

const (
	rA = "11111111-1111-1111-1111-111111111111"
	rB = "22222222-2222-2222-2222-222222222222"
	rC = "33333333-3333-3333-3333-333333333333"
)

func startStore(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, d := range ddls {
		if err := conn.Exec(ctx, d); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, conn, client
}

func exec(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, q string, a ...any) {
	t.Helper()
	if err := conn.Exec(ctx, fmt.Sprintf(q, a...)); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func conf(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repo, day string, score float64, factors, computed string) {
	exec(t, ctx, conn, `INSERT INTO testops_release_confidence (repo_id, day, confidence_score, factors_json, org_id, computed_at) SELECT '%s', '%s', %v, '%s', '%s', '%s'`, repo, day, score, factors, org, computed)
}

func drag(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repo, day string, hours, fr, fl, qw, ro float64, computed string) {
	exec(t, ctx, conn, `INSERT INTO testops_quality_drag (repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours, queue_wait_hours, retry_overhead_hours, org_id, computed_at) SELECT '%s', '%s', %v, %v, %v, %v, %v, '%s', '%s'`, repo, day, hours, fr, fl, qw, ro, org, computed)
}

func stab(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repo, day string, idx float64, computed string) {
	exec(t, ctx, conn, `INSERT INTO testops_pipeline_stability (repo_id, day, stability_index, org_id, computed_at) SELECT '%s', '%s', %v, '%s', '%s'`, repo, day, idx, org, computed)
}

func TestRealClickHouse_TestopsRisk(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/web', 'org-1', now64(3), now64(3)`, rA)
	// the same repository id in another org, with a different name
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'other/web', 'org-2', now64(3), now64(3)`, rA)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/api', 'org-1', now64(3), now64(3)`, rB)
	const f1 = `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`
	const f2 = `{"pipeline_success_rate": 0.5, "test_pass_rate": 0.4}`
	// day 01: rA .8 (older compute .1 must lose) and rB .6 -> mean .7; drag rA 2 + rB 3 = 5; stability rA .9
	conf(t, ctx, conn, "org-1", rA, "2026-01-01", 0.1, f2, "2026-01-01 01:00:00")
	conf(t, ctx, conn, "org-1", rA, "2026-01-01", 0.8, f1, "2026-01-01 02:00:00")
	conf(t, ctx, conn, "org-1", rB, "2026-01-01", 0.6, f2, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-1", rA, "2026-01-01", 2, 1, 0.5, 0.25, 0.25, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-1", rB, "2026-01-01", 3, 1, 1, 0.5, 0.5, "2026-01-01 02:00:00")
	stab(t, ctx, conn, "org-1", rA, "2026-01-01", 0.9, "2026-01-01 02:00:00")
	// day 02: only stability (a day the other two tables lack) -> the full outer join keeps it
	stab(t, ctx, conn, "org-1", rA, "2026-01-02", 0.5, "2026-01-02 02:00:00")
	// day 03: confidence only
	conf(t, ctx, conn, "org-1", rA, "2026-01-03", 0.5, f1, "2026-01-03 02:00:00")
	// outside the range on both sides, and another org on an in-range day
	conf(t, ctx, conn, "org-1", rA, "2025-12-31", 0.99, f1, "2025-12-31 02:00:00")
	conf(t, ctx, conn, "org-1", rA, "2026-02-01", 0.99, f1, "2026-02-01 02:00:00")
	conf(t, ctx, conn, "org-2", rA, "2026-01-01", 0.05, f1, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-2", rA, "2026-01-01", 99, 9, 9, 9, 9, "2026-01-01 02:00:00")
	stab(t, ctx, conn, "org-2", rA, "2026-01-01", 0.01, "2026-01-01 02:00:00")
	// rC has no repos row: its label is its id
	conf(t, ctx, conn, "org-1", rC, "2026-01-03", 0.9, f1, "2026-01-03 03:00:00")

	in := model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")}
	got, err := Resolve(ctx, client, "org-1", in)
	if err != nil {
		t.Fatal(err)
	}
	// days: 01, 02 (stability only), 03 (confidence only: mean of rA .5 and rC .9 = .7)
	if len(got.Timeseries) != 2 || got.Timeseries[0].Date.String() != "2026-01-01" || got.Timeseries[1].Date.String() != "2026-01-03" {
		t.Fatalf("timeseries %#v", got.Timeseries)
	}
	if r := got.Timeseries[0].RiskScore; r < 0.2999 || r > 0.3001 {
		t.Errorf("day 1 risk %v (mean confidence .7, the newer compute of rA wins)", r)
	}
	if r := got.Timeseries[1].RiskScore; r < 0.2999 || r > 0.3001 {
		t.Errorf("day 3 risk %v", r)
	}
	if got.ReleaseConfidence == nil || *got.ReleaseConfidence < 0.6999 || *got.ReleaseConfidence > 0.7001 {
		t.Errorf("latest confidence %v", got.ReleaseConfidence)
	}
	if got.QualityDragHours == nil || *got.QualityDragHours != 5 {
		t.Errorf("drag %v (sum of the latest per repo)", got.QualityDragHours)
	}
	if fmt.Sprint(got.QualityDragBreakdown) != "[{Failure Rework 2} {Flake Investigation 1.5} {Queue Wait 0.75} {Retry Overhead 0.75}]" {
		t.Errorf("breakdown %v", got.QualityDragBreakdown)
	}
	if got.PipelineStability == nil || *got.PipelineStability != 0.5 {
		t.Errorf("stability %v (latest day that has one, day 2)", got.PipelineStability)
	}
	if len(got.StabilitySpark) != 2 || got.StabilitySpark[0].Value < 89.999 || got.StabilitySpark[0].Value > 90.001 || got.StabilityDelta == nil || *got.StabilityDelta > -44.4 || *got.StabilityDelta < -44.5 {
		t.Errorf("stability spark %#v delta %v", got.StabilitySpark, got.StabilityDelta)
	}
	if len(got.DragSpark) != 1 || got.DragDelta != nil {
		t.Errorf("drag spark %#v", got.DragSpark)
	}
	// quadrant: one row per repo from its latest row, least confident first, org-scoped labels
	if len(got.QuadrantData) != 3 {
		t.Fatalf("quadrant %#v", got.QuadrantData)
	}
	// latest by (day, computed_at): rA day 03 (.5, f1) ; rB day 01 (.6, f2); rC day 03 (.9, f1) -> order .5, .6, .9
	wantIDs := []string{"acme/web", "acme/api", rC}
	for i, q := range got.QuadrantData {
		if q.ID != wantIDs[i] {
			t.Errorf("quadrant[%d] id %q want %q", i, q.ID, wantIDs[i])
		}
	}
	if *got.QuadrantData[0].PipelineSuccessRate != 0.9 || *got.QuadrantData[1].TestPassRate != 0.4 {
		t.Errorf("quadrant rates %#v", got.QuadrantData)
	}

	other, err := Resolve(ctx, client, "org-2", in)
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Timeseries) != 1 || *other.ReleaseConfidence != 0.05 || *other.QualityDragHours != 99 || len(other.QuadrantData) != 1 || other.QuadrantData[0].ID != "other/web" {
		t.Errorf("org-2 sees %#v", other)
	}
	none, err := Resolve(ctx, client, "org-3", in)
	if err != nil {
		t.Fatal(err)
	}
	if len(none.Timeseries) != 0 || none.ReleaseConfidence != nil || len(none.QualityDragBreakdown) != 0 || none.Timeseries == nil {
		t.Errorf("unknown org %#v", none)
	}
	// The range is inclusive on both ends.
	edge, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-03"), EndDate: date("2026-01-03")})
	if err != nil {
		t.Fatal(err)
	}
	if len(edge.Timeseries) != 1 || edge.Timeseries[0].Date.String() != "2026-01-03" {
		t.Errorf("single-day range %#v", edge.Timeseries)
	}
}

// The quadrant reads each repository's latest row by (day, computed_at): a
// newer day computed earlier still beats an older day computed later.
func TestRealClickHouse_TestopsRiskQuadrantLatestByDay(t *testing.T) {
	ctx, conn, client := startStore(t)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/api', 'org-1', now64(3), now64(3)`, rB)
	conf(t, ctx, conn, "org-1", rB, "2026-01-04", 0.3, `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`, "2026-01-04 01:00:00")
	conf(t, ctx, conn, "org-1", rB, "2026-01-02", 0.99, `{"pipeline_success_rate": 0.1, "test_pass_rate": 0.2}`, "2026-01-09 01:00:00")
	got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.QuadrantData) != 1 || *got.QuadrantData[0].PipelineSuccessRate != 0.9 || *got.QuadrantData[0].TestPassRate != 0.8 {
		t.Fatalf("quadrant %#v", got.QuadrantData)
	}
}

// A factor missing from the latest row's factors_json (absent key, JSON null)
// reads as absent, never as 0; a present 0 stays 0, and an older row's value
// never stands in for the latest row's missing one.
func TestRealClickHouse_TestopsRiskQuadrantMissingFactorIsNull(t *testing.T) {
	ctx, conn, client := startStore(t)
	for i, r := range []string{rA, rB, rC} {
		exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/r%d', 'org-1', now64(3), now64(3)`, r, i)
	}
	// rA: latest row has neither factor, an older row has both -> both absent
	conf(t, ctx, conn, "org-1", rA, "2026-01-01", 0.1, `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`, "2026-01-01 01:00:00")
	conf(t, ctx, conn, "org-1", rA, "2026-01-02", 0.2, `{}`, "2026-01-02 01:00:00")
	// rB: one factor present as 0, the other JSON null -> 0 and absent
	conf(t, ctx, conn, "org-1", rB, "2026-01-01", 0.3, `{"pipeline_success_rate": 0, "test_pass_rate": null}`, "2026-01-01 01:00:00")
	// rC: both present
	conf(t, ctx, conn, "org-1", rC, "2026-01-01", 0.4, `{"pipeline_success_rate": 0.7, "test_pass_rate": 0.6}`, "2026-01-01 01:00:00")
	got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.QuadrantData) != 3 {
		t.Fatalf("quadrant %#v", got.QuadrantData)
	}
	byID := map[string]model.TestOpsRiskQuadrantPoint{}
	for _, q := range got.QuadrantData {
		byID[q.ID] = q
	}
	if q := byID["acme/r0"]; q.PipelineSuccessRate != nil || q.TestPassRate != nil {
		t.Errorf("missing factors must be absent: %#v", q)
	}
	if q := byID["acme/r1"]; q.PipelineSuccessRate == nil || *q.PipelineSuccessRate != 0 || q.TestPassRate != nil {
		t.Errorf("present 0 must stay 0, JSON null must be absent: %#v", q)
	}
	if q := byID["acme/r2"]; q.PipelineSuccessRate == nil || *q.PipelineSuccessRate != 0.7 || q.TestPassRate == nil || *q.TestPassRate != 0.6 {
		t.Errorf("present factors unchanged: %#v", q)
	}
}

// A stability row on the range's last day is included.
func TestRealClickHouse_TestopsRiskRangeEndIsInclusiveForEveryTable(t *testing.T) {
	ctx, conn, client := startStore(t)
	stab(t, ctx, conn, "org-1", rA, "2026-01-31", 0.25, "2026-01-31 02:00:00")
	drag(t, ctx, conn, "org-1", rA, "2026-01-31", 4, 1, 1, 1, 1, "2026-01-31 02:00:00")
	conf(t, ctx, conn, "org-1", rA, "2026-01-31", 0.5, "{}", "2026-01-31 02:00:00")
	got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-31")})
	if err != nil {
		t.Fatal(err)
	}
	if got.PipelineStability == nil || *got.PipelineStability != 0.25 || got.QualityDragHours == nil || *got.QualityDragHours != 4 || got.ReleaseConfidence == nil || *got.ReleaseConfidence != 0.5 {
		t.Fatalf("%#v %v %v", got.PipelineStability, got.QualityDragHours, got.ReleaseConfidence)
	}
}
