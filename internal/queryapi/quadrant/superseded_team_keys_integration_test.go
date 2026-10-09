//go:build integration

package quadrant

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The quadrant WIP reads against a day that was computed again after a team id
// changed, on the schema of the migration chain.
//
// work_item_metrics_daily then holds, for the work scope of one repository and
// one day: the row of the team under its new id; under the old id the row of
// the first compute and, newer, the row of zeros that the recompute wrote over
// it; and the row of another team.
//
// A key whose newest row is a row of zeros holds no measure: the team scope
// must not plot its team, and the repository scope must not take it as a
// sample of the average WIP.
//
// The rows are stored by plain INSERT statements, so the same file runs on a
// tree without the writer rule and without the reader clause.
func TestQuadrantWIPReadsLeaveOutAKeyThatARecomputeSuperseded(t *testing.T) {
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

	const org = "quadrant-superseded-keys-it"
	for _, statement := range []string{
		`INSERT INTO repos (id, repo, provider, created_at, last_synced, org_id)
VALUES ('00000000-0000-4000-8000-0000000000a1', 'acme/api', 'github', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, computed_at, org_id)
VALUES ('2026-01-02', 'github', 'acme/api', 'platform', 'Platform', 1, 4, '2026-01-03 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily
    (day, provider, work_scope_id, team_id, team_name, items_started, wip_count_end_of_day, computed_at, org_id)
VALUES ('2026-01-02', 'github', 'acme/api', 'github:platform', 'Platform', 1, 4, '2026-01-09 06:00:00', '` + org + `'),
       ('2026-01-02', 'github', 'acme/api', 'github:apps', 'Apps', 1, 2, '2026-01-09 06:00:00', '` + org + `')`,
		`INSERT INTO work_item_metrics_daily (day, provider, work_scope_id, team_id, computed_at, org_id)
VALUES ('2026-01-02', 'github', 'acme/api', 'platform', '2026-01-09 06:00:00', '` + org + `')`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("exec %q: %v", statement, err)
		}
	}
	start, end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	points := func(rows []metricRow) map[string]float64 {
		out := map[string]float64{}
		for _, row := range rows {
			out[row.EntityID] += row.Value
		}
		return out
	}

	teams, err := fetchQuadrantMetric(ctx, client, TeamMetrics["wip"], start, end, "week", org, "")
	if err != nil {
		t.Fatalf("team wip: %v", err)
	}
	if got, want := points(teams), map[string]float64{"github:platform": 4, "github:apps": 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("team-scope WIP points = %v, want %v: the old id holds no measure for the day", got, want)
	}
	repos, err := fetchQuadrantMetric(ctx, client, RepoMetrics["wip"], start, end, "week", org, "")
	if err != nil {
		t.Fatalf("repository wip: %v", err)
	}
	if got, want := points(repos), map[string]float64{"acme/api": 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("repository-scope WIP points = %v, want %v: the average of the two keys that hold a measure", got, want)
	}
}
