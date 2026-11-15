//go:build integration

package testopsrisk

import (
	"context"
	"math"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func pipelineDay(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repo, day string, success, failure int, computed string) {
	exec(t, ctx, conn, `INSERT INTO testops_pipeline_metrics_daily (repo_id, day, success_count, failure_count, org_id, computed_at) SELECT '%s', '%s', %d, %d, '%s', '%s'`, repo, day, success, failure, org, computed)
}

// A stability row written before the success rate counted completed runs only stays the newest row of its key when
// the new compute writes none for a day with no completed run. The read leaves it out: on day D repo A's old 0 is
// absent and the day is repo B's 0.9 alone (never the mean with 0); day E holds only repo A's old row and has no
// stability; a stability row with no pipeline row (repo C, day F) is kept.
func TestOldStabilityRowOfADayWithNoCompletedRunReadsAbsent(t *testing.T) {
	ctx, conn, client := startStore(t)
	const org = "org-13802"
	stab(t, ctx, conn, org, rA, "2026-11-01", 0, "2026-11-02 00:00:00")
	pipelineDay(t, ctx, conn, org, rA, "2026-11-01", 2, 2, "2026-11-02 00:00:00")
	pipelineDay(t, ctx, conn, org, rA, "2026-11-01", 0, 0, "2026-11-15 03:00:00")
	stab(t, ctx, conn, org, rB, "2026-11-01", 0.9, "2026-11-02 00:00:00")
	pipelineDay(t, ctx, conn, org, rB, "2026-11-01", 3, 1, "2026-11-02 00:00:00")
	stab(t, ctx, conn, org, rA, "2026-11-02", 0, "2026-11-03 00:00:00")
	pipelineDay(t, ctx, conn, org, rA, "2026-11-02", 0, 0, "2026-11-03 00:00:00")
	stab(t, ctx, conn, org, rC, "2026-11-03", 0.7, "2026-11-04 00:00:00")

	got, err := Resolve(ctx, client, org, model.TestOpsRiskInput{StartDate: date("2026-11-01"), EndDate: date("2026-11-03")})
	if err != nil {
		t.Fatal(err)
	}
	spark := map[string]float64{}
	for _, point := range got.StabilitySpark {
		spark[point.Ts.String()] = point.Value
	}
	t.Logf("stability spark: %v", spark)
	if len(spark) != 2 || math.Abs(spark["2026-11-01"]-90) > 1e-9 || math.Abs(spark["2026-11-03"]-70) > 1e-9 {
		t.Errorf("stability spark = %v, want 2026-11-01 = 90 (repo A's old 0 left out) and 2026-11-03 = 70 (no pipeline row: kept); 2026-11-02 absent", spark)
	}
	if got.PipelineStability == nil || math.Abs(*got.PipelineStability-0.7) > 1e-9 {
		t.Errorf("PipelineStability = %v, want 0.7", got.PipelineStability)
	}
}
