//go:build integration

package testopsrisk

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// A repo-day whose newest row of a risk table retires the day (retracted = 1, the column defaults in every value) is
// absent from that table's aggregate, never 0: the release confidence mean, the quality-drag sum and its partial flag,
// the stability mean and the quadrant's latest day each read as if the repo-day had no row. A later computed row of
// the key makes the day live again.
func TestRealClickHouse_TestopsRiskRetiredDayIsAbsent(t *testing.T) {
	ctx, conn, client := startStore(t)
	const f1 = `{"pipeline_success_rate": 0.9, "test_pass_rate": 0.8}`
	retire := func(table, repo, day, computed string) {
		t.Helper()
		exec(t, ctx, conn, `INSERT INTO %s (repo_id, day, org_id, computed_at, retracted) SELECT '%s', '%s', 'org-1', '%s', 1`,
			table, repo, day, computed)
	}
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/web', 'org-1', now64(3), now64(3)`, rA)
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, org_id, created_at, last_synced) SELECT '%s', 'acme/api', 'org-1', now64(3), now64(3)`, rB)

	// Day 01: rA is computed, then retired in all three tables; rB stays.
	conf(t, ctx, conn, "org-1", rA, "2026-01-01", 0.8, f1, "2026-01-01 02:00:00")
	conf(t, ctx, conn, "org-1", rB, "2026-01-01", 0.6, f1, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-1", rA, "2026-01-01", 2, 1, 0.5, 0.25, 0.25, "2026-01-01 02:00:00")
	drag(t, ctx, conn, "org-1", rB, "2026-01-01", 3, 1, 1, 0.5, 0.5, "2026-01-01 02:00:00")
	stab(t, ctx, conn, "org-1", rA, "2026-01-01", 0.9, "2026-01-01 02:00:00")
	stab(t, ctx, conn, "org-1", rB, "2026-01-01", 0.5, "2026-01-01 02:00:00")
	retire("testops_release_confidence", rA, "2026-01-01", "2026-01-02 03:00:00")
	retire("testops_quality_drag", rA, "2026-01-01", "2026-01-02 03:00:00")
	retire("testops_pipeline_stability", rA, "2026-01-01", "2026-01-02 03:00:00")
	// Day 02: rB's only rows are retired: the day has no point in any series.
	conf(t, ctx, conn, "org-1", rB, "2026-01-02", 0.7, f1, "2026-01-02 02:00:00")
	drag(t, ctx, conn, "org-1", rB, "2026-01-02", 4, 1, 1, 1, 1, "2026-01-02 02:00:00")
	stab(t, ctx, conn, "org-1", rB, "2026-01-02", 0.4, "2026-01-02 02:00:00")
	retire("testops_release_confidence", rB, "2026-01-02", "2026-01-03 03:00:00")
	retire("testops_quality_drag", rB, "2026-01-02", "2026-01-03 03:00:00")
	retire("testops_pipeline_stability", rB, "2026-01-02", "2026-01-03 03:00:00")

	got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-02")})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("release_confidence", func(t *testing.T) {
		if len(got.Timeseries) != 1 || got.Timeseries[0].Date.String() != "2026-01-01" {
			t.Fatalf("timeseries %#v, want day 01 only", got.Timeseries)
		}
		if got.ReleaseConfidence == nil || *got.ReleaseConfidence < 0.5999 || *got.ReleaseConfidence > 0.6001 {
			t.Errorf("release confidence %s, want 0.6 (rB alone: the retired rA day is no sample)", deref(got.ReleaseConfidence))
		}
	})
	t.Run("quality_drag", func(t *testing.T) {
		if len(got.DragSpark) != 1 {
			t.Fatalf("drag spark %#v, want day 01 only", got.DragSpark)
		}
		if got.QualityDragHours == nil || *got.QualityDragHours != 3 {
			t.Errorf("drag %s, want 3 (rB alone)", deref(got.QualityDragHours))
		}
		if got.QualityDragPartial {
			t.Error("qualityDragPartial true: the NULL parts of a retired row are not a partial measurement")
		}
	})
	t.Run("pipeline_stability", func(t *testing.T) {
		if len(got.StabilitySpark) != 1 {
			t.Fatalf("stability spark %#v, want day 01 only", got.StabilitySpark)
		}
		if got.PipelineStability == nil || *got.PipelineStability != 0.5 {
			t.Errorf("stability %s, want 0.5 (rB alone)", deref(got.PipelineStability))
		}
	})
	t.Run("quadrant", func(t *testing.T) {
		// rA's latest live day is none (day 01 retired); rB's latest live day is day 01 (day 02 retired).
		if len(got.QuadrantData) != 1 || got.QuadrantData[0].ID != "acme/api" {
			t.Fatalf("quadrant %#v, want acme/api only", got.QuadrantData)
		}
		point := got.QuadrantData[0]
		if point.PipelineSuccessRate == nil || *point.PipelineSuccessRate != 0.9 || point.TestPassRate == nil || *point.TestPassRate != 0.8 {
			t.Errorf("acme/api rates %s / %s, want 0.9 / 0.8 from its live day 01", deref(point.PipelineSuccessRate), deref(point.TestPassRate))
		}
	})

	// The day comes back: a computed row newer than the retirement.
	conf(t, ctx, conn, "org-1", rB, "2026-01-02", 0.7, f1, "2026-01-04 02:00:00")
	drag(t, ctx, conn, "org-1", rB, "2026-01-02", 4, 1, 1, 1, 1, "2026-01-04 02:00:00")
	stab(t, ctx, conn, "org-1", rB, "2026-01-02", 0.4, "2026-01-04 02:00:00")
	again, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-01-01"), EndDate: date("2026-01-02")})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("reappears", func(t *testing.T) {
		if again.ReleaseConfidence == nil || *again.ReleaseConfidence < 0.6999 || *again.ReleaseConfidence > 0.7001 {
			t.Errorf("release confidence %s, want 0.7 (day 02 live again)", deref(again.ReleaseConfidence))
		}
		if again.QualityDragHours == nil || *again.QualityDragHours != 4 {
			t.Errorf("drag %s, want 4", deref(again.QualityDragHours))
		}
		if again.PipelineStability == nil || *again.PipelineStability != 0.4 {
			t.Errorf("stability %s, want 0.4", deref(again.PipelineStability))
		}
	})
}
