package syncreconciler

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// CHAOS-13743 T1: syncreconciler.stage_slow on a SUCCESSFUL pass at >= 75% of
// the stage budget, at most one line per stage per 10 s, with count + max.

func sleepThenSucceed(d time.Duration) func(context.Context) error {
	return func(context.Context) error {
		time.Sleep(d)
		return nil
	}
}

func stageSlowRecords(records []map[string]any) []map[string]any {
	var slow []map[string]any
	for _, record := range records {
		if record["msg"] == "syncreconciler.stage_slow" {
			slow = append(slow, record)
		}
	}
	return slow
}

// runStepsCapturingLogs runs one Step per entry of gaps (sleeping that long of
// virtual time before it) and returns every log record.
func runStepsCapturingLogs(t *testing.T, config MutationPipelineConfig, behaviors stageBehaviors, gaps []time.Duration) []map[string]any {
	t.Helper()
	var records []map[string]any
	synctest.Test(t, func(t *testing.T) {
		captured, restore := captureSlogRecords(t)
		defer restore()
		pipeline := newStageBehaviorPipeline(t, config, behaviors)
		for _, gap := range gaps {
			time.Sleep(gap)
			_, _ = pipeline.Step(context.Background(), time.Now().UTC(), 10)
		}
		records = *captured
	})
	return records
}

func TestStageSlowLogsASlowSuccess(t *testing.T) {
	records := runStepsCapturingLogs(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
		stageBehaviors{StageMaterializer: sleepThenSucceed(80 * time.Millisecond)}, []time.Duration{0})
	slow := stageSlowRecords(records)
	if len(slow) != 1 {
		t.Fatalf("stage_slow lines = %d, want 1: %v", len(slow), slow)
	}
	line := slow[0]
	if line["stage"] != "materializer" || line["budget_ms"] != int64(100) || line["elapsed_ms"] != int64(80) ||
		line["count"] != int64(1) || line["max_elapsed_ms"] != int64(80) {
		t.Fatalf("stage_slow = %v, want materializer budget 100 elapsed 80 count 1 max 80", line)
	}
	for _, key := range []string{"phases", "max_phases", "phase"} {
		if _, ok := line[key]; !ok {
			t.Fatalf("stage_slow has no %q attribute: %v", key, line)
		}
	}
	if _, found := findSlogRecord(records, "syncreconciler.stage_failed"); found {
		t.Fatal("a successful pass logged stage_failed")
	}
}

func TestStageSlowQuietBelowThreshold(t *testing.T) {
	records := runStepsCapturingLogs(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
		stageBehaviors{StageMaterializer: sleepThenSucceed(74 * time.Millisecond)}, []time.Duration{0})
	if slow := stageSlowRecords(records); len(slow) != 0 {
		t.Fatalf("74%% of the budget logged stage_slow: %v", slow)
	}
}

func TestStageSlowThresholdIsInclusive(t *testing.T) {
	records := runStepsCapturingLogs(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
		stageBehaviors{StageMaterializer: sleepThenSucceed(75 * time.Millisecond)}, []time.Duration{0})
	if slow := stageSlowRecords(records); len(slow) != 1 {
		t.Fatalf("exactly 75%% of the budget: stage_slow lines = %d, want 1", len(slow))
	}
}

func TestStageSlowNotOnAFailedPass(t *testing.T) {
	records := runStepsCapturingLogs(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
		stageBehaviors{StageMaterializer: func(context.Context) error {
			time.Sleep(80 * time.Millisecond)
			return ErrUnavailable
		}}, []time.Duration{0})
	if slow := stageSlowRecords(records); len(slow) != 0 {
		t.Fatalf("a failed pass logged stage_slow: %v", slow)
	}
	if _, found := findSlogRecord(records, "syncreconciler.stage_failed"); !found {
		t.Fatal("the failed pass logged no stage_failed")
	}
}

func TestStageSlowIsPerStage(t *testing.T) {
	config := DefaultMutationPipelineConfig()
	config.StageBudgets = DefaultStageBudgets()
	config.StageBudgets[StageMaterializer] = 100 * time.Millisecond
	records := runStepsCapturingLogs(t, config, stageBehaviors{
		StageMaterializer: sleepThenSucceed(80 * time.Millisecond),
		StageKernel:       sleepThenSucceed(800 * time.Millisecond),
	}, []time.Duration{0})
	slow := stageSlowRecords(records)
	if len(slow) != 2 || slow[0]["stage"] != "materializer" || slow[1]["stage"] != "kernel" {
		t.Fatalf("stage_slow lines = %v, want one for materializer then one for kernel", slow)
	}
	if slow[1]["budget_ms"] != int64(1000) || slow[1]["elapsed_ms"] != int64(800) || slow[1]["count"] != int64(1) {
		t.Fatalf("kernel stage_slow = %v, want budget 1000 elapsed 800 count 1", slow[1])
	}
}

func TestStageSlowNotOnParentCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		captured, restore := captureSlogRecords(t)
		defer restore()
		pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
			stageBehaviors{StageMaterializer: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(80 * time.Millisecond):
					return nil
				}
			}})
		parent, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = pipeline.Step(parent, time.Now().UTC(), 10)
		for _, record := range *captured {
			if record["msg"] == "syncreconciler.stage_slow" || record["msg"] == "syncreconciler.stage_failed" {
				t.Fatalf("a parent cancel logged %v", record)
			}
		}
	})
}

// TestStageSlowRateLimitsPerStageWithCountAndMax (T1b): one line per stage per
// 10 s; the next line counts every slow pass since the previous line and
// carries the slowest.
func TestStageSlowRateLimitsPerStageWithCountAndMax(t *testing.T) {
	elapsed := []time.Duration{80 * time.Millisecond, 90 * time.Millisecond, 85 * time.Millisecond, 76 * time.Millisecond}
	pass := 0
	records := runStepsCapturingLogs(t, pipelineConfigWithBudget(StageMaterializer, 100*time.Millisecond),
		stageBehaviors{StageMaterializer: func(context.Context) error {
			time.Sleep(elapsed[pass])
			pass++
			return nil
		}}, []time.Duration{0, time.Second, time.Second, 10 * time.Second})
	slow := stageSlowRecords(records)
	if len(slow) != 2 {
		t.Fatalf("stage_slow lines = %d, want 2 (first pass, then the first pass 10 s later): %v", len(slow), slow)
	}
	if slow[0]["count"] != int64(1) || slow[0]["elapsed_ms"] != int64(80) || slow[0]["max_elapsed_ms"] != int64(80) {
		t.Fatalf("first stage_slow = %v, want count 1 elapsed 80 max 80", slow[0])
	}
	if slow[1]["count"] != int64(3) || slow[1]["elapsed_ms"] != int64(76) || slow[1]["max_elapsed_ms"] != int64(90) {
		t.Fatalf("second stage_slow = %v, want count 3 elapsed 76 max 90", slow[1])
	}
}

// TestStageSlowZeroLinesWhenHealthy: passes under 75% never log, however many.
func TestStageSlowZeroLinesWhenHealthy(t *testing.T) {
	gaps := make([]time.Duration, 20)
	for index := range gaps {
		gaps[index] = time.Second
	}
	records := runStepsCapturingLogs(t, DefaultMutationPipelineConfig(), stageBehaviors{
		StageMaterializer: sleepThenSucceed(300 * time.Millisecond),
		StageKernel:       sleepThenSucceed(100 * time.Millisecond),
	}, gaps)
	if slow := stageSlowRecords(records); len(slow) != 0 {
		t.Fatalf("healthy passes logged %d stage_slow lines: %v", len(slow), slow)
	}
	if _, found := findSlogRecord(records, "syncreconciler.stage_failed"); found {
		t.Fatal("healthy passes logged stage_failed")
	}
}
