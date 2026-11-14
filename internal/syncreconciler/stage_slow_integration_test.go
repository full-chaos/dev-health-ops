//go:build integration

package syncreconciler

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestStageSlowNamesTheStatementOfASlowSuccessfulPass (CHAOS-13743 T1, S7): a
// real statement at >= 75% of a 5 s budget succeeds and stage_slow names it.
// pg_sleep(3.8) can only be longer than 3.75 s; the test fails only if the host
// adds more than 1.2 s on top of it.
func TestStageSlowNamesTheStatementOfASlowSuccessfulPass(t *testing.T) {
	pool := startPhasePool(t, 2)
	pipeline := newStageBehaviorPipeline(t, pipelineConfigWithBudget(StageMaterializer, 5*time.Second), stageBehaviors{
		StageMaterializer: func(stageCtx context.Context) error {
			_, err := pool.Exec(stageCtx, "-- reconciler slow success probe\nSELECT pg_sleep(3.8)")
			return err
		},
	})
	captured, restore := captureSlogRecords(t)
	defer restore()
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	if _, found := findSlogRecord(*captured, "syncreconciler.stage_failed"); found {
		t.Fatal("the pass failed; the host added more than 1.2 s to a 3.8 s sleep")
	}
	record, found := findSlogRecord(*captured, "syncreconciler.stage_slow")
	if !found {
		t.Fatal("no syncreconciler.stage_slow record for a pass at >= 75% of its budget")
	}
	if record["phase"] != "statement" || record["phase_name"] != "reconciler slow success probe" {
		t.Fatalf("stage_slow phase = %v/%v, want statement/reconciler slow success probe; record %v", record["phase"], record["phase_name"], record)
	}
	if phases, _ := record["phases"].(string); !strings.Contains(phases, ";statement:reconciler slow success probe=") {
		t.Fatalf("phases = %q, want the slow statement listed", phases)
	}
}
