package syncreconciler

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func phasePipeline(t *testing.T, materializer func(context.Context) error) *MutationPipeline {
	t.Helper()
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(ctx context.Context, _ time.Time, _ time.Time, _ int) (MaterializerResult, error) {
			return MaterializerResult{}, materializer(ctx)
		}),
		pipelineKernelFunc(func(context.Context, time.Time, int, time.Duration, AtLeastOncePublisher, PostSyncHandoff) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) { return "", nil }),
		PostSyncHandoff(func(context.Context, TransportClaim) error { return nil }),
		nil,
		noopTerminalOutboxClose(),
		noopOrphanedUnitRepair(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline
}

// The acquire-wait histogram is registered at zero for EVERY stage, with the
// stage as its only label: "no waiting" is a series, and the vocabulary is
// closed (no pool, statement or tenant label can appear).
func TestAcquireWaitHistogramIsRegisteredAtZeroForEveryStageWithOnlyAStageLabel(t *testing.T) {
	pipeline := phasePipeline(t, func(context.Context) error { return nil })
	var metrics strings.Builder
	if err := pipeline.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	const name = "dev_health_reconciler_stage_acquire_wait_seconds"
	if !strings.Contains(metrics.String(), "# TYPE "+name+" histogram") {
		t.Fatalf("%s is not declared as a histogram", name)
	}
	stageOnly := regexp.MustCompile(`^` + name + `_(bucket|sum|count)\{stage="[a-z_]+"(,le="[^"]+")?\} `)
	seen := map[string]bool{}
	for _, line := range strings.Split(metrics.String(), "\n") {
		if !strings.HasPrefix(line, name+"_") {
			continue
		}
		if !stageOnly.MatchString(line) {
			t.Fatalf("series %q carries a label beyond the stage (and le)", line)
		}
		if strings.Contains(line, "_count{") {
			if !strings.HasSuffix(line, " 0") {
				t.Fatalf("fresh pipeline count is not zero: %q", line)
			}
			seen[strings.SplitN(strings.SplitN(line, `stage="`, 2)[1], `"`, 2)[0]] = true
		}
	}
	for _, stage := range orderedStages {
		if !seen[string(stage)] {
			t.Errorf("stage %q has no zero-registered acquire-wait series", stage)
		}
	}
}

// A stage that fails before it touches the database says so ("none"), rather
// than logging nothing, so an absent phase never reads as "unknown".
func TestStageFailedSaysNoneWhenTheStageNeverTouchedTheDatabase(t *testing.T) {
	pipeline := phasePipeline(t, func(context.Context) error { return errors.New("boom") })
	captured, restore := captureSlogRecords(t)
	defer restore()
	if _, err := pipeline.Step(context.Background(), time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	record, found := findSlogRecord(*captured, "syncreconciler.stage_failed")
	if !found {
		t.Fatal("no stage_failed record")
	}
	if record["phase"] != "none" || record["phases"] != "" {
		t.Fatalf("stage_failed = %v, want phase=none and no phases", record)
	}
}
