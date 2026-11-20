package reconcilerservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
)

type envelopeStepper struct{}

func (envelopeStepper) Step(context.Context, time.Time, int) (syncreconciler.Observation, error) {
	return syncreconciler.Observation{}, nil
}

// CHAOS-14386: the composed outer envelope (dependencies.go:968, the stage
// budget sum plus stageBudgetOuterEnvelopeMargin, dependencies.go:50) must
// still be a loop config the sync loop accepts; its ObservationTimeout bound
// is maxObservationTimeout (syncreconciler/loop.go:24, checked at :120).
func TestComposedStageEnvelopeFitsTheSyncLoopBound(t *testing.T) {
	config := syncreconciler.DefaultLoopConfig(health.NewRegistry(readinessTestCheckTimeout))
	config.ObservationTimeout = syncreconciler.DefaultStageBudgets().Sum() + stageBudgetOuterEnvelopeMargin
	if _, err := syncreconciler.NewLoop(envelopeStepper{}, config); err != nil {
		t.Fatalf("composed envelope %s refused by the sync loop: %v", config.ObservationTimeout, err)
	}
}
