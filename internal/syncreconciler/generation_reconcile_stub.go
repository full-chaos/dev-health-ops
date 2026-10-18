package syncreconciler

import (
	"context"
	"time"
)

type StrandedKindCount struct {
	Kind     string
	Stranded int
}

type GenerationReconcileReport struct {
	Kind             string
	Rearmed          int
	SkippedNotReady  int
	SkippedLiveJob   int
	SkippedLockedJob int
	OtherKinds       []StrandedKindCount
}

// RED proof only: no reconcile exists on main.
func (repair *TerminalDeliveryRepair) ReconcileOtherGenerations(context.Context, time.Time, string) (GenerationReconcileReport, error) {
	return GenerationReconcileReport{}, nil
}
