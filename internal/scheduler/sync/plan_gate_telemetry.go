package sync

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Plan-gate outcomes recorded by planGateTelemetry. "planned" is the
// canonical claim actually minted a unit; the other three are the CHAOS-4054/
// CHAOS-4060 gate refusing to plan one, split by which check failed.
const (
	planGateOutcomePlanned                  = "planned"
	planGateOutcomeUnknownPair              = "unknown_pair"
	planGateOutcomeRouteNotReady            = "route_not_ready_or_not_plannable"
	planGateOutcomeExecutedProofUnsatisfied = "executed_proof_unsatisfied"
)

// planGateKey identifies one (provider, dataset, outcome) series.
type planGateKey struct {
	Provider string
	Dataset  string
	Outcome  string
}

// planGateTelemetry backs sync_plan_gate_total{provider,dataset,outcome}.
// CHAOS-4731: the executed-proof gate (CHAOS-4060) silently vetoed every
// github/work-items planning attempt for over two months -- the route was
// RouteReady/Plannable the whole time, so nothing about the skip looked like
// a failure anywhere else in the system, and devhealth_scheduler_
// zero_planned_occurrences_total (materializer.go) is occurrence-scoped, not
// per-dataset, so it stayed flat as long as sibling datasets on the same
// occurrence kept planning fine. This is the missing per-pair signal: an
// operator can now alert on planGateOutcomeExecutedProofUnsatisfied /
// planGateOutcomeRouteNotReady climbing for a pair that should be planning,
// instead of discovering the gap two months later by reading ClickHouse row
// counts by hand.
//
// A package-level singleton, matching this file's existing warnedCapClamps /
// warnedEnvIntRejections shared-state pattern (planner.go) rather than
// threading a new field through PlannerInput/BuildScheduledPlan/
// BuildBackfillPlan -- both are free functions exercised directly by dozens
// of existing unit tests with fixed signatures, and this counter is a pure
// side observation of a decision already made, never an input to it.
type planGateTelemetry struct {
	mu     sync.Mutex
	counts map[planGateKey]uint64
}

var globalPlanGateTelemetry = &planGateTelemetry{counts: make(map[planGateKey]uint64)}

// observe is a no-op on a nil receiver so call sites never need a guard.
func (telemetry *planGateTelemetry) observe(provider, dataset, outcome string) {
	if telemetry == nil {
		return
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	if telemetry.counts == nil {
		telemetry.counts = make(map[planGateKey]uint64)
	}
	telemetry.counts[planGateKey{Provider: provider, Dataset: dataset, Outcome: outcome}]++
}

// resetForTest clears accumulated counts. Test-only: production never needs
// to reset a counter series, but package-level state otherwise leaks between
// table tests in the same process.
func (telemetry *planGateTelemetry) resetForTest() {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.counts = make(map[planGateKey]uint64)
}

// snapshotForTest returns the current count for one series without mutating
// anything, so a test can assert on it directly.
func (telemetry *planGateTelemetry) snapshotForTest(provider, dataset, outcome string) uint64 {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	return telemetry.counts[planGateKey{Provider: provider, Dataset: dataset, Outcome: outcome}]
}

// WritePrometheus satisfies the same health.MetricsSource shape
// NativeMaterializer.WritePrometheus and sourceDiscoveryTelemetry already
// implement, so it folds into the same scrape with no separate registration.
func (telemetry *planGateTelemetry) WritePrometheus(output io.Writer) error {
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	var text strings.Builder
	text.WriteString(
		"# HELP sync_plan_gate_total Scheduled/backfill planner decisions for " +
			"each (provider,dataset) canonical claim, by outcome (CHAOS-4731). " +
			"executed_proof_unsatisfied climbing with no matching planned growth " +
			"means a route has been attempted before but never once persisted a " +
			"row, and the CHAOS-4060 gate is now refusing to plan it again until " +
			"an ExecutedProofWaiver is recorded or a live run finally proves it.\n",
	)
	text.WriteString("# TYPE sync_plan_gate_total counter\n")
	keys := make([]planGateKey, 0, len(telemetry.counts))
	for key := range telemetry.counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Provider != keys[j].Provider {
			return keys[i].Provider < keys[j].Provider
		}
		if keys[i].Dataset != keys[j].Dataset {
			return keys[i].Dataset < keys[j].Dataset
		}
		return keys[i].Outcome < keys[j].Outcome
	})
	for _, key := range keys {
		fmt.Fprintf(&text, "sync_plan_gate_total{provider=%q,dataset=%q,outcome=%q} %d\n",
			key.Provider, key.Dataset, key.Outcome, telemetry.counts[key])
	}
	_, err := io.WriteString(output, text.String())
	return err
}
