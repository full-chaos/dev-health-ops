package workgraph

import (
	"context"
	"encoding/json"
	"fmt"
)

// NativePreStep is native Go compute that runs INSIDE a work-graph build
// execution, before the request reaches the Python bridge.
//
// # Why this seam exists
//
// `workgraph.build` is a bridge kind: Go owns the request/claim/lease/ledger
// state machine and Python owns 100% of the compute. Porting that compute
// happens one producer at a time (CHAOS-4441), and each ported producer needs
// to run BEFORE the Python build, because the Python build consumes what it
// writes -- the issue-PR mapping is read four lines later by
// `_build_issue_pr_edges_from_fast_path` (builder.py:1814).
//
// Running the ported producer here rather than as its own River kind is what
// makes that ordering a property of the CODE instead of a property of two
// producers' scheduling: it holds on both paths that start a build (the
// post-sync fanout at `native_post_sync.go:221` and the fixed-schedule
// producer at `scheduler/fixed/producers.go:826`) with no new wiring in either.
// It is also the narrowing the migration plans sanction --
// `go-worker-cutover-implementation-plan.md:457-459` (CUT-13, "port, or
// temporarily isolate behind fixed Go-owned compatibility adapters") and
// `go-worker-migration-implementation-plan.md:883` ("the temporary Python
// boundary is explicit and NOT a generic task executor") -- because each step
// moved here makes the bridge strictly narrower.
//
// # Failure semantics
//
// A pre-step failure fails the whole build, deliberately. The Python build
// four lines later reads what the step writes, so a build that continued past
// a failed mapping step would emit an edge set silently missing those links --
// which is a wrong answer, not a degraded one. The claim is released as
// AMBIGUOUS rather than failed, because a step that writes in batches may have
// written some rows before the error.
//
// Steps run in registration order, before the bridge, inside the same lease
// renewal. A step must be idempotent: the request it serves can be retried.
type NativePreStep interface {
	// Name identifies the step in the ledger evidence and in errors. It must be
	// a stable, unique JSON object key.
	Name() string
	// Run performs the step for one claimed request. The returned map is merged
	// into the request's ledger evidence under Name(); nil is allowed.
	Run(ctx context.Context, claim Claim) (map[string]any, error)
}

// mergePreStepEvidence folds each step's evidence fragment into the evidence
// the compatibility executor returned, under that step's name.
//
// It is deliberately conservative: the Python payload is authoritative and is
// returned UNCHANGED unless it is a JSON object that can be re-encoded. A
// ledger row is durable execution evidence, and corrupting it to attach a
// telemetry nicety would be a bad trade -- the same counts are already on the
// step's own structured log line and its observer.
func mergePreStepEvidence(evidence []byte, fragments map[string]map[string]any) []byte {
	if len(fragments) == 0 || len(evidence) == 0 {
		return evidence
	}
	var payload map[string]any
	if err := json.Unmarshal(evidence, &payload); err != nil || payload == nil {
		// Not a JSON object (or not decodable): leave it exactly as Python
		// produced it.
		return evidence
	}
	for name, fragment := range fragments {
		if len(fragment) == 0 {
			continue
		}
		// Never overwrite a key the executor already produced.
		if _, taken := payload[name]; taken {
			continue
		}
		payload[name] = fragment
	}
	merged, err := json.Marshal(payload)
	if err != nil {
		return evidence
	}
	return merged
}

// runPreSteps executes every registered step in order, collecting evidence.
func runPreSteps(ctx context.Context, steps []NativePreStep, claim Claim) (map[string]map[string]any, error) {
	if len(steps) == 0 {
		return nil, nil
	}
	fragments := make(map[string]map[string]any, len(steps))
	for _, step := range steps {
		if step == nil {
			continue
		}
		fragment, err := step.Run(ctx, claim)
		if err != nil {
			return nil, fmt.Errorf("work-graph pre-step %s: %w", step.Name(), err)
		}
		if fragment != nil {
			fragments[step.Name()] = fragment
		}
	}
	return fragments, nil
}
