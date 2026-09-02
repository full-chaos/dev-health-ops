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
// Steps run in REGISTRATION ORDER, before the bridge, inside the same lease
// renewal. A step must be idempotent: the request it serves can be retried.
//
// # Ordering invariant (binding on every future step)
//
// Order is load-bearing, because the Python build these steps run ahead of is
// itself ordered and its stages feed each other (`work_graph/builder.py`
// `build()`):
//
//	:464  _build_issue_issue_edges                  edges from work_item_dependencies
//	:466  _derive_issue_pr_links_from_dependencies  the MAPPING (ported: issueprlinks)
//	:470  _build_issue_pr_edges_from_fast_path      READS work_graph_issue_pr
//
// Note that the edge work STRADDLES the mapping: part of it precedes the
// mapping and part of it consumes the mapping. A single pre-step covering both
// halves cannot satisfy that, so a port of the edge producer has to register at
// least two steps, one on each side.
//
// The invariant, stated so it survives the specific ports:
//
//	ANY step that READS a table an earlier step WRITES must be registered
//	after it.
//
// Concretely today: anything reading `work_graph_issue_pr` registers after the
// mapping step. Getting this wrong does not fail — a fast-path edge builder
// running before the mapping simply reads the PREVIOUS run's rows and produces
// a plausible, stale edge set. `buildPreStepOrder` in the composition root
// pins the current order so that appending a step has to be a decision rather
// than an accident of registration.
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
//
// It decodes through decodeJSONPreservingNumbers rather than a plain
// json.Unmarshal into `any`, for the reason already documented above that
// helper: the default decoder turns every JSON number into a float64, which
// silently rounds integers above 2^53. Python's evidence carries
// `spend_limit_microunits`, a bigint the schema allows, so a plain decode would
// re-encode 9007199254740993 as 9007199254740992 -- still valid jsonb, and no
// longer what Python sent (codex round 1, F3).
func mergePreStepEvidence(evidence []byte, fragments map[string]map[string]any) []byte {
	if len(fragments) == 0 || len(evidence) == 0 {
		return evidence
	}
	decoded, err := decodeJSONPreservingNumbers(evidence)
	if err != nil {
		return evidence
	}
	payload, isObject := decoded.(map[string]any)
	if !isObject || payload == nil {
		// Not a JSON object: leave it exactly as Python produced it.
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
	merged, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
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
