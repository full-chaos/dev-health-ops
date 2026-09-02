package workgraph

import (
	"context"
	"fmt"
)

// NativePostStep is native Go compute that runs INSIDE a work-graph build
// execution, AFTER the Python bridge has returned.
//
// # Why a second seam exists
//
// NativePreStep exists because the Python build CONSUMES what a ported producer
// writes. This one exists for the opposite reason: because the Python build
// OVERWRITES what a ported producer writes.
//
// `builder.build()` takes no arguments, has no stage selection, and the bridge
// calls it unconditionally (`work_graph_tasks.py:157`), so every stage runs on
// every build whether or not a native step already did that work. For a ported
// producer computing the SAME values that is harmless -- whichever write wins,
// the row is identical, which is why the issue-PR mapping runs as a pre-step.
//
// It is not harmless for a producer that deliberately computes a DIFFERENT
// value. Python's `_build_issue_issue_edges` writes `confidence=1.0`
// (`builder.py:905`) where this port writes variant-C's 0.9, and the two rows
// do not coexist -- which is the part worth stating as a MECHANISM rather than
// as "last writer wins":
//
//	ENGINE = ReplacingMergeTree(last_synced)
//	ORDER BY (org_id, source_type, source_id, edge_type, target_type, target_id)
//
// The dedup key does NOT include `confidence`. So the 0.9 row and the 1.0 row
// are THE SAME ROW to the engine and collapse by `last_synced` -- the
// divergence is erased precisely because the key excludes the column that
// diverges. Saying only "the later writer wins" leaves a reader wondering why
// two different confidences do not simply sit side by side. Counts reconcile, no error is raised, and the policy the port
// exists to apply is gone. Running that step AFTER the bridge makes the native
// writer the last writer, which is the only ordering under which its value
// survives while the Python stage still runs.
//
// # Failure semantics, and how they differ from a pre-step
//
// A pre-step failure fails the build because the Python build four lines later
// reads what the step writes, so continuing would emit a silently incomplete
// edge set.
//
// A post-step failure fails the build for a different reason: the bridge has
// already written, so the rows exist but carry PYTHON's values. That is not a
// missing answer, it is a wrong one that looks healthy -- exactly the state
// this seam was added to prevent -- so it must not be reported as success. The
// claim is released as AMBIGUOUS, as for pre-steps, because a step that writes
// in batches may have written some rows before the error.
//
// # This is a bridge-era seam, not a permanent one
//
// It exists only while `_build_issue_issue_edges` still runs. When that stage
// retires under the executor cutover, a step here should move to a pre-step or
// stop being a step at all -- being last writer is a property this arrangement
// depends on, not one worth preserving.
//
// Steps run in REGISTRATION ORDER, inside the same lease renewal as the bridge.
// A step must be idempotent: the request it serves can be retried.
type NativePostStep interface {
	// Name identifies the step in the ledger evidence and in errors. It must be
	// a stable, unique JSON object key, and must not collide with a pre-step's.
	Name() string
	// Run performs the step for one claimed request. The returned map is merged
	// into the request's ledger evidence under Name(); nil is allowed.
	Run(ctx context.Context, claim Claim) (map[string]any, error)
}

// runPostSteps runs each step in registration order, returning the evidence
// fragments to merge. It mirrors runPreSteps deliberately: two seams that
// behave differently under failure would be a trap for whoever adds the third
// step.
func runPostSteps(ctx context.Context, steps []NativePostStep, claim Claim) (map[string]map[string]any, error) {
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
			return nil, fmt.Errorf("work-graph post-step %s: %w", step.Name(), err)
		}
		if fragment != nil {
			fragments[step.Name()] = fragment
		}
	}
	return fragments, nil
}

// mergeStepFragments folds post-step fragments into the pre-step map so a
// single mergePreStepEvidence call carries both.
//
// A name collision is impossible by construction -- the composition root
// refuses a family whose constructed steps do not match the declared order, and
// the declared orders are disjoint -- but if one ever occurred the PRE-step
// fragment is kept, matching mergePreStepEvidence's own "never overwrite a key
// that is already present" rule.
func mergeStepFragments(pre, post map[string]map[string]any) map[string]map[string]any {
	if len(post) == 0 {
		return pre
	}
	if pre == nil {
		pre = make(map[string]map[string]any, len(post))
	}
	for name, fragment := range post {
		if _, taken := pre[name]; taken {
			continue
		}
		pre[name] = fragment
	}
	return pre
}
