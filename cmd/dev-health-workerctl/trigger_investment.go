// trigger_investment.go: `dev-health-workerctl investment trigger` (CHAOS-5173).
//
// `dev-hops investment materialize` (src/dev_health_ops/work_graph/runner.py:281,
// calling materialize_investments from work_graph/investment/materialize.py)
// computes entirely in Python, in-process -- a SEPARATE entry point from the
// `investment.materialize` River kind, which is NATIVE
// (internal/jobs/investment/nativeexecutor.go) and already reached
// automatically from post-sync (cmd/dev-health-worker/sync_dispatch.go:229-235,
// syncdispatchruntime's plan.Investment branch). The Python implementation is
// retained but unreachable from the worker path (CHAOS-4767 tracks its
// removal); the CLI just never enqueues through the native path either.
//
// This command enqueues a FRESH `investment.materialize` request through the
// same `workgraph.RequestWriter.WriteTx` the post-sync producer uses
// (internal/jobs/workgraph/publisher.go:32), so a manual trigger reaches the
// SAME native executor instead of running the retained Python path a second
// time, unguarded.
//
// DROPPED FLAGS, mirroring the CHAOS-5055 precedent (workerctl_dispatch.py's
// module doc: "per-run overrides that only ever made sense for a direct
// Python call ... have no equivalent on the worker's dispatch path and are
// no longer accepted here"). The current `dev-hops investment materialize`
// (runner.py:439-538) accepts many flags with NO Go-side equivalent, because
// `workgraph.Request`/`jobcontract.InvestmentMaterializePayload` carry only
// an org id and an optional date window -- everything else is resolved
// server-side by the native orchestrator (materialize.go). Dropped: --window-days
// (this command takes --from/--to directly, the CLI wrapper resolves any
// window-days convenience itself, matching how `metrics daily-start` resolves
// --day/--to rather than accepting a window count); --repo-id/--team-id (no
// per-component scoping field exists on the request); every LLM flag
// (--llm-provider/--model/--llm-api-key/--llm-base-url/--llm-concurrency/
// --llm-batch-*, all resolved server-side); --persist-evidence-snippets,
// --force, --allow-unscoped, --analytics-db/--db.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
)

// manualInvestmentTriggerNamespace is a UUIDv5 namespace distinct from
// postSyncFanoutNamespace (sync_dispatch.go) and manualWorkGraphTriggerNamespace
// above -- each producer (automatic post-sync, automatic scheduled fanout
// where applicable, and this manual command) must derive request ids from
// its own namespace so none can ever collide with another's.
var manualInvestmentTriggerNamespace = uuid.MustParse("6a1d3f47-8c2e-4a9b-b5f0-1e6d2c8a9b34")

// dispatchInvestment routes `dev-health-workerctl investment ...`. Only
// `trigger` exists today; unlike `workgraph`, there is no `investment
// repair`/`list-ambiguous` -- stuck investment.materialize requests are
// resolved through `workgraph repair` (repair_workgraph.go), since both
// kinds share the same work_graph_execution_requests/ledger tables.
func dispatchInvestment(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, "invalid_request")
	}
	switch args[0] {
	case "trigger":
		return dispatchInvestmentTrigger(ctx, runtime, args[1:], stdout, stderr)
	default:
		return writeError(stderr, "invalid_request")
	}
}

func dispatchInvestmentTrigger(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("investment trigger")
	org := flags.String("org", "", "organization id (uuid)")
	from := flags.String("from", "", "optional window start (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "optional window end (YYYY-MM-DD, UTC)")
	reviewEvidence := flags.String(
		"review-evidence", "",
		`REQUIRED: why this materialize is being triggered manually (e.g. "CHAOS-5173 -- confirming investment quotes after a late-arriving sync")`,
	)
	dryRun := flags.Bool("dry-run", false, "validate flags and print the request that WOULD be written, without writing it")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	canonicalOrg, err := canonicalUUID(*org)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	// See trigger_workgraph.go's identical check for the full rationale:
	// canonicalUUID/uuid.Parse is permissive; WriteTx's validRequest
	// requires RFC-4122 version/variant bits (codex review, 2026-09-05,
	// CHAOS-5170 r1 P2).
	if !workgraph.ValidUUID(canonicalOrg) {
		return writeError(stderr, "invalid_request")
	}
	if strings.TrimSpace(*reviewEvidence) == "" {
		return writeError(stderr, "invalid_request")
	}
	fromTime, toTime, err := parseOptionalUTCDateRange(*from, *to)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}

	// Authorized BEFORE anything backend-related is even attempted, dry-run
	// included -- see trigger_workgraph.go's identical check for the full
	// rationale (codex review, 2026-09-05, CHAOS-5170 r1 P1).
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	if err := runtime.service.AuthorizeInvestmentTrigger(ctx, runtime.principal, canonicalOrg); err != nil {
		return writeServiceError(stderr, err)
	}

	// Date-only (YYYY-MM-DD), matching postSyncWorkGraphScope's
	// KindMaterialize branch exactly -- the native orchestrator
	// (materialize.go) reads this scope the same way regardless of producer.
	scope, err := manualTriggerDateRangeScope(fromTime, toTime, true)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}

	// "manual-trigger:" namespace, deliberately distinct from "post-sync:
	// <sync_run_id>:investment" -- coexists with (never suppresses) an
	// automatic post-sync materialize for the same org/window. The native
	// executor's own WriteTx-level idempotency (publisher.go's ON CONFLICT
	// + sameRequest check) still protects a REPEATED manual invocation with
	// identical flags from creating a second request.
	generation := "manual-trigger:investment.materialize:" + canonicalOrg + ":" + *from + ":" + *to
	requestID := manualTriggerRequestID(manualInvestmentTriggerNamespace, generation)

	request := workgraph.Request{
		ID:                   requestID,
		OrganizationID:       canonicalOrg,
		Kind:                 workgraph.KindMaterialize,
		Scope:                scope,
		LLMConcurrency:       1,
		SpendLimitMicrounits: 0,
		CorrelationID:        generation,
		IdempotencyKey:       generation,
	}

	if *dryRun {
		return writeResult(stdout, stderr, map[string]any{
			"dry_run":         true,
			"request_id":      requestID,
			"org":             canonicalOrg,
			"kind":            string(workgraph.KindMaterialize),
			"scope":           json.RawMessage(scope),
			"generation":      generation,
			"review_evidence": *reviewEvidence,
		})
	}

	if runtime.pools == nil || runtime.registry == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	writer, err := workgraph.NewRequestWriter(runtime.registry)
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	logAttrs := []slog.Attr{
		slog.String("request_id", requestID),
		slog.String("org", canonicalOrg),
		slog.String("generation", generation),
	}
	tx, err := runtime.pools.Domain.Begin(ctx)
	if err != nil {
		// See trigger_workgraph.go's identical log call for the full
		// rationale (codex review, 2026-09-05, CHAOS-5170 r1 P2).
		slog.Default().LogAttrs(ctx, slog.LevelError,
			"workerctl manual investment trigger: begin failed",
			append(logAttrs, slog.Any("error", err))...)
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer rollbackTriggerTx(ctx, tx, logAttrs...)
	if err := writer.WriteTx(ctx, tx, request); err != nil {
		status := "error"
		if errors.Is(err, workgraph.ErrInvalidState) {
			status = "invalid_state"
		}
		return writeResultOrError(stdout, stderr, status, err, map[string]any{
			"request_id": requestID,
			"org":        canonicalOrg,
			"generation": generation,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Default().LogAttrs(ctx, slog.LevelError,
			"workerctl manual investment trigger: commit failed",
			append(logAttrs, slog.Any("error", err))...)
		return writeError(stderr, "operator_backend_unavailable")
	}
	slog.Default().LogAttrs(ctx, slog.LevelInfo,
		"workerctl manual investment trigger: enqueued",
		append(logAttrs, slog.String("review_evidence", *reviewEvidence))...)
	return writeResult(stdout, stderr, map[string]any{
		"request_id":      requestID,
		"org":             canonicalOrg,
		"kind":            string(workgraph.KindMaterialize),
		"generation":      generation,
		"review_evidence": *reviewEvidence,
		"status":          "started",
	})
}
