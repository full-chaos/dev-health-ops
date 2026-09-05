// trigger_workgraph.go: `dev-health-workerctl workgraph trigger` (CHAOS-5172).
//
// `dev-hops work-graph build` (src/dev_health_ops/work_graph/runner.py:56
// run_work_graph_build) computes work graph edges entirely in Python,
// in-process -- a SEPARATE code path from the `workgraph.build` River kind
// the worker dispatches automatically (post-sync fanout,
// cmd/dev-health-worker/sync_dispatch.go:271-306; scheduled fanout,
// internal/scheduler/fixed/producers.go:862-901). This command gives the
// CLI a way to enqueue a FRESH `workgraph.build` request through the same
// `workgraph.RequestWriter.WriteTx` path those two producers use
// (internal/jobs/workgraph/publisher.go:32), instead of a second, unguarded
// Python compute that the automatic path knows nothing about.
//
// No existing operator surface does this: `workgraph repair`
// (repair_workgraph.go) only resolves an ALREADY-STUCK 'ambiguous' row: it
// never constructs a new request.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// rollbackTriggerTx is a plain best-effort rollback for a trigger command's
// own transaction -- deliberately separate from rollbackOperatorLock
// (main.go), which is named and scoped for the operator LOCK transaction
// specifically, not a generic helper to repurpose here.
//
// A rollback failure is logged (with the identifiers the caller supplies),
// never silently discarded (codex review, 2026-09-05, CHAOS-5170 r1 P2): a
// caller that swallows a failed rollback has no signal that the
// transaction's actual fate on the server is now UNKNOWN rather than
// "rolled back as requested" -- exactly the class of defect this trigger
// command's whole error-handling pass exists to close.
func rollbackTriggerTx(ctx context.Context, tx pgx.Tx, attrs ...slog.Attr) {
	if tx == nil {
		return
	}
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		args := make([]any, 0, len(attrs)+2)
		args = append(args, "error", err)
		for _, a := range attrs {
			args = append(args, a)
		}
		slog.Default().Error("workerctl manual trigger: rollback failed", args...)
	}
}

// manualWorkGraphTriggerNamespace is a UUIDv5 namespace distinct from
// postSyncFanoutNamespace (sync_dispatch.go) and the scheduled fanout's own
// occurrence-domain namespace (internal/scheduler/fixed/producers.go) --
// this command's requests must never collide with either automatic
// producer's deterministic IDs. Generated once, fixed forever (like
// postSyncFanoutNamespace itself).
var manualWorkGraphTriggerNamespace = uuid.MustParse("2b7f9e2e-6f3c-4b8a-9d1a-7a3d9c4e5f61")

// manualTriggerRequestID derives a deterministic request id from a
// generation string, exactly the way postSyncRequestID does for the
// automatic path (sync_dispatch.go) -- a retried CLI invocation with
// IDENTICAL flags must land on WriteTx's `ON CONFLICT (id) DO NOTHING` +
// sameRequest idempotency path, not create a second request.
func manualTriggerRequestID(namespace uuid.UUID, generation string) string {
	return uuid.NewSHA1(namespace, []byte(generation)).String()
}

// manualTriggerDateRangeScope builds the `{"from_date":...,"to_date":...}`
// scope JSON `postSyncWorkGraphScope` builds for KindBuild (RFC3339), or
// the date-only form it uses for KindMaterialize -- both fields are
// OPTIONAL (WriteTx's validRequest only requires len(Scope) > 1, so `{}`
// with neither date is legal, matching the scheduled fanout's own
// zero-window `{}` scope).
func manualTriggerDateRangeScope(from, to *time.Time, dateOnly bool) ([]byte, error) {
	scope := map[string]any{}
	layout := time.RFC3339
	if dateOnly {
		layout = "2006-01-02"
	}
	if from != nil {
		scope["from_date"] = from.UTC().Format(layout)
	}
	if to != nil {
		scope["to_date"] = to.UTC().Format(layout)
	}
	return json.Marshal(scope)
}

func dispatchWorkgraphTrigger(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("workgraph trigger")
	org := flags.String("org", "", "organization id (uuid)")
	from := flags.String("from", "", "optional window start (YYYY-MM-DD, UTC)")
	to := flags.String("to", "", "optional window end (YYYY-MM-DD, UTC)")
	reviewEvidence := flags.String(
		"review-evidence", "",
		`REQUIRED: why this build is being triggered manually (e.g. "CHAOS-5172 -- confirming a repo's edges after a late-arriving sync")`,
	)
	dryRun := flags.Bool("dry-run", false, "validate flags and print the request that WOULD be written, without writing it")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return writeError(stderr, "invalid_request")
	}
	canonicalOrg, err := canonicalUUID(*org)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}
	// canonicalUUID/uuid.Parse is permissive (any 32 hex digits in the right
	// shape); WriteTx's own validRequest requires RFC-4122 version/variant
	// bits (workgraph.ValidUUID). Rejecting the mismatch HERE, before a
	// transaction is ever opened, reports the same "invalid_request" every
	// other malformed flag gets, instead of discovering it deep inside
	// WriteTx as ErrInvalidState (codex review, 2026-09-05, CHAOS-5170 r1
	// P2) -- and this check runs before the dry-run branch below, so a
	// dry-run preview never reports success for an org WriteTx would refuse.
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
	// included -- the same gate AuthorizeProvidersyncCleanup/
	// AuthorizeSyncDispatchOutboxClose use for the same class of action: a
	// workers:read-only credential must never reach WriteTx (codex review,
	// 2026-09-05, CHAOS-5170 r1 P1 -- the first version of this command
	// authenticated the caller but never authorized the action at all).
	// Covering dry-run too matches this service's own established
	// convention: AuthorizeProvidersyncCleanup gates its own dry-run
	// preview identically, so this trigger does not introduce a NEW
	// asymmetry where a preview needs less privilege than the real write.
	if runtime.service == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	if err := runtime.service.AuthorizeWorkgraphTrigger(ctx, runtime.principal, canonicalOrg); err != nil {
		return writeServiceError(stderr, err)
	}

	// RFC3339, matching postSyncWorkGraphScope's KindBuild branch exactly --
	// the native prestep/poststep (internal/jobs/workgraph/prestep.go,
	// poststep.go) read this scope the same way regardless of producer.
	scope, err := manualTriggerDateRangeScope(fromTime, toTime, false)
	if err != nil {
		return writeError(stderr, "invalid_request")
	}

	// "manual-trigger:" -- deliberately its OWN namespace, distinct from
	// "post-sync:<sync_run_id>" and the scheduled fanout's
	// "<occurrence-key>" generations, so a manual build COEXISTS with
	// (never suppresses) either automatic producer's own request for the
	// same org/window, mirroring `metrics remaining trigger-backstop`'s
	// design (main.go's dispatchMetricsRemainingTriggerBackstop doc
	// comment): both may run, and the native compute is idempotent by
	// construction (it recomputes edges from source data), so a manual
	// trigger racing an automatic one is a cost (duplicate work), never a
	// correctness problem.
	generation := "manual-trigger:workgraph.build:" + canonicalOrg + ":" + *from + ":" + *to
	requestID := manualTriggerRequestID(manualWorkGraphTriggerNamespace, generation)

	request := workgraph.Request{
		ID:                   requestID,
		OrganizationID:       canonicalOrg,
		Kind:                 workgraph.KindBuild,
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
			"kind":            string(workgraph.KindBuild),
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
		// Begin's underlying error and the request's own identifiers are
		// logged here even though the caller-facing response stays the
		// bounded "operator_backend_unavailable" code (this binary's
		// established contract, see writeError's own doc comment on the
		// bounded reason-code vocabulary) -- codex review, 2026-09-05,
		// CHAOS-5170 r1 P2: the underlying cause was previously discarded
		// entirely, with no identifier trail to correlate it.
		slog.Default().LogAttrs(ctx, slog.LevelError,
			"workerctl manual workgraph trigger: begin failed",
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
			"workerctl manual workgraph trigger: commit failed",
			append(logAttrs, slog.Any("error", err))...)
		return writeError(stderr, "operator_backend_unavailable")
	}
	slog.Default().LogAttrs(ctx, slog.LevelInfo,
		"workerctl manual workgraph trigger: enqueued",
		append(logAttrs, slog.String("review_evidence", *reviewEvidence))...)
	return writeResult(stdout, stderr, map[string]any{
		"request_id":      requestID,
		"org":             canonicalOrg,
		"kind":            string(workgraph.KindBuild),
		"generation":      generation,
		"review_evidence": *reviewEvidence,
		"status":          "started",
	})
}

// parseOptionalUTCDateRange parses two optional YYYY-MM-DD flags. Either,
// both, or neither may be set -- WriteTx's validRequest accepts an empty
// `{}` scope (len > 1 is satisfied by the braces alone), matching the
// scheduled fanout's own zero-window trigger.
func parseOptionalUTCDateRange(from, to string) (*time.Time, *time.Time, error) {
	var fromTime, toTime *time.Time
	if strings.TrimSpace(from) != "" {
		parsed, err := parseUTCDate(from)
		if err != nil {
			return nil, nil, err
		}
		fromTime = &parsed
	}
	if strings.TrimSpace(to) != "" {
		parsed, err := parseUTCDate(to)
		if err != nil {
			return nil, nil, err
		}
		toTime = &parsed
	}
	return fromTime, toTime, nil
}

// parseUTCDate parses one "YYYY-MM-DD" flag and rejects year <= 0.
//
// Go's time.Parse happily accepts "0000-01-01" (year zero is a real,
// representable time.Time in Go's proleptic Gregorian calendar) and
// produces no error -- this repository explicitly documents that exact
// behavior at cmd/dev-health-worker/workgraph_issue_pr_links.go:339 as a
// known Go/real-calendar mismatch. manualTriggerDateRangeScope only checks
// JSON syntax, not date sanity, so a year-zero (or negative-year) scope was
// silently WRITTEN, then only caught later, ambiguously, by the build
// pre-step (internal/jobs/workgraph/handler.go) releasing the request as
// "ambiguous" -- indistinguishable from a dozen other reasons a request can
// be released. Rejecting it here reports the same "invalid_request" every
// other malformed flag gets, before a transaction is ever opened (codex
// review, 2026-09-05, CHAOS-5170 r1 P2).
func parseUTCDate(value string) (time.Time, error) {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return time.Time{}, err
	}
	if parsed.Year() <= 0 {
		return time.Time{}, fmt.Errorf("date %q has a non-positive year, which is not a real calendar date", value)
	}
	return parsed.UTC(), nil
}

// writeResultOrError reports a WriteTx failure as a structured result
// (never a bare exit code) so an operator can see the request id and
// generation it attempted even when the write failed -- the same shape
// `dispatchMetricsRemainingTriggerBackstop` uses for its own error states.
func writeResultOrError(stdout, stderr io.Writer, status string, err error, extra map[string]any) int {
	result := map[string]any{"status": status, "error": err.Error()}
	for k, v := range extra {
		result[k] = v
	}
	writeResult(stdout, stderr, result)
	return 1
}
