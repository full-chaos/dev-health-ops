package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// Webhook scoped-sync requests (CHAOS-6695, decision D2487, option C).
//
// A webhook delivery that routes to a sync configuration is recorded by the
// webhook worker, on the domain role, as one webhook_sync_requests row. The
// domain role cannot write the scheduling tables (Option B), so the scheduler
// claims the row here, on the coordinator role, and mints the occurrence and
// its manual trigger through synchandoff.Mint -- the one minting seam the
// admin sync/backfill routes and `dho backfill run` also use -- deleting the
// request in the same transaction. The existing occurrence reconciler then
// materializes the run in the same scheduler window.
//
// Partial-write limit (R436): the request and its occurrence are two
// transactions on two roles. A request can exist without an occurrence until
// this minter mints it or refuses it; an occurrence never exists without its
// request having been marked minted in the same transaction. A minted request
// is KEPT for webhookRequestMintedRetention (and then pruned): its row is what
// makes a retried delivery a no-op even when the delivery would now route to a
// different configuration, and it survives its configuration being deleted (no
// foreign key), so a request is never dropped without a recorded outcome.
const (
	// webhookRequestMaxAge bounds how old a request, and the delivery it
	// stands for, may be when it is minted. An older one is refused with a
	// reason rather than starting a sync for a delivery that stale: the
	// configuration's own schedule has had ample time to cover it. It is also
	// the acceptance window that webhookRequestMintedRetention must exceed.
	webhookRequestMaxAge = 24 * time.Hour
	// webhookRequestMaxAttempts bounds retries of a failing mint; the row is
	// then refused with the last stage-named error kept.
	webhookRequestMaxAttempts = 10
	// webhookRequestMintedRetention is how long a minted request is kept for
	// delivery-level idempotency before the scheduler prunes it.
	webhookRequestMintedRetention = 7 * 24 * time.Hour
	// webhookRequestMaxInstantBumps bounds how many microseconds a request's
	// instant moves when another delivery already holds (configuration,
	// instant): distinct deliveries each get their own occurrence.
	webhookRequestMaxInstantBumps = 8
	webhookRequestBaseBackoff     = 30 * time.Second
	webhookRequestMaxBackoff      = 30 * time.Minute
	// webhookRequestTriggeredBy is the manual trigger's triggered_by, as the
	// webhook path always wrote it (the sync_manual_triggers CHECK allows
	// only "manual" and "backfill").
	webhookRequestTriggeredBy = "manual"
)

type webhookRequest struct {
	deliveryID, orgID, syncConfigID, mode string
	sourceIDs                             []string
	scheduledFor, createdAt               time.Time
	attempts                              int
}

// webhookRequestMinter claims and mints pending webhook_sync_requests.
type webhookRequestMinter struct {
	pool   *pgxpool.Pool
	maxAge time.Duration

	minted, refused, failed, bumped, pruned atomic.Uint64
}

// mintPending claims up to limit pending requests, one transaction each, and
// mints or refuses them. A failure of one request is recorded on its own row
// and never stops the others; only an error that prevents claiming at all is
// returned.
func (minter *webhookRequestMinter) mintPending(ctx context.Context, now time.Time, limit int) error {
	for index := 0; index < limit; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		claimed, err := minter.mintOne(ctx, now)
		if err != nil {
			return err
		}
		if !claimed {
			break
		}
	}
	// Retention of the minted rows must never fail the window: it is logged
	// and retried next window.
	if err := minter.prune(ctx, now); err != nil {
		slog.ErrorContext(ctx, "sync.webhook_request.prune_failed", slog.String("error", safeErrorSummary(err)))
	}
	return nil
}

// mintOne claims one due request and settles it. It reports whether a
// request was claimed.
func (minter *webhookRequestMinter) mintOne(ctx context.Context, now time.Time) (bool, error) {
	tx, err := minter.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin webhook sync request claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var request webhookRequest
	err = tx.QueryRow(ctx, `
SELECT delivery_id::text, org_id, sync_config_id::text, mode, source_ids, scheduled_for, created_at, attempts
FROM public.webhook_sync_requests
WHERE refused_at IS NULL AND minted_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
ORDER BY created_at, delivery_id
LIMIT 1
FOR UPDATE SKIP LOCKED`, now).Scan(&request.deliveryID, &request.orgID, &request.syncConfigID, &request.mode,
		&request.sourceIDs, &request.scheduledFor, &request.createdAt, &request.attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim a webhook sync request: %w", err)
	}

	if now.Sub(request.createdAt) > minter.maxAge {
		return true, minter.refuse(ctx, tx, request, now,
			fmt.Sprintf("stale: older than %s", minter.maxAge))
	}
	// The idempotency window must exceed the acceptance window: a delivery
	// older than the age bound is refused here whether or not its request row
	// still exists, so a replay of a delivery whose minted row was pruned
	// (webhookRequestMintedRetention) can never mint a second occurrence.
	if now.Sub(request.scheduledFor) > minter.maxAge {
		return true, minter.refuse(ctx, tx, request, now,
			fmt.Sprintf("stale: delivery older than %s", minter.maxAge))
	}
	config, err := synchandoff.LoadConfig(ctx, tx, request.syncConfigID)
	if err != nil {
		return true, minter.fail(ctx, tx, request, now, "load_config", err)
	}
	if config == nil {
		return true, minter.refuse(ctx, tx, request, now, "sync configuration no longer exists")
	}
	if config.OrgID != request.orgID {
		return true, minter.refuse(ctx, tx, request, now, "sync configuration belongs to another organization")
	}

	if _, err := tx.Exec(ctx, `SAVEPOINT webhook_sync_request_mint`); err != nil {
		return true, fmt.Errorf("webhook sync request savepoint: %w", err)
	}
	scheduledFor := request.scheduledFor
	mode, datasetKeys := syncNowSelection(config, request.mode)
	var trigger synchandoff.Trigger
	var mintErr error
	for bump := 0; ; bump++ {
		trigger, mintErr = synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{
			Mode: mode, SourceIDs: request.sourceIDs, DatasetKeys: datasetKeys, TriggeredBy: webhookRequestTriggeredBy,
			ScheduledFor: &scheduledFor,
		}, now)
		if mintErr != nil || !trigger.Existing {
			break
		}
		// An occurrence for this (configuration, instant) already exists and
		// this request is not the one that made it (a request mints once, in one
		// transaction, and a retried delivery is stopped by its own row): a
		// DIFFERENT delivery landed on the same microsecond. It gets its own
		// occurrence one microsecond later rather than being merged into the
		// first one, whose sources it may not cover.
		if bump >= webhookRequestMaxInstantBumps {
			mintErr = instantTakenError{}
			break
		}
		scheduledFor = scheduledFor.Add(time.Microsecond)
		minter.bumped.Add(1)
	}
	if mintErr != nil {
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT webhook_sync_request_mint`); err != nil {
			return true, fmt.Errorf("webhook sync request rollback: %w", err)
		}
		return true, minter.fail(ctx, tx, request, now, "mint", mintErr)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.webhook_sync_requests SET minted_at = $2, occurrence_id = $3 WHERE delivery_id = $1::uuid`,
		request.deliveryID, now, trigger.OccurrenceID); err != nil {
		return true, fmt.Errorf("mark a webhook sync request minted: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit a minted webhook sync request: %w", err)
	}
	minter.minted.Add(1)
	slog.InfoContext(ctx, "sync.webhook_request.minted",
		slog.String("delivery_id", request.deliveryID), slog.String("org_id", request.orgID),
		slog.String("sync_config_id", request.syncConfigID), slog.String("occurrence_id", trigger.OccurrenceID),
		slog.String("mode", mode), slog.Int("source_count", len(request.sourceIDs)), slog.Int("dataset_key_count", len(datasetKeys)),
		slog.Time("scheduled_for", scheduledFor), slog.Bool("instant_bumped", !scheduledFor.Equal(request.scheduledFor)))
	return true, nil
}

// instantTakenError: every microsecond the request could move to already holds
// an occurrence of the configuration.
type instantTakenError struct{}

func (instantTakenError) Error() string { return "the occurrence instants are all taken" }

// prune removes minted requests past their retention.
func (minter *webhookRequestMinter) prune(ctx context.Context, now time.Time) error {
	tag, err := minter.pool.Exec(ctx, `
DELETE FROM public.webhook_sync_requests WHERE minted_at IS NOT NULL AND minted_at < $1`, now.Add(-webhookRequestMintedRetention))
	if err != nil {
		return fmt.Errorf("prune minted webhook sync requests: %w", err)
	}
	minter.pruned.Add(uint64(tag.RowsAffected()))
	return nil
}

// syncNowSelection is the mode and dataset part of sync/trigger_routing.py's
// plan_request_for_config, the selection Python's "Sync Now" producer
// (create_sync_execution_trigger) writes for the configuration, read at mint
// time: an incremental request is promoted to full_resync when the
// configuration's sync_options carry the legacy full_resync flag, and a child
// configuration (pinned to one source) names the datasets its sync_targets
// map to (none when they map to nothing: every enabled dataset). The sources
// stay the request's: the delivery's own source.
func syncNowSelection(config *synchandoff.Config, mode string) (string, []string) {
	if mode == "incremental" && synchandoff.Truthy(config.SyncOptions["full_resync"]) {
		mode = "full_resync"
	}
	if config.SourceID == nil {
		return mode, nil
	}
	keys := providersync.DatasetKeysForTargets(config.Provider, config.SyncTargets)
	if len(keys) == 0 {
		return mode, nil
	}
	return mode, keys
}

// refuse settles a request for good, keeping its row and the reason.
func (minter *webhookRequestMinter) refuse(ctx context.Context, tx pgx.Tx, request webhookRequest, now time.Time, reason string) error {
	if _, err := tx.Exec(ctx, `
UPDATE public.webhook_sync_requests SET refused_at = $2, refused_reason = $3
WHERE delivery_id = $1::uuid`, request.deliveryID, now, reason); err != nil {
		return fmt.Errorf("refuse a webhook sync request: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit a refused webhook sync request: %w", err)
	}
	minter.refused.Add(1)
	slog.ErrorContext(ctx, "sync.webhook_request.refused",
		slog.String("delivery_id", request.deliveryID), slog.String("org_id", request.orgID),
		slog.String("sync_config_id", request.syncConfigID), slog.String("reason", reason))
	return nil
}

// fail records a failed attempt with backoff, or refuses the request once
// its attempts are spent. The recorded error names the stage and, for a
// PostgreSQL error, its SQLSTATE only: an error message can carry values.
func (minter *webhookRequestMinter) fail(ctx context.Context, tx pgx.Tx, request webhookRequest, now time.Time, stage string, cause error) error {
	recorded := stage + ": " + safeErrorSummary(cause)
	attempts := request.attempts + 1
	if attempts >= webhookRequestMaxAttempts {
		minter.failed.Add(1)
		return minter.refuse(ctx, tx, request, now,
			fmt.Sprintf("attempts exhausted after %d; last error %s", attempts, recorded))
	}
	next := now.Add(webhookRequestBackoff(attempts))
	if _, err := tx.Exec(ctx, `
UPDATE public.webhook_sync_requests SET attempts = $2, next_attempt_at = $3, last_error = $4
WHERE delivery_id = $1::uuid`, request.deliveryID, attempts, next, recorded); err != nil {
		return fmt.Errorf("record a failed webhook sync request: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit a failed webhook sync request: %w", err)
	}
	minter.failed.Add(1)
	slog.ErrorContext(ctx, "sync.webhook_request.mint_failed",
		slog.String("delivery_id", request.deliveryID), slog.String("org_id", request.orgID),
		slog.String("sync_config_id", request.syncConfigID), slog.String("stage", stage),
		slog.String("error", recorded), slog.Int("attempts", attempts), slog.Time("next_attempt_at", next))
	return nil
}

// webhookRequestBackoff doubles from the base per attempt, capped.
func webhookRequestBackoff(attempts int) time.Duration {
	delay := webhookRequestBaseBackoff
	for step := 1; step < attempts && delay < webhookRequestMaxBackoff; step++ {
		delay *= 2
	}
	if delay > webhookRequestMaxBackoff {
		delay = webhookRequestMaxBackoff
	}
	return delay
}

// safeErrorSummary is an error's SQLSTATE when it is a PostgreSQL error, and
// its type otherwise: never the message, which can carry row values.
func safeErrorSummary(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "sqlstate " + pgErr.Code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "context: " + err.Error()
	}
	return strings.TrimPrefix(fmt.Sprintf("%T", err), "*")
}

// writePrometheus reports the minter's counters.
func (minter *webhookRequestMinter) writePrometheus(output io.Writer) error {
	_, err := fmt.Fprintf(output,
		"# HELP devhealth_scheduler_webhook_sync_requests_total Webhook scoped-sync requests the scheduler settled, by outcome (CHAOS-6695).\n"+
			"# TYPE devhealth_scheduler_webhook_sync_requests_total counter\n"+
			"devhealth_scheduler_webhook_sync_requests_total{outcome=\"minted\"} %d\n"+
			"devhealth_scheduler_webhook_sync_requests_total{outcome=\"failed_attempt\"} %d\n"+
			"devhealth_scheduler_webhook_sync_requests_total{outcome=\"refused\"} %d\n"+
			"devhealth_scheduler_webhook_sync_requests_total{outcome=\"instant_bumped\"} %d\n"+
			"devhealth_scheduler_webhook_sync_requests_total{outcome=\"pruned\"} %d\n",
		minter.minted.Load(), minter.failed.Load(), minter.refused.Load(), minter.bumped.Load(), minter.pruned.Load())
	return err
}

// MintWebhookSyncRequests settles up to limit pending webhook_sync_requests
// on pool (the coordinator role) exactly as the scheduler's window does. It
// is the entry point tests and tooling use to drive the real minter.
func MintWebhookSyncRequests(ctx context.Context, pool *pgxpool.Pool, now time.Time, limit int) error {
	if pool == nil {
		return ErrOccurrenceReconcilerUnavailable
	}
	minter := &webhookRequestMinter{pool: pool, maxAge: webhookRequestMaxAge}
	return minter.mintPending(ctx, now.UTC(), limit)
}
