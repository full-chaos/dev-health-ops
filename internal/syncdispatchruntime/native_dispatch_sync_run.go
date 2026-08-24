package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// referenceDiscoverySucceeded ports reference_discovery.py's
// reference_discovery_succeeded verbatim: true iff a ledger row for this
// run exists with status=success. dispatch_sync_run gates ALL unit
// dispatch on this -- reference discovery must complete before any unit
// is claimed.
//
// Error classification: pgx.ErrNoRows never fires here (a plain SELECT
// with no row is a legitimate "not succeeded yet" false, not a query
// failure) -- any OTHER query error is a bare execution failure, retryable
// via ErrDiscoveryTransientFailure (round 3's sentinel, reused for the
// same reasoning: a Postgres blip mid-dispatch is exactly the class it
// exists for).
func referenceDiscoverySucceeded(ctx context.Context, tx pgx.Tx, runID string) (bool, error) {
	var id string
	err := tx.QueryRow(ctx, `
SELECT id::text FROM public.sync_run_reference_discoveries
WHERE sync_run_id = $1::uuid AND status = $2`, runID, discoveryStatusSuccess).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: check reference discovery success: %w", ErrDiscoveryTransientFailure, err)
	}
	return true, nil
}

// ensureReferenceDiscoveryWakeup ports ensure_reference_discovery_wakeup
// verbatim: lazily creates the reference-discovery ledger row if this run
// has never had one (reusing ensureReferenceDiscoveryLedger, already
// landed for family 2 -- "port once, share", not re-implemented here),
// then arms a reference_discovery outbox wakeup at the ledger's OWN
// available_at, not `now` -- a ledger already RETRYING with a future
// backoff must not have its wakeup pulled earlier just because
// dispatch_sync_run happened to run again first.
//
// Python falls back to `now` only if the ledger's available_at is NULL,
// which the schema (available_at timestamptz NOT NULL) makes unreachable
// in both languages -- kept for byte-for-byte parity with the Python
// source, not because it can fire.
//
// Error classification: both helpers already classify their own errors
// (ensureReferenceDiscoveryLedger: permanent ErrReferenceDiscoveryUnavailable;
// upsertDiscoveryOutboxWakeup: same); the new query this function adds
// (reading the ledger's available_at back) is a bare execution failure,
// retryable via ErrDiscoveryTransientFailure.
func ensureReferenceDiscoveryWakeup(ctx context.Context, tx pgx.Tx, orgID, runID string, now time.Time) error {
	ledgerID, err := ensureReferenceDiscoveryLedger(ctx, tx, orgID, runID, now)
	if err != nil {
		return err
	}
	var availableAt *time.Time
	if err := tx.QueryRow(ctx, `SELECT available_at FROM public.sync_run_reference_discoveries WHERE id = $1::uuid`, ledgerID).
		Scan(&availableAt); err != nil {
		return fmt.Errorf("%w: load reference discovery ledger available_at: %w", ErrDiscoveryTransientFailure, err)
	}
	if availableAt == nil {
		availableAt = &now
	}
	return upsertDiscoveryOutboxWakeup(ctx, tx, orgID, runID, outboxKindReferenceDiscovery, *availableAt)
}
