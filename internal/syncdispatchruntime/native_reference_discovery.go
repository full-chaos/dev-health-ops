package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrReferenceDiscoveryUnavailable = errors.New("native reference discovery is unavailable")

// Discovery ledger statuses mirror
// reference_discovery.py's DISCOVERY_STATUS_* constants exactly.
const (
	discoveryStatusPlanned  = "planned"
	discoveryStatusRunning  = "running"
	discoveryStatusRetrying = "retrying"
	discoveryStatusSuccess  = "success"
	discoveryStatusFailed   = "failed"

	outboxKindReferenceDiscovery    = "reference_discovery" // Python: OUTBOX_KIND_DISCOVERY
	referenceDiscoveryErrorCategory = "reference_discovery_failed"
	referenceDiscoveryErrorMessage  = "Reference discovery failed"
)

// DiscoveryExecutor performs the actual reference-discovery work for one
// sync run: loading its context (provider, credentials, scope), invoking the
// narrow run_team_autoimport_strict bridge, and verifying the ClickHouse
// readback -- Python's _load_discovery_context + run_team_autoimport_strict +
// _verify_reference_readback, called as one logical unit from
// run_sync_reference_discovery's point of view.
//
// Injected as an interface (CHAOS-4175) so the lease/claim/heartbeat/retry
// state machine in this file can be built and red-first tested independently
// of credential resolution, the ClickHouse client, and the populate bridge --
// each landing in its own later commit, per the family's agreed build order.
// A caller whose error implements retryAfterProvider gets that hint honored
// in the backoff calculation, matching Python's
// `getattr(exc, "retry_after_seconds", None)`.
type DiscoveryExecutor interface {
	Discover(ctx context.Context, runID string) (summary map[string]any, err error)
}

// retryAfterProvider is satisfied by a DiscoveryExecutor error that carries a
// provider-supplied retry-after hint.
type retryAfterProvider interface {
	RetryAfterSeconds() float64
}

type NativeReferenceDiscoveryService struct {
	pool     *pgxpool.Pool
	logger   *slog.Logger
	executor DiscoveryExecutor
	now      func() time.Time
}

func NewNativeReferenceDiscoveryService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	executor DiscoveryExecutor,
) (*NativeReferenceDiscoveryService, error) {
	if pool == nil || executor == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NativeReferenceDiscoveryService{pool: pool, logger: logger, executor: executor, now: time.Now}, nil
}

func (service *NativeReferenceDiscoveryService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// Discover is the native equivalent of bridge.Discover /
// run_sync_reference_discovery. Structurally different from Finalize: Python
// does NOT hold one database transaction across the whole call -- claiming
// the ledger commits before the (potentially slow, network-bound) discovery
// work starts, and stamping the result is a separate later transaction. This
// port preserves that split: holding a single pgx.Tx across a provider API
// call (inside executor.Discover) would pin a connection from the pool for
// the duration of an external call this code does not control the latency
// of.
func (service *NativeReferenceDiscoveryService) Discover(ctx context.Context, args ReferenceDiscoveryArgs) error {
	if service == nil || service.pool == nil || ctx == nil || args.valid() != nil {
		return ErrReferenceDiscoveryUnavailable
	}

	claimed, leaseOwner, deadline, err := service.claim(ctx, args)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	stopHeartbeat := service.startHeartbeat(args.SyncRunID(), leaseOwner, deadline)
	defer stopHeartbeat()

	summary, discoverErr := service.executor.Discover(ctx, args.SyncRunID())
	if discoverErr != nil {
		return service.handleFailure(ctx, args.SyncRunID(), leaseOwner, discoverErr)
	}
	return service.stampSuccess(ctx, args.OrganizationID(), args.SyncRunID(), leaseOwner, summary)
}

// claim mirrors run_sync_reference_discovery's first `with
// get_postgres_session_sync()` block: validate the transport reference is
// current, load the run, check (and possibly enforce) the canonical-incident
// feature gate, ensure a ledger row exists (planned, if this is the first
// attempt), and atomically claim it. Returns claimed=false for every Python
// "return {status: ...}" branch that isn't a hard failure (stale transport
// reference, missing run, feature-disabled denial, or a lost claim race) --
// all of those are legitimate no-ops for a River job, not errors.
func (service *NativeReferenceDiscoveryService) claim(
	ctx context.Context, args ReferenceDiscoveryArgs,
) (claimed bool, leaseOwner string, deadline time.Time, err error) {
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return false, "", time.Time{}, ErrReferenceDiscoveryUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	current, err := currentTransportReference(ctx, tx, args, outboxKindReferenceDiscovery)
	if err != nil {
		return false, "", time.Time{}, err
	}
	if !current {
		return false, "", time.Time{}, nil
	}

	run, err := loadFinalizeRun(ctx, tx, args.OrganizationID(), args.SyncRunID())
	if err != nil {
		return false, "", time.Time{}, err
	}
	if run == nil {
		// Python: `if run is None: return {"status": "skipped", "reason": "missing_run"}`.
		return false, "", time.Time{}, nil
	}

	// Python: `if sync_run_requires_canonical_incident_feature(session, run):
	// require_canonical_incident_feature_for_update_sync(session, run.org_id)`
	// -- the row-locking (FOR UPDATE) phase-B check, run BEFORE the ledger is
	// even created, so a denied run never gets claimed at all.
	requiresFeature, err := syncRunRequiresCanonicalIncidentFeature(ctx, tx, run.id, run.integrationID)
	if err != nil {
		return false, "", time.Time{}, err
	}
	if requiresFeature {
		gateNow := service.nowUTC()
		allowed, err := scheduledsync.CanonicalIncidentAllowedForUpdate(ctx, tx, run.orgID, gateNow)
		if err != nil {
			return false, "", time.Time{}, ErrReferenceDiscoveryUnavailable
		}
		if !allowed {
			if _, err := terminalizeFeatureDisabledPlan(ctx, tx, run, gateNow); err != nil {
				return false, "", time.Time{}, err
			}
			if err := tx.Commit(ctx); err != nil {
				return false, "", time.Time{}, ErrReferenceDiscoveryUnavailable
			}
			// Python: `return {"status": "feature_disabled", ...}` -- a
			// legitimate terminal outcome, not a claim.
			return false, "", time.Time{}, nil
		}
	}

	ledgerID, err := ensureReferenceDiscoveryLedger(ctx, tx, args.OrganizationID(), args.SyncRunID(), service.nowUTC())
	if err != nil {
		return false, "", time.Time{}, err
	}

	startedAt := service.nowUTC()
	deadline = startedAt.Add(maxDiscoveryLifetime())
	leaseExpiresAt := earlier(startedAt.Add(discoveryLeaseSeconds()), deadline)
	leaseOwner = uuid.New().String()

	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET status = $2, attempts = attempts + 1, lease_owner = $3, lease_expires_at = $4,
    last_heartbeat_at = $5, error = NULL, updated_at = $5
WHERE id = $1::uuid
  AND (
        status IN ($6, $7)
        OR (status = $2 AND lease_expires_at IS NOT NULL AND lease_expires_at <= $5)
      )
  AND available_at <= $5`,
		ledgerID, discoveryStatusRunning, leaseOwner, leaseExpiresAt, startedAt,
		discoveryStatusPlanned, discoveryStatusRetrying)
	if err != nil {
		return false, "", time.Time{}, ErrReferenceDiscoveryUnavailable
	}
	if tag.RowsAffected() == 0 {
		// Python: `return {"status": "skipped", "reason": "not_claimed"}`.
		return false, "", time.Time{}, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, "", time.Time{}, ErrReferenceDiscoveryUnavailable
	}
	return true, leaseOwner, deadline, nil
}

// ensureReferenceDiscoveryLedger ports _ensure_reference_discovery: a
// sync_run_reference_discoveries row is unique per sync_run_id (schema
// constraint), created PLANNED on first sight and left alone afterward.
func ensureReferenceDiscoveryLedger(ctx context.Context, tx pgx.Tx, orgID, runID string, now time.Time) (string, error) {
	var ledgerID string
	err := tx.QueryRow(ctx, `SELECT id::text FROM public.sync_run_reference_discoveries WHERE sync_run_id = $1::uuid`, runID).
		Scan(&ledgerID)
	if err == nil {
		return ledgerID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", ErrReferenceDiscoveryUnavailable
	}
	newID := uuid.New().String()
	if err := tx.QueryRow(ctx, `
INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
VALUES ($1::uuid, $2::uuid, $3, $4, 0, $5, $5, $5)
ON CONFLICT (sync_run_id) DO UPDATE SET sync_run_id = EXCLUDED.sync_run_id
RETURNING id::text`,
		newID, runID, orgID, discoveryStatusPlanned, now).Scan(&ledgerID); err != nil {
		return "", ErrReferenceDiscoveryUnavailable
	}
	return ledgerID, nil
}

// stampSuccess ports run_sync_reference_discovery's success path: the
// terminal UPDATE (guarded by lease ownership and liveness, exactly like the
// claim UPDATE) followed by arming the dispatch_sync_run outbox wakeup --
// Python's OUTBOX_KIND_DISPATCH, the sibling family this one unblocks.
func (service *NativeReferenceDiscoveryService) stampSuccess(
	ctx context.Context, orgID, runID, leaseOwner string, summary map[string]any,
) error {
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	completedAt := service.nowUTC()
	encodedSummary, err := json.Marshal(summary)
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET status = $2, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $3,
    completed_at = $3, error = NULL, result = $4::json, updated_at = $3
WHERE sync_run_id = $1::uuid AND status = $5 AND lease_owner = $6
  AND lease_expires_at IS NOT NULL AND lease_expires_at > $3`,
		runID, discoverySuccessLiteral(), completedAt, encodedSummary, discoveryStatusRunning, leaseOwner)
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	if tag.RowsAffected() == 0 {
		// Python: `return {"status": "skipped", "reason": "lease_lost"}`.
		return tx.Commit(ctx)
	}
	if err := upsertDiscoveryOutboxWakeup(ctx, tx, orgID, runID, outboxKindDispatchSyncRun, completedAt); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	return nil
}

// discoverySuccessLiteral exists only so stampSuccess's $2 argument reads as
// obviously-the-success-status at the call site; discoveryStatusSuccess
// already IS that literal.
func discoverySuccessLiteral() string { return discoveryStatusSuccess }

// handleFailure ports _handle_reference_discovery_failure: retry with
// backoff while attempts remain and the error is retryable, otherwise
// terminalize the ledger, fail every nonterminal unit, stamp the run's
// error, and arm the finalize_sync_run wakeup so the run still completes
// (as FAILED) rather than hanging forever with no dispatcher watching it.
//
// Python's function returns bool (whether the failure was durably recorded
// at all -- false only when the lease was already lost/stolen, which is not
// an error, just a race this attempt lost). Go mirrors that: a lost-lease
// race returns nil (the job succeeded at reporting nothing, matching the
// "skipped"/"lease_lost" branch), never propagated as a job failure.
func (service *NativeReferenceDiscoveryService) handleFailure(
	ctx context.Context, runID, leaseOwner string, discoverErr error,
) error {
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := service.nowUTC()
	var ledgerID, status string
	var attempts int
	var leaseExpiresAt *time.Time
	var actualLeaseOwner *string
	err = tx.QueryRow(ctx, `
SELECT id::text, status, attempts, lease_expires_at, lease_owner
FROM public.sync_run_reference_discoveries
WHERE sync_run_id = $1::uuid`, runID).Scan(&ledgerID, &status, &attempts, &leaseExpiresAt, &actualLeaseOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	actualOwner := ""
	if actualLeaseOwner != nil {
		actualOwner = *actualLeaseOwner
	}
	if !ledgerLeaseIsOwnedAndLive(status, leaseExpiresAt, leaseOwner, actualOwner, now) {
		return tx.Commit(ctx)
	}

	retryable := isRetryableDiscoveryError(discoverErr)
	if retryable && attempts < maxDiscoveryAttempts() {
		availableAt := now.Add(referenceDiscoveryBackoff(attempts, retryAfterSecondsOf(discoverErr)))
		tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET status = $2, available_at = $3, lease_owner = NULL, lease_expires_at = NULL,
    last_heartbeat_at = $4, error = $5, updated_at = $4
WHERE id = $1::uuid AND status = $6 AND lease_owner = $7 AND lease_expires_at > $4`,
			ledgerID, discoveryStatusRetrying, availableAt, now, referenceDiscoveryErrorMessage,
			discoveryStatusRunning, leaseOwner)
		if err != nil {
			return ErrReferenceDiscoveryUnavailable
		}
		if tag.RowsAffected() == 0 {
			return tx.Commit(ctx)
		}
		if err := upsertDiscoveryOutboxWakeup(ctx, tx, "", runID, outboxKindReferenceDiscovery, availableAt); err != nil {
			return err
		}
		return commitOrUnavailable(ctx, tx)
	}

	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET status = $2, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $3,
    completed_at = $3, error = $4, result = $5::json, updated_at = $3
WHERE id = $1::uuid AND status = $6 AND lease_owner = $7 AND lease_expires_at > $3`,
		ledgerID, discoveryStatusFailed, now, referenceDiscoveryErrorMessage,
		discoveryFailureResultJSON(retryable, attempts), discoveryStatusRunning, leaseOwner)
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if err := failNonterminalUnits(ctx, tx, runID, now, referenceDiscoveryErrorMessage); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE public.sync_runs SET error = $2 WHERE id = $1::uuid`,
		runID, referenceDiscoveryErrorMessage); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	if err := upsertDiscoveryOutboxWakeup(ctx, tx, "", runID, outboxKindFinalizeSyncRun, now); err != nil {
		return err
	}
	return commitOrUnavailable(ctx, tx)
}

func commitOrUnavailable(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	return nil
}

func discoveryFailureResultJSON(retryable bool, attempts int) []byte {
	encoded, err := json.Marshal(map[string]any{
		"error_category": referenceDiscoveryErrorCategory,
		"retryable":      retryable,
		"attempts":       attempts,
	})
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

// ledgerLeaseIsOwnedAndLive ports _ledger_lease_is_owned_and_live.
func ledgerLeaseIsOwnedAndLive(status string, leaseExpiresAt *time.Time, ownedBy, actualOwner string, now time.Time) bool {
	if status != discoveryStatusRunning || actualOwner != ownedBy {
		return false
	}
	if leaseExpiresAt == nil {
		return false
	}
	return leaseExpiresAt.After(now)
}

// failNonterminalUnits ports _fail_nonterminal_units.
func failNonterminalUnits(ctx context.Context, tx pgx.Tx, runID string, now time.Time, message string) error {
	resultJSON, err := json.Marshal(map[string]any{"error_category": referenceDiscoveryErrorCategory})
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, error = $3, result = $4::json, lease_owner = NULL, lease_expires_at = NULL,
    last_heartbeat_at = $5, updated_at = $5
WHERE sync_run_id = $1::uuid AND status NOT IN ($6, $7)`,
		runID, syncRunUnitStatusFailed, message, resultJSON, now,
		syncRunUnitStatusSuccess, syncRunUnitStatusFailed); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	return nil
}

// upsertDiscoveryOutboxWakeup arms a durable wakeup for `kind` -- the same
// CASE-based, feature-disabled-preserving upsert shape as
// upsertPostSyncOutboxWakeup, parameterized over kind since reference
// discovery arms three different sibling kinds (itself for a retry,
// dispatch_sync_run on success, finalize_sync_run on terminal failure).
// orgID may be empty: when it is, the org_id is read from sync_runs instead
// of trusted from the caller, matching Python's upsert_outbox_wakeup, which
// always re-resolves it from the run row rather than accepting a
// caller-supplied value.
func upsertDiscoveryOutboxWakeup(ctx context.Context, tx pgx.Tx, orgID, runID, kind string, availableAt time.Time) error {
	if orgID == "" {
		if err := tx.QueryRow(ctx, `SELECT org_id FROM public.sync_runs WHERE id = $1::uuid`, runID).Scan(&orgID); err != nil {
			return ErrReferenceDiscoveryUnavailable
		}
	}
	_, err := tx.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    created_at, updated_at
) VALUES ($5::uuid, $1, $2::uuid, $4, 'pending', $3, 0, $3, $3)
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET status = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.status
        ELSE 'pending'
    END,
    available_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.available_at
        ELSE LEAST(public.sync_dispatch_outbox.available_at, EXCLUDED.available_at)
    END,
    dispatched_at = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_at
        ELSE NULL
    END,
    last_error = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.last_error
        ELSE NULL
    END,
    claim_token = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_token
        ELSE NULL
    END,
    claim_expires_at = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_expires_at
        ELSE NULL
    END,
    claim_transport = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_transport
        ELSE NULL
    END,
    claim_route_generation = CASE
        WHEN NOT (
            public.sync_dispatch_outbox.status = 'dispatched'
            AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        )
         AND public.sync_dispatch_outbox.claim_expires_at IS NOT NULL
         AND public.sync_dispatch_outbox.claim_expires_at > EXCLUDED.updated_at
        THEN public.sync_dispatch_outbox.claim_route_generation
        ELSE NULL
    END,
    dispatched_transport = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_transport
        ELSE NULL
    END,
    dispatched_route_generation = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.dispatched_route_generation
        ELSE NULL
    END,
    transport_job_id = CASE
        WHEN public.sync_dispatch_outbox.status = 'dispatched'
         AND public.sync_dispatch_outbox.last_error = 'feature_disabled'
        THEN public.sync_dispatch_outbox.transport_job_id
        ELSE NULL
    END,
    updated_at = EXCLUDED.updated_at`,
		orgID, runID, availableAt, kind, uuid.New().String())
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}
	return nil
}

const outboxKindDispatchSyncRun = "dispatch_sync_run"

// isRetryableDiscoveryError ports _is_retryable_discovery_error's
// message/type-name substring classification. Python also special-cases
// RateLimitException/TimeoutError/SoftTimeLimitExceeded by TYPE; the native
// port has no equivalent typed exceptions yet (the populate bridge hasn't
// landed), so this is deliberately the substring-only fallback for now --
// revisited once the bridge's real error shapes exist.
func isRetryableDiscoveryError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"timeout", "rate", "429", "temporar", "transient", "too many"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func retryAfterSecondsOf(err error) *float64 {
	var provider retryAfterProvider
	if errors.As(err, &provider) {
		value := provider.RetryAfterSeconds()
		return &value
	}
	return nil
}

// referenceDiscoveryBackoff ports _reference_discovery_backoff_seconds
// verbatim, including the jitter, which Python draws from
// random.randint(0, max(1, min(base, 30))). Go's math/rand default source is
// unseeded-but-deterministic-per-process in the same way Python's unseeded
// `random` module is process-global -- neither this port nor Python's own
// function needs cryptographic randomness here, only enough spread to avoid
// a thundering herd.
func referenceDiscoveryBackoff(attempts int, retryAfterSeconds *float64) time.Duration {
	exponent := attempts - 1
	if exponent < 0 {
		exponent = 0
	}
	if exponent > 5 {
		exponent = 5
	}
	base := 30 * (1 << uint(exponent))
	if base > 900 {
		base = 900
	}
	if retryAfterSeconds != nil && *retryAfterSeconds > 0 {
		requested := int(*retryAfterSeconds)
		if requested > base {
			base = requested
		}
		if base > 900 {
			base = 900
		}
	}
	jitterBound := base
	if jitterBound > 30 {
		jitterBound = 30
	}
	if jitterBound < 1 {
		jitterBound = 1
	}
	jitter := rand.Intn(jitterBound + 1)
	return time.Duration(base+jitter) * time.Second
}

func maxDiscoveryAttempts() int {
	return envPositiveInt("SYNC_REFERENCE_DISCOVERY_MAX_ATTEMPTS", 5)
}

func discoveryLeaseSeconds() time.Duration {
	return time.Duration(envPositiveInt("SYNC_REFERENCE_DISCOVERY_LEASE_SECONDS", 300)) * time.Second
}

func maxDiscoveryLifetime() time.Duration {
	seconds := envPositiveInt("SYNC_REFERENCE_DISCOVERY_MAX_LIFETIME_SECONDS", 3720)
	if seconds < 3600 {
		seconds = 3600
	}
	return time.Duration(seconds) * time.Second
}

func discoveryHeartbeatInterval() time.Duration {
	lease := discoveryLeaseSeconds()
	quarter := lease / 4
	if quarter > 60*time.Second {
		return 60 * time.Second
	}
	if quarter < time.Second {
		return time.Second
	}
	return quarter
}

func envPositiveInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return fallback
	}
	return value
}

func earlier(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// startHeartbeat ports _start_reference_discovery_heartbeat /
// _heartbeat_reference_discovery: a background goroutine that keeps the
// ledger's lease alive while the (potentially slow) discovery work runs, and
// stops itself the moment it observes the lease is no longer live -- exactly
// like Python's heartbeat thread setting its own stop_event when the UPDATE
// matches zero rows. Returns a stop function the caller must always invoke
// (`defer stopHeartbeat()`), matching Python's `finally` block.
func (service *NativeReferenceDiscoveryService) startHeartbeat(runID, leaseOwner string, deadline time.Time) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	interval := discoveryHeartbeatInterval()
	leaseSeconds := discoveryLeaseSeconds()
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				now := service.nowUTC()
				if !now.Before(deadline) {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				newExpiry := earlier(now.Add(leaseSeconds), deadline)
				tag, err := service.pool.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET lease_expires_at = $4, last_heartbeat_at = $3, updated_at = $3
WHERE sync_run_id = $1::uuid AND status = $5 AND lease_owner = $2 AND lease_expires_at > $3`,
					runID, leaseOwner, now, newExpiry, discoveryStatusRunning)
				cancel()
				if err != nil {
					service.logger.ErrorContext(context.Background(), "run_sync_reference_discovery.heartbeat_failed",
						slog.String("error_code", referenceDiscoveryErrorCategory),
						slog.String("sync_run_id", runID),
						slog.String("error", err.Error()))
					continue
				}
				if tag.RowsAffected() == 0 {
					return
				}
			}
		}
	}()
	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() { close(stop) })
		<-done
	}
}
