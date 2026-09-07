package externalrecompute

// drain.go is the native consumer that replaces the Celery task
// dev_health_ops.workers.tasks.dispatch_external_ingest_recompute_bridge
// (CHAOS-5296, absorbing CHAOS-4057).
//
// WHY THIS EXISTS. The Go stream runner has written external-ingest recompute
// rows continuously, and Celery -- their only reader -- has been stopped in
// production since 2026-08-19. A live writer with a dead reader is not a
// degraded recompute, it is no recompute at all, and nothing said so. This
// consumer is that reader, in Go, enqueuing through the SAME native job kinds
// the scheduled and post-sync paths already use rather than a second compute
// path of its own.
//
// WHAT IT DELIBERATELY DOES NOT DO. It does not compute anything. Every row it
// drains turns into metrics.daily_dispatch and/or investment.materialize
// handoffs on the existing outbox -- the identical two dispatch surfaces the
// Python planner targeted by Celery task name. This is plumbing, not a metrics
// port.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The two native job kinds this consumer enqueues into. They are the SAME
// kinds the scheduled fan-out and post-sync fanout publish, aliased here only
// so the ledger-row writer below reads as the Python original did.
const (
	dailyDispatchKind         = jobcontract.KindDailyMetricsDispatch
	investmentMaterializeKind = jobcontract.KindInvestmentMaterialize
)

const (
	// NativeDrainTaskName is the dispatch-target discriminator written into
	// external_ingest_recompute_jobs.celery_task_name by
	// PostgresNativeDispatcher, and the ONLY value this consumer claims.
	//
	// It is deliberately a NEW value rather than the retired Celery task name.
	// That single fact is what keeps ~2.5 weeks of accumulated legacy rows
	// structurally invisible to this consumer: a worker starting up cannot
	// replay the backlog even by accident, because it never selects those rows.
	// The backlog is reachable only through `dev-health-workerctl
	// external-recompute replay`, which an operator runs once, by hand, and
	// which collapses the backlog instead of replaying it row by row.
	NativeDrainTaskName = "external_ingest.recompute.native.v1"

	// LegacyCeleryTaskName is the retired Celery task name still carried by
	// every backlog row. Nothing but the replay command may select on it.
	LegacyCeleryTaskName = "dev_health_ops.workers.tasks.dispatch_external_ingest_recompute_bridge"

	statusPending    = "bridge_pending"
	statusClaimed    = "bridge_claimed"
	statusDispatched = "bridge_dispatched"
	statusFailed     = "bridge_failed"

	// The four batch-row recompute outcomes, preserved verbatim from the Python
	// RecomputeDispatchResult.status vocabulary. The API surface
	// (api/external_ingest/status.py) and the operator runbooks read these
	// strings, so a "clearer" Go spelling here would be a silent contract break.
	outcomeNotApplicable  = "not_applicable"
	outcomeDispatched     = "dispatched"
	outcomeSkippedNoScope = "skipped_no_scope"
	outcomeFailed         = "failed"
)

// ErrPermanent marks a rejection no retry can fix -- a malformed bridge payload,
// or an envelope the job contract refuses (a non-RFC-4122 organization id, say).
// Such a row is marked bridge_failed and logged with its cause rather than left
// to be reclaimed by the stale lease forever, which is the difference between a
// visible bad row and an invisible hot loop (CHAOS-3903's lesson, applied here).
var ErrPermanent = errors.New("external recompute: permanent rejection")

// Enqueued reports what one drained row actually produced, so the caller can log
// it and the tests can assert it without reading the outbox twice.
type Enqueued struct {
	DailyRunIDs         []string
	InvestmentRequestID string
}

// Enqueuer turns a bounded plan into native job handoffs inside the CALLER's
// transaction. The transaction is the caller's on purpose: the handoff rows and
// the bridge row's terminal mark must commit together or not at all, so a crash
// can never leave work enqueued under a row that still looks pending (which
// would double-enqueue on the next lease reclaim) nor a row marked done with
// nothing enqueued (which is the silent-drop this whole ticket exists about).
//
// Implemented by the worker (cmd/dev-health-worker/external_recompute.go), which
// owns the daily store/publisher and work-graph request writer.
type Enqueuer interface {
	Enqueue(ctx context.Context, tx pgx.Tx, plan Plan, correlation string) (Enqueued, error)
}

// DrainConfig bounds the consumer's loop.
type DrainConfig struct {
	// PollInterval is the idle cadence. A batch that filled to BatchSize is
	// followed immediately by another pass rather than this sleep, so a burst
	// drains at write speed instead of BatchSize rows per interval.
	PollInterval time.Duration
	// ClaimLease mirrors the Python task's _GO_BRIDGE_CLAIM_TTL: a row claimed
	// longer ago than this is eligible again, covering a worker that died
	// between claiming and enqueuing.
	ClaimLease time.Duration
	// BatchSize mirrors _GO_BRIDGE_BATCH_LIMIT.
	BatchSize int
	// StaleWarnAfter is the age at which a claimed row's ORIGINAL dispatch time
	// is reported at Warn. It exists to make the exact failure mode that
	// produced this ticket -- a live writer and a stopped reader -- loud the
	// next time it happens, instead of discoverable only by a repo audit.
	StaleWarnAfter time.Duration
}

// DefaultDrainConfig carries the Python task's own numbers where they exist.
func DefaultDrainConfig() DrainConfig {
	return DrainConfig{
		PollInterval:   10 * time.Second, // the retired Beat entry's cadence
		ClaimLease:     5 * time.Minute,  // _GO_BRIDGE_CLAIM_TTL
		BatchSize:      50,               // _GO_BRIDGE_BATCH_LIMIT
		StaleWarnAfter: 7 * 24 * time.Hour,
	}
}

func (cfg DrainConfig) validate() error {
	if cfg.PollInterval < time.Second || cfg.PollInterval > 5*time.Minute ||
		cfg.ClaimLease < time.Minute || cfg.ClaimLease > time.Hour ||
		cfg.BatchSize < 1 || cfg.BatchSize > 1_000 ||
		cfg.StaleWarnAfter < time.Minute {
		return ErrInvalidConfig
	}
	return nil
}

// Drain claims native-dispatch rows and turns each into native job handoffs.
type Drain struct {
	pool     *pgxpool.Pool
	enqueuer Enqueuer
	config   DrainConfig
	logger   *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewDrain constructs the consumer. Every collaborator is required: a Drain
// built without a logger would be the CHAOS-3907 defect again, where a failing
// step was discarded with no counter and no output at all.
func NewDrain(
	pool *pgxpool.Pool,
	enqueuer Enqueuer,
	cfg DrainConfig,
	logger *slog.Logger,
) (*Drain, error) {
	if pool == nil || enqueuer == nil || logger == nil || cfg.validate() != nil {
		return nil, ErrInvalidConfig
	}
	return &Drain{
		pool: pool, enqueuer: enqueuer, config: cfg, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (*Drain) Name() string { return "external-recompute-drain" }

func (drain *Drain) Start(parent context.Context) error {
	if drain == nil || parent == nil || parent.Err() != nil {
		return ErrInvalidConfig
	}
	drain.mu.Lock()
	defer drain.mu.Unlock()
	if drain.done != nil {
		return ErrInvalidConfig
	}
	ctx, cancel := context.WithCancel(parent)
	drain.cancel = cancel
	drain.done = make(chan struct{})
	go drain.run(ctx, drain.done)
	return nil
}

func (drain *Drain) Shutdown(ctx context.Context) error {
	if drain == nil || ctx == nil {
		return ErrInvalidConfig
	}
	drain.mu.Lock()
	cancel, done := drain.cancel, drain.done
	drain.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (drain *Drain) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			drained, err := drain.Step(ctx)
			if err != nil && ctx.Err() == nil {
				drain.logger.ErrorContext(ctx, "external recompute drain step failed",
					"error", err.Error())
			}
			// A full batch means there is very likely more behind it. Waiting a
			// whole PollInterval between full batches caps the drain at
			// BatchSize per interval, which is how a backlog outlives the burst
			// that created it.
			delay := drain.config.PollInterval
			if drained >= drain.config.BatchSize {
				delay = time.Millisecond
			}
			timer.Reset(delay)
		}
	}
}

// Step claims one batch and drains it, returning how many rows were claimed.
// Exported so an integration test can drive exactly one pass deterministically
// rather than racing the loop's timer.
func (drain *Drain) Step(ctx context.Context) (int, error) {
	claims, err := drain.claimBatch(ctx)
	if err != nil {
		return 0, err
	}
	var failures []error
	for _, claim := range claims {
		if ctx.Err() != nil {
			return len(claims), ctx.Err()
		}
		if err := drain.drainOne(ctx, claim); err != nil {
			failures = append(failures, err)
		}
	}
	return len(claims), errors.Join(failures...)
}

type drainClaim struct {
	JobID          uuid.UUID
	BridgeID       string
	OrgID          string
	SourceSystem   string
	SourceInstance string
	// FirstDispatchedAt is the row's dispatch time BEFORE this claim overwrote
	// it. The lease and the age share one column (as they did in Python), so a
	// reclaimed row's reported age is the age since its previous claim, not
	// since it was first written -- still monotonically increasing while a row
	// is stuck, which is what the Warn is for.
	FirstDispatchedAt time.Time
}

func (drain *Drain) claimBatch(ctx context.Context) ([]drainClaim, error) {
	now := drain.now().UTC()
	// One statement rather than Python's SELECT-then-N-UPDATEs: the claim and
	// its mark cannot interleave with another worker's, and a batch of 50 costs
	// one round trip instead of 51.
	rows, err := drain.pool.Query(ctx, `
UPDATE external_ingest_recompute_jobs AS jobs
SET status = $4, dispatched_at = $5
FROM (
    SELECT id, dispatched_at AS first_dispatched_at
    FROM external_ingest_recompute_jobs
    WHERE celery_task_name = $1
      AND (status = $2 OR (status = $4 AND dispatched_at < $3))
    ORDER BY dispatched_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $6
) AS due
WHERE jobs.id = due.id
RETURNING jobs.id, jobs.celery_task_id, jobs.org_id, jobs.source_system,
          jobs.source_instance, due.first_dispatched_at`,
		NativeDrainTaskName, statusPending, now.Add(-drain.config.ClaimLease),
		statusClaimed, now, drain.config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("claim external recompute rows: %w", err)
	}
	defer rows.Close()
	claims := make([]drainClaim, 0, drain.config.BatchSize)
	for rows.Next() {
		var claim drainClaim
		var bridgeID *string
		if err := rows.Scan(&claim.JobID, &bridgeID, &claim.OrgID,
			&claim.SourceSystem, &claim.SourceInstance, &claim.FirstDispatchedAt); err != nil {
			return nil, fmt.Errorf("scan external recompute claim: %w", err)
		}
		if bridgeID != nil {
			claim.BridgeID = *bridgeID
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate external recompute claims: %w", err)
	}
	for _, claim := range claims {
		if age := now.Sub(claim.FirstDispatchedAt); age > drain.config.StaleWarnAfter {
			// The signal this ticket exists because of: rows piling up behind a
			// reader that is not consuming them. Logged per row and not
			// aggregated, because the org and source instance are the part an
			// operator needs to act on.
			drain.logger.WarnContext(ctx, "external recompute row claimed long after it was written",
				"bridge_id", claim.BridgeID,
				"org_id", claim.OrgID,
				"source_system", claim.SourceSystem,
				"source_instance", claim.SourceInstance,
				"age", age.String())
		}
	}
	return claims, nil
}

func (drain *Drain) drainOne(ctx context.Context, claim drainClaim) error {
	scope, ingestionIDs, err := loadBridgeScope(ctx, drain.pool, claim.OrgID,
		claim.SourceSystem, claim.SourceInstance, claim.BridgeID)
	if err != nil {
		if errors.Is(err, ErrPermanent) {
			drain.logger.ErrorContext(ctx, "external recompute bridge payload rejected",
				"bridge_id", claim.BridgeID, "org_id", claim.OrgID, "error", err.Error())
			return drain.failPermanently(ctx, claim, err)
		}
		drain.logger.ErrorContext(ctx, "external recompute scope load failed",
			"bridge_id", claim.BridgeID, "org_id", claim.OrgID, "error", err.Error())
		// Leave the row bridge_claimed: its bounded stale lease makes this
		// retryable without replaying an already-completed ingestion batch.
		return err
	}
	if scope == nil {
		// Crash-after-enqueue/before-mark: the batch outcomes are already
		// non-pending, so this stale claim completes without re-emitting jobs.
		drain.logger.InfoContext(ctx, "external recompute row already terminal",
			"bridge_id", claim.BridgeID, "org_id", claim.OrgID)
		return drain.mark(ctx, claim.JobID, statusDispatched)
	}

	plan := PlanRecompute(*scope, drain.now())
	outcome, enqueued, err := drain.applyPlan(ctx, claim, *scope, plan, ingestionIDs)
	if err != nil {
		if errors.Is(err, ErrPermanent) {
			drain.logger.ErrorContext(ctx, "external recompute enqueue permanently rejected",
				"bridge_id", claim.BridgeID, "org_id", claim.OrgID, "error", err.Error())
			return drain.failPermanently(ctx, claim, err)
		}
		drain.logger.ErrorContext(ctx, "external recompute enqueue failed",
			"bridge_id", claim.BridgeID, "org_id", claim.OrgID, "error", err.Error())
		return err
	}
	drain.logConsumed(ctx, claim, plan, outcome, enqueued, len(ingestionIDs))
	return nil
}

// applyPlan performs the whole terminal half of one row in ONE transaction:
// enqueue, batch-status write, job-log rows, and the bridge row's terminal mark.
// See Enqueuer's doc comment for why they cannot be split.
func (drain *Drain) applyPlan(
	ctx context.Context,
	claim drainClaim,
	scope PlanScope,
	plan Plan,
	ingestionIDs []string,
) (string, Enqueued, error) {
	tx, err := drain.pool.Begin(ctx)
	if err != nil {
		return "", Enqueued{}, fmt.Errorf("begin external recompute drain: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var enqueued Enqueued
	outcome := outcomeNotApplicable
	if plan.Trigger {
		enqueued, err = drain.enqueuer.Enqueue(ctx, tx, plan, "ext-recompute:"+claim.BridgeID)
		if err != nil {
			return "", Enqueued{}, err
		}
		outcome = outcomeSkippedNoScope
		if len(enqueued.DailyRunIDs) > 0 || enqueued.InvestmentRequestID != "" {
			outcome = outcomeDispatched
		}
	}

	now := drain.now().UTC()
	if err := writeBatchOutcome(ctx, tx, claim.OrgID, ingestionIDs, scope, plan, outcome, now); err != nil {
		return "", Enqueued{}, err
	}
	if err := writeJobLog(ctx, tx, claim, enqueued, now); err != nil {
		return "", Enqueued{}, err
	}
	if err := markTx(ctx, tx, claim.JobID, statusDispatched); err != nil {
		return "", Enqueued{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", Enqueued{}, fmt.Errorf("commit external recompute drain: %w", err)
	}
	committed = true
	return outcome, enqueued, nil
}

func (drain *Drain) logConsumed(
	ctx context.Context,
	claim drainClaim,
	plan Plan,
	outcome string,
	enqueued Enqueued,
	ingestionCount int,
) {
	// Every field the Python task already reported (org, source, window, repo
	// and team counts, which kinds were enqueued, and both capped flags) is
	// here. The capped flags in particular are the existing "this recompute was
	// truncated" signal and are the easiest thing to lose in a port -- a
	// truncated recompute that logs like a complete one is indistinguishable
	// from a correct one afterwards.
	drain.logger.InfoContext(ctx, "external recompute row consumed",
		"bridge_id", claim.BridgeID,
		"org_id", claim.OrgID,
		"source_system", claim.SourceSystem,
		"source_instance", claim.SourceInstance,
		"outcome", outcome,
		"ingestion_ids", ingestionCount,
		"day", planDayString(plan),
		"backfill_days", plan.BackfillDays,
		"from_date", planDateString(plan.FromDate),
		"to_date", planDateString(plan.ToDate),
		"repo_ids", len(plan.RepoIDs),
		"team_ids", len(plan.TeamIDs),
		"dispatch_daily", plan.DispatchDaily,
		"fallback_org_wide_daily", plan.FallbackOrgWideDaily,
		"skip_investment_no_scope", plan.SkipInvestmentNoScope,
		"daily_runs_enqueued", len(enqueued.DailyRunIDs),
		"investment_request_id", enqueued.InvestmentRequestID,
		"capped_days", plan.CappedDays,
		"capped_repos", plan.CappedRepos)
}

func (drain *Drain) mark(ctx context.Context, jobID uuid.UUID, status string) error {
	if _, err := drain.pool.Exec(ctx, `
UPDATE external_ingest_recompute_jobs
SET status = $2
WHERE id = $1 AND celery_task_name = $3`, jobID, status, NativeDrainTaskName); err != nil {
		return fmt.Errorf("mark external recompute row %s: %w", status, err)
	}
	return nil
}

// failPermanently retires a row no retry can fix, and -- the part that is easy
// to forget -- retires the BATCH rows with it. Marking only the bridge row
// would strand every batch it covered at recompute_status='pending' forever:
// the bridge row is never claimed again, so nothing would ever move them, and
// the external-ingest status API would report a recompute as still in flight
// for the life of the row. Both halves commit together.
func (drain *Drain) failPermanently(ctx context.Context, claim drainClaim, cause error) error {
	tx, err := drain.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin external recompute failure: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	now := drain.now().UTC()
	if _, err := tx.Exec(ctx, `
UPDATE external_ingest_batches
SET recompute_status = $5,
    recompute_completed_at = $6,
    recompute_error = $7
WHERE org_id = $1 AND source_system = $2 AND source_instance = $3
  AND recompute_status = 'pending'
  AND COALESCE(recompute_scope ->> 'bridgeId', '') = $4`,
		claim.OrgID, claim.SourceSystem, claim.SourceInstance, claim.BridgeID,
		outcomeFailed, now, cause.Error()); err != nil {
		return fmt.Errorf("persist external recompute permanent failure: %w", err)
	}
	if err := markTx(ctx, tx, claim.JobID, statusFailed); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit external recompute failure: %w", err)
	}
	committed = true
	return nil
}

func markTx(ctx context.Context, tx pgx.Tx, jobID uuid.UUID, status string) error {
	if _, err := tx.Exec(ctx, `
UPDATE external_ingest_recompute_jobs
SET status = $2
WHERE id = $1`, jobID, status); err != nil {
		return fmt.Errorf("mark external recompute row %s: %w", status, err)
	}
	return nil
}

// loadBridgeScope is the port of the Python task's _load_go_bridge_scope: read
// the allowlisted payload back off the batch rows this bridge id covers,
// refusing anything whose version or kind it does not recognise, and refusing a
// set of rows that disagree with each other.
//
// A nil scope with a nil error means "no pending rows carry this bridge id",
// which is the already-terminal case, not an error.
func loadBridgeScope(
	ctx context.Context,
	pool *pgxpool.Pool,
	orgID, sourceSystem, sourceInstance, bridgeID string,
) (*PlanScope, []string, error) {
	rows, err := pool.Query(ctx, `
SELECT ingestion_id, recompute_scope
FROM external_ingest_batches
WHERE org_id = $1 AND source_system = $2 AND source_instance = $3
  AND recompute_status = 'pending'
ORDER BY ingestion_id`, orgID, sourceSystem, sourceInstance)
	if err != nil {
		return nil, nil, fmt.Errorf("load external recompute bridge scope: %w", err)
	}
	defer rows.Close()

	var (
		scope        *PlanScope
		canonical    string
		ingestionIDs []string
	)
	for rows.Next() {
		var ingestionID uuid.UUID
		var raw []byte
		if err := rows.Scan(&ingestionID, &raw); err != nil {
			return nil, nil, fmt.Errorf("scan external recompute bridge scope: %w", err)
		}
		candidate, matched, err := decodeBridgeScope(raw, orgID, bridgeID)
		if err != nil {
			return nil, nil, err
		}
		if !matched {
			continue
		}
		// Byte-comparing the canonical JSON is how the Python original detected
		// two batch rows claiming the same bridge id with different scopes. It
		// is a real corruption signal, not a formality: the bridge id is
		// derived from the claim, so disagreeing scopes under one id mean the
		// writer and the reader no longer agree on what was ingested.
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: re-encode bridge scope: %w", ErrPermanent, err)
		}
		if scope != nil && canonical != string(encoded) {
			return nil, nil, fmt.Errorf("%w: inconsistent external recompute bridge scope", ErrPermanent)
		}
		scope, canonical = &candidate, string(encoded)
		ingestionIDs = append(ingestionIDs, ingestionID.String())
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate external recompute bridge scope: %w", err)
	}
	if scope == nil {
		return nil, nil, nil
	}
	return scope, sortedUnique(ingestionIDs), nil
}

func decodeBridgeScope(raw []byte, orgID, bridgeID string) (PlanScope, bool, error) {
	var payload bridgeScope
	if err := json.Unmarshal(raw, &payload); err != nil {
		return PlanScope{}, false, fmt.Errorf("%w: decode bridge scope: %w", ErrPermanent, err)
	}
	if payload.BridgeID != bridgeID {
		return PlanScope{}, false, nil
	}
	if payload.BridgeVersion != 1 || payload.BridgeKind != CompatibilityBridgeKind {
		return PlanScope{}, false, fmt.Errorf(
			"%w: unsupported external recompute bridge payload (version %d kind %q)",
			ErrPermanent, payload.BridgeVersion, payload.BridgeKind)
	}
	windowStart, err := parseOptionalBridgeTime(payload.WindowStartedAt)
	if err != nil {
		return PlanScope{}, false, fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	windowEnd, err := parseOptionalBridgeTime(payload.WindowEndedAt)
	if err != nil {
		return PlanScope{}, false, fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	return PlanScope{
		OrgID:       orgID,
		RepoIDs:     sortedUnique(payload.RepoIDs),
		TeamIDs:     sortedUnique(payload.TeamIDs),
		RecordKinds: sortedUnique(payload.RecordKinds),
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
	}, true, nil
}

// writeBatchOutcome is the port of recompute_status.record_recompute_dispatch's
// batch half: every ingestion folded into this coalesced scope gets the same
// outcome snapshot, because they all fed one bounded plan (the Python original's
// Risk 5 -- a flush covering several ingestion ids must not update only one).
func writeBatchOutcome(
	ctx context.Context,
	tx pgx.Tx,
	orgID string,
	ingestionIDs []string,
	scope PlanScope,
	plan Plan,
	outcome string,
	now time.Time,
) error {
	if len(ingestionIDs) == 0 {
		return nil
	}
	scopeJSON, err := json.Marshal(struct {
		RepoIDs         []string `json:"repoIds"`
		TeamIDs         []string `json:"teamIds"`
		RecordKinds     []string `json:"recordKinds"`
		WindowStartedAt *string  `json:"windowStartedAt"`
		WindowEndedAt   *string  `json:"windowEndedAt"`
		CappedDays      bool     `json:"cappedDays"`
		CappedRepos     bool     `json:"cappedRepos"`
	}{
		// The scope's OWN repo/team/kind sets and window, not the plan's
		// capped ones: this blob is the record of what was ingested, and the
		// capped flags beside it are what say the plan narrowed it. Writing the
		// capped sets here instead would erase the evidence that anything was
		// truncated (Python wrote the scope's sets for the same reason).
		RepoIDs:         orEmpty(scope.RepoIDs),
		TeamIDs:         orEmpty(scope.TeamIDs),
		RecordKinds:     orEmpty(scope.RecordKinds),
		WindowStartedAt: optionalTimeRFC3339(scope.WindowStart),
		WindowEndedAt:   optionalTimeRFC3339(scope.WindowEnd),
		CappedDays:      plan.CappedDays,
		CappedRepos:     plan.CappedRepos,
	})
	if err != nil {
		return fmt.Errorf("%w: encode recompute outcome scope: %w", ErrPermanent, err)
	}
	var dispatchedAt *time.Time
	if outcome == outcomeDispatched {
		dispatchedAt = &now
	}
	for _, ingestionID := range ingestionIDs {
		if _, err := tx.Exec(ctx, `
UPDATE external_ingest_batches
SET recompute_status = $3,
    recompute_scope = $4,
    recompute_dispatched_at = $5,
    recompute_completed_at = $6,
    recompute_error = NULL
WHERE org_id = $1 AND ingestion_id = $2`,
			orgID, ingestionID, outcome, scopeJSON, dispatchedAt, now); err != nil {
			return fmt.Errorf("persist external recompute batch outcome: %w", err)
		}
	}
	return nil
}

// writeJobLog is the port of record_recompute_dispatch's job-ledger half: one
// row per handoff actually enqueued. celery_task_name carries the NATIVE job
// kind now -- the column name is legacy, its meaning is "what this handoff went
// to", and renaming it is a migration this port deliberately does not take on.
func writeJobLog(
	ctx context.Context,
	tx pgx.Tx,
	claim drainClaim,
	enqueued Enqueued,
	now time.Time,
) error {
	// repo_id is left NULL on every row, unlike Python. That is not a dropped
	// field: Python fanned out one Celery task PER repository, so each ledger
	// row named the one repository its task covered. A native daily run carries
	// its whole repository scope on the run itself (daily_metrics_partitions),
	// so there is no single repository to name here, and inventing one would be
	// wrong rather than merely absent.
	type logRow struct {
		kind  string
		jobID string
		queue string
	}
	rows := make([]logRow, 0, len(enqueued.DailyRunIDs)+1)
	for _, runID := range enqueued.DailyRunIDs {
		rows = append(rows, logRow{kind: dailyDispatchKind, jobID: runID, queue: "metrics"})
	}
	if enqueued.InvestmentRequestID != "" {
		rows = append(rows, logRow{
			kind:  investmentMaterializeKind,
			jobID: enqueued.InvestmentRequestID,
			queue: "default",
		})
	}
	for _, row := range rows {
		if _, err := tx.Exec(ctx, `
INSERT INTO external_ingest_recompute_jobs (
    id, org_id, source_system, source_instance, celery_task_name,
    celery_task_id, queue, repo_id, status, dispatched_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'dispatched',$9)
ON CONFLICT (id) DO NOTHING`,
			// Deterministic id per (bridge, kind, handoff): a lease reclaim that
			// re-enqueues an already-published handoff must not also grow a
			// second ledger row for it.
			uuid.NewSHA1(recomputeJobLogNamespace, []byte(claim.BridgeID+":"+row.kind+":"+row.jobID)),
			claim.OrgID, claim.SourceSystem, claim.SourceInstance, row.kind,
			row.jobID, row.queue, nil, now); err != nil {
			return fmt.Errorf("persist external recompute job log: %w", err)
		}
	}
	return nil
}

var recomputeJobLogNamespace = uuid.MustParse("2f0f6ad6-6c8a-5a4e-9f2d-6b3c1a9e4d70")

func orEmpty(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func optionalTimeRFC3339(value *time.Time) *string {
	if value == nil || value.IsZero() {
		return nil
	}
	formatted := value.UTC().Format(time.RFC3339Nano)
	return &formatted
}

func planDayString(plan Plan) string {
	if plan.Day.IsZero() {
		return ""
	}
	return plan.Day.Format(time.DateOnly)
}

func planDateString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
