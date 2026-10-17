package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	gosync "sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// Initial backfill (CHAOS-2275). A new configuration syncs only its initial
// window (resolveInitialSyncDepth, 30 days by default). When that first sync
// succeeds, this enqueuer mints ONE backfill for the history older than the
// window, through the same seam the admin backfill route uses
// (synchandoff.Mint, triggered_by "backfill"), and records its backfill_jobs row
// so the existing progress views and observeTerminalSyncRun see it.
//
// It runs on the coordinator role: the domain role (where finalize runs) must
// never create a backfill job. The coordinator holds INSERT on backfill_jobs
// for this one use.
//
// Idempotency has no table of its own. The occurrence identity is
// (configuration, scheduled_for) and scheduled_for is fixed to the first
// successful run's completed_at, so a second scheduler, a re-sync or a replay
// mints the same identity and Mint reports it Existing (ON CONFLICT DO NOTHING
// waits for a concurrent winner to commit). The backfill_jobs row is written in
// the same transaction as the occurrence.
//
// Only a configuration whose FIRST successful run started at or after the roll
// instant (INITIAL_BACKFILL_NOT_BEFORE; the feature is off without it) and
// completed within initialBackfillMaxAge is a candidate, and a configuration
// with ANY run (failed ones included) created before the roll instant is not,
// so a rollout never backfills a configuration that was already syncing.
const (
	initialBackfillMaxAge          = 7 * 24 * time.Hour
	initialBackfillDefaultHorizon  = 365
	initialBackfillHorizonEnv      = "INITIAL_BACKFILL_HORIZON_DAYS"
	initialBackfillNotBeforeEnv    = "INITIAL_BACKFILL_NOT_BEFORE"
	initialBackfillTriggeredBy     = "backfill"
	initialBackfillMode            = SyncModeBackfill
	initialBackfillOutcomeEnqueued = "enqueued"

	InitialBackfillSkipCapNotAboveDepth = "cap_le_depth"
	InitialBackfillSkipAlreadyEnqueued  = "already_enqueued"
	InitialBackfillSkipConfigGone       = "config_gone"
	InitialBackfillSkipInactive         = "config_inactive"
	InitialBackfillSkipNotPlanner       = "not_planner_managed"
	InitialBackfillSkipOrgIneligible    = "org_ineligible"
	initialBackfillOutcomeFailed        = "failed"
)

type initialBackfillCandidate struct {
	configID       string
	orgID          string
	integrationOpt []byte
	runCreatedAt   time.Time
	completedAt    time.Time
}

type initialBackfillEnqueuer struct {
	pool   *pgxpool.Pool
	maxAge time.Duration
	// notBefore is the roll instant: only a first sync that STARTED at or after
	// it is a candidate, so a configuration that was already syncing when the
	// feature went live is never backfilled.
	notBefore time.Time

	mu       gosync.Mutex
	outcomes map[string]uint64
	// seen keeps the skips already counted for a (configuration, first success),
	// with that first success's instant, so a candidate that stays one for the
	// window is counted once and is left out of the next scan.
	seen map[string]time.Time
}

// initialBackfillNotBefore reads INITIAL_BACKFILL_NOT_BEFORE (RFC 3339). The
// feature is OFF when it is unset or unparseable: it is switched on by the roll
// that sets it, never by the image alone.
func initialBackfillNotBefore() (time.Time, bool) {
	raw := strings.TrimSpace(os.Getenv(initialBackfillNotBeforeEnv))
	if raw == "" {
		return time.Time{}, false
	}
	instant, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		slog.Error("sync.initial_backfill.disabled", slog.String("reason", "unparseable "+initialBackfillNotBeforeEnv))
		return time.Time{}, false
	}
	return instant.UTC(), true
}

func newInitialBackfillEnqueuer(pool *pgxpool.Pool, notBefore time.Time) *initialBackfillEnqueuer {
	return &initialBackfillEnqueuer{
		pool: pool, maxAge: initialBackfillMaxAge, notBefore: notBefore,
		outcomes: map[string]uint64{}, seen: map[string]time.Time{},
	}
}

func (enqueuer *initialBackfillEnqueuer) count(outcome string) {
	enqueuer.mu.Lock()
	enqueuer.outcomes[outcome]++
	enqueuer.mu.Unlock()
}

// countSkipOnce counts and reports true the first time a skip is seen.
func (enqueuer *initialBackfillEnqueuer) countSkipOnce(candidate initialBackfillCandidate, reason string) bool {
	key := candidate.configID + "|" + candidate.completedAt.UTC().Format(time.RFC3339Nano) + "|" + reason
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	if _, done := enqueuer.seen[key]; done {
		return false
	}
	enqueuer.seen[key] = candidate.completedAt
	enqueuer.outcomes[reason]++
	return true
}

// pruneSeen forgets the skips whose first success is past the age bound; those
// configurations can no longer be candidates.
func (enqueuer *initialBackfillEnqueuer) pruneSeen(now time.Time) {
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	for key, completedAt := range enqueuer.seen {
		if completedAt.Before(now.Add(-enqueuer.maxAge)) {
			delete(enqueuer.seen, key)
		}
	}
}

// skippedConfigs are the configurations already skipped for their current first
// success, left out of the candidate query so they cannot starve newer ones.
func (enqueuer *initialBackfillEnqueuer) skippedConfigs() []string {
	enqueuer.mu.Lock()
	defer enqueuer.mu.Unlock()
	ids := make([]string, 0, len(enqueuer.seen))
	for key := range enqueuer.seen {
		ids = append(ids, key[:strings.Index(key, "|")])
	}
	return ids
}

// enqueuePending mints the initial backfill of every candidate, one
// transaction each. A failure of one candidate is logged and counted and never
// stops the others; it is retried next window while the candidate stays inside
// the age bound.
func (enqueuer *initialBackfillEnqueuer) enqueuePending(ctx context.Context, now time.Time, limit int) error {
	enqueuer.pruneSeen(now)
	candidates, err := enqueuer.candidates(ctx, now, limit)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := enqueuer.enqueueOne(ctx, now, candidate); err != nil {
			enqueuer.count(initialBackfillOutcomeFailed)
			summary := safeErrorSummary(err)
			slog.ErrorContext(ctx, "sync.initial_backfill.failed",
				slog.String("org_id", candidate.orgID), slog.String("sync_config_id", candidate.configID),
				slog.String("error", summary))
		}
	}
	return nil
}

// candidates are the configurations whose own first successful (non-backfill) run, the one their occurrence planned,
// completed inside the age bound and that have no occurrence at that instant.
func (enqueuer *initialBackfillEnqueuer) candidates(ctx context.Context, now time.Time, limit int) ([]initialBackfillCandidate, error) {
	rows, err := enqueuer.pool.Query(ctx, `
SELECT cfg.id::text, cfg.org_id, coalesce(integration.config::jsonb, '{}'::jsonb), first_run.created_at, first_run.completed_at
FROM public.sync_configurations AS cfg
JOIN public.integrations AS integration
  ON integration.id = cfg.integration_id AND integration.org_id = cfg.org_id
JOIN LATERAL (
  SELECT run.created_at, run.completed_at
  FROM public.sync_runs AS run
  JOIN public.scheduled_sync_occurrences AS owner
    ON owner.sync_run_id = run.id AND owner.sync_config_id = cfg.id
  WHERE run.org_id = cfg.org_id AND run.integration_id = cfg.integration_id
    AND run.status = 'success' AND run.triggered_by <> 'backfill' AND run.completed_at IS NOT NULL
  ORDER BY run.completed_at, run.id
  LIMIT 1
) AS first_run ON TRUE
WHERE cfg.integration_id IS NOT NULL
  AND first_run.completed_at >= $1
  AND first_run.created_at >= $3
  AND NOT (cfg.id::text = ANY($4::text[]))
  AND NOT EXISTS (
    SELECT 1 FROM public.sync_runs AS earlier
    WHERE earlier.org_id = cfg.org_id AND earlier.integration_id = cfg.integration_id AND earlier.created_at < $3)
  AND NOT EXISTS (
    SELECT 1 FROM public.scheduled_sync_occurrences AS occurrence
    WHERE occurrence.sync_config_id = cfg.id AND occurrence.scheduled_for = date_trunc('microseconds', first_run.completed_at))
ORDER BY first_run.completed_at, cfg.id
LIMIT $2`, now.Add(-enqueuer.maxAge), limit, enqueuer.notBefore, enqueuer.skippedConfigs())
	if err != nil {
		return nil, fmt.Errorf("list initial backfill candidates: %w", err)
	}
	defer rows.Close()
	var out []initialBackfillCandidate
	for rows.Next() {
		var candidate initialBackfillCandidate
		if err := rows.Scan(&candidate.configID, &candidate.orgID, &candidate.integrationOpt, &candidate.runCreatedAt, &candidate.completedAt); err != nil {
			return nil, fmt.Errorf("scan an initial backfill candidate: %w", err)
		}
		out = append(out, candidate)
	}
	return out, rows.Err()
}

func initialBackfillHorizonDays() int {
	if raw, ok := os.LookupEnv(initialBackfillHorizonEnv); ok {
		if value, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && value > 0 {
			return value
		}
		warnUnparseableEnvInt(initialBackfillHorizonEnv, raw, initialBackfillDefaultHorizon)
	}
	return initialBackfillDefaultHorizon
}

// initialBackfillWindow is the history older than the initial window: from the
// horizon (the tier cap, bounded by the configured horizon) up to the instant
// the initial window starts. ok is false when the cap does not reach past it.
func initialBackfillWindow(runCreatedAt time.Time, depthDays, horizonDays int, tierCap *int) (since, before time.Time, ok bool) {
	if tierCap != nil && *tierCap < horizonDays {
		horizonDays = *tierCap
	}
	if horizonDays <= depthDays {
		return time.Time{}, time.Time{}, false
	}
	created := runCreatedAt.UTC()
	return created.AddDate(0, 0, -horizonDays), created.AddDate(0, 0, -depthDays), true
}

func (enqueuer *initialBackfillEnqueuer) skip(ctx context.Context, candidate initialBackfillCandidate, reason string) error {
	if enqueuer.countSkipOnce(candidate, reason) {
		slog.InfoContext(ctx, "sync.initial_backfill.skipped",
			slog.String("org_id", candidate.orgID), slog.String("sync_config_id", candidate.configID),
			slog.String("reason", reason))
	}
	return nil
}

func (enqueuer *initialBackfillEnqueuer) enqueueOne(ctx context.Context, now time.Time, candidate initialBackfillCandidate) error {
	tx, err := enqueuer.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin initial backfill: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	config, err := synchandoff.LoadConfig(ctx, tx, candidate.configID)
	if err != nil {
		return fmt.Errorf("load the configuration: %w", err)
	}
	switch {
	case config == nil || config.OrgID != candidate.orgID:
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipConfigGone)
	case !config.IsActive:
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipInactive)
	case !config.PlannerManaged || config.SourceID != nil || config.IntegrationID == nil:
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipNotPlanner)
	}
	tierCap, _, err := loadPlanLimits(ctx, tx, config.OrgID, 0)
	if errors.Is(err, ErrOccurrenceIneligible) {
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipOrgIneligible)
	}
	if err != nil {
		return err
	}
	var integration integrationOptions
	if len(candidate.integrationOpt) > 0 {
		if decodeErr := json.Unmarshal(candidate.integrationOpt, &integration); decodeErr != nil {
			return fmt.Errorf("decode the integration options: %w", decodeErr)
		}
	}
	var integrationDepth *int
	if integration.InitialSyncDepth > 0 {
		integrationDepth = &integration.InitialSyncDepth
	}
	depth := resolveInitialSyncDepth(nil, integrationDepth, tierCap)
	since, before, ok := initialBackfillWindow(candidate.runCreatedAt, depth, initialBackfillHorizonDays(), tierCap)
	if !ok {
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipCapNotAboveDepth)
	}

	scheduledFor := candidate.completedAt.UTC().Truncate(time.Microsecond)
	trigger, err := synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{
		Mode: initialBackfillMode, Since: &since, Before: &before,
		TriggeredBy: initialBackfillTriggeredBy, ScheduledFor: &scheduledFor, Caller: "initial-backfill",
	}, now)
	if err != nil {
		return fmt.Errorf("mint the backfill occurrence: %w", err)
	}
	if trigger.Existing {
		return enqueuer.skip(ctx, candidate, InitialBackfillSkipAlreadyEnqueued)
	}
	ids, err := deterministicMaterializationIDs(trigger.OccurrenceID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.backfill_jobs
	(id, org_id, sync_config_id, celery_task_id, status, since_date, before_date, total_chunks, completed_chunks, failed_chunks)
VALUES (gen_random_uuid(), $1, $2::uuid, $3, 'pending', $4, $5, 0, 0, 0)`,
		config.OrgID, config.ID, "sync_run:"+ids.SyncRunID, since.Format("2006-01-02"), before.Add(-time.Microsecond).Format("2006-01-02")); err != nil {
		return fmt.Errorf("record the backfill job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the initial backfill: %w", err)
	}
	enqueuer.count(initialBackfillOutcomeEnqueued)
	slog.InfoContext(ctx, "sync.initial_backfill.enqueued",
		slog.String("org_id", config.OrgID), slog.String("sync_config_id", config.ID),
		slog.String("occurrence_id", trigger.OccurrenceID), slog.String("sync_run_id", ids.SyncRunID),
		slog.Time("since", since), slog.Time("before", before), slog.Int("depth_days", depth))
	return nil
}

// writePrometheus emits the counter by outcome.
func (enqueuer *initialBackfillEnqueuer) writePrometheus(output io.Writer) error {
	enqueuer.mu.Lock()
	outcomes := make([]string, 0, len(enqueuer.outcomes))
	values := make(map[string]uint64, len(enqueuer.outcomes))
	for outcome, value := range enqueuer.outcomes {
		outcomes = append(outcomes, outcome)
		values[outcome] = value
	}
	enqueuer.mu.Unlock()
	sort.Strings(outcomes)
	var text strings.Builder
	text.WriteString("# HELP sync_initial_backfill_total Initial backfills by outcome: enqueued, failed, or the closed reason a first-synced configuration was skipped.\n# TYPE sync_initial_backfill_total counter\n")
	if len(outcomes) == 0 {
		text.WriteString("sync_initial_backfill_total{outcome=\"enqueued\"} 0\n")
	}
	for _, outcome := range outcomes {
		fmt.Fprintf(&text, "sync_initial_backfill_total{outcome=%q} %d\n", outcome, values[outcome])
	}
	_, err := io.WriteString(output, text.String())
	return err
}
