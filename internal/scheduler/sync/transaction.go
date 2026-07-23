package sync

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// OccurrenceIdentityVersion is the stable framing used to deduplicate one
	// config's cron occurrence across scheduler replicas and retries.
	OccurrenceIdentityVersion = "sync_scheduler_occurrence_v1"
)

var (
	// ErrInvalidTransactionRequest identifies a caller contract violation.
	ErrInvalidTransactionRequest = errors.New("invalid scheduler transaction request")
	// ErrScheduleMarkerLost identifies a locked marker that disappeared before
	// the atomic handoff could advance it.
	ErrScheduleMarkerLost = errors.New("scheduler schedule marker was lost")
)

// Occurrence is the deterministic handoff envelope for one due schedule. It
// contains timing identity only. Organization existence, entitlement, target
// routing, and business payload construction remain coordinator obligations.
type Occurrence struct {
	ID              string
	IdentityVersion string
	ConfigID        string
	OrgID           string
	JobID           string
	ScheduledFor    time.Time
	ObservedAt      time.Time
	NextRunAt       time.Time
}

// HandoffTransaction is the same PostgreSQL transaction that protects the
// locked schedule marker. Coordinators must persist a durable handoff through
// this transaction; a remote or otherwise non-transactional side effect does
// not satisfy the atomic handoff contract.
type HandoffTransaction interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Coordinator owns every non-timing eligibility decision, including
// organization existence and feature entitlement, then persists the durable
// handoff. Returning nil means the authorized handoff is present in the
// supplied transaction and permits the kernel to advance the schedule marker.
type Coordinator interface {
	Handoff(context.Context, HandoffTransaction, Occurrence) error
}

// CoordinatorFunc adapts a function to Coordinator.
type CoordinatorFunc func(context.Context, HandoffTransaction, Occurrence) error

func (function CoordinatorFunc) Handoff(
	ctx context.Context,
	tx HandoffTransaction,
	occurrence Occurrence,
) error {
	if function == nil {
		return ErrInvalidTransactionRequest
	}
	return function(ctx, tx, occurrence)
}

type lockedCandidate struct {
	orgID     string
	candidate Candidate
}

type lockedCandidateRows interface {
	candidateRows
	Close()
}

type schedulerTransaction interface {
	HandoffTransaction
	queryCandidates(context.Context, string, ...any) (lockedCandidateRows, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type beginSchedulerTransaction func(context.Context) (schedulerTransaction, error)

type postgresSchedulerTransaction struct{ pgx.Tx }

func (transaction postgresSchedulerTransaction) queryCandidates(
	ctx context.Context,
	statement string,
	args ...any,
) (lockedCandidateRows, error) {
	return transaction.Query(ctx, statement, args...)
}

// HandoffDue locks and re-evaluates one bounded candidate window, persists each
// coordinator handoff, and only then advances its schedule marker. All changes
// commit or roll back together. It is intentionally not called by the dormant
// scheduler command.
//
// The kernel requires an existing sync ScheduledJob marker. Creating missing
// markers requires configuration and entitlement policy that is deliberately
// outside this timing-only package.
func (repository *Repository) HandoffDue(
	ctx context.Context,
	observedAt time.Time,
	limit int,
	coordinator Coordinator,
) ([]Occurrence, error) {
	if ctx == nil || observedAt.IsZero() || limit < minimumSnapshotLimit ||
		limit > maximumSnapshotLimit || coordinator == nil ||
		repository == nil || repository.begin == nil {
		return nil, ErrInvalidTransactionRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	transaction, err := repository.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scheduler transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	rows, err := transaction.queryCandidates(
		ctx,
		schedulerHandoffCandidatesSQL,
		observedAt.UTC(),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("lock scheduler candidates: %w", err)
	}
	candidates, err := readLockedCandidates(ctx, rows, limit)
	rows.Close()
	if err != nil {
		return nil, err
	}

	handedOff := make([]Occurrence, 0, len(candidates))
	for _, locked := range candidates {
		evaluation, err := evaluateContext(ctx, locked.candidate, observedAt)
		if err != nil {
			return nil, err
		}
		if !evaluation.TimingEligible || evaluation.NextOccurrence == nil ||
			locked.candidate.Job == nil {
			continue
		}
		nextRunAt, _, err := nextOccurrenceContext(
			ctx,
			locked.candidate.Job.ScheduleCron,
			observedAt.UTC(),
			locked.candidate.Job.Timezone,
		)
		if err != nil {
			return nil, fmt.Errorf("compute next schedule marker for config %s: %w", locked.candidate.ConfigID, err)
		}
		occurrence := newOccurrence(
			locked.candidate.ConfigID,
			locked.orgID,
			locked.candidate.Job.ID,
			*evaluation.NextOccurrence,
			observedAt,
			nextRunAt,
		)
		if err := coordinator.Handoff(ctx, transaction, occurrence); err != nil {
			return nil, fmt.Errorf("handoff scheduler occurrence %s: %w", occurrence.ID, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		command, err := transaction.Exec(
			ctx,
			schedulerAdvanceMarkerSQL,
			occurrence.NextRunAt,
			occurrence.ObservedAt,
			occurrence.JobID,
		)
		if err != nil {
			return nil, fmt.Errorf("advance scheduler marker for config %s: %w", occurrence.ConfigID, err)
		}
		if command.RowsAffected() != 1 {
			return nil, ErrScheduleMarkerLost
		}
		handedOff = append(handedOff, occurrence)
	}

	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scheduler transaction: %w", err)
	}
	return handedOff, nil
}

func readLockedCandidates(
	ctx context.Context,
	rows lockedCandidateRows,
	capacity int,
) ([]lockedCandidate, error) {
	candidates := make([]lockedCandidate, 0, capacity)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(candidates) >= capacity {
			return nil, fmt.Errorf("scheduler locked candidate window exceeded limit")
		}
		var locked lockedCandidate
		var job Job
		var configCron, configTimezone string
		if err := rows.Scan(
			&locked.candidate.ConfigID,
			&locked.orgID,
			&locked.candidate.Active,
			&configCron,
			&configTimezone,
			&locked.candidate.LastSyncAt,
			&locked.candidate.CreatedAt,
			&job.ID,
			&job.ScheduleCron,
			&job.Timezone,
			&job.Status,
			&job.IsRunning,
			&job.LastRunAt,
			&job.UpdatedAt,
			&job.NextRunAt,
		); err != nil {
			return nil, err
		}
		locked.candidate.ScheduleCron = configCron
		locked.candidate.ScheduleTZ = configTimezone
		locked.candidate.Job = &job
		candidates = append(candidates, locked)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func newOccurrence(
	configID, orgID, jobID string,
	scheduledFor, observedAt, nextRunAt time.Time,
) Occurrence {
	scheduledFor = scheduledFor.UTC()
	hasher := sha256.New()
	writeDigestField(hasher, "identity_version", OccurrenceIdentityVersion)
	writeDigestField(hasher, "config_id", configID)
	writeDigestField(hasher, "scheduled_for", canonicalTime(scheduledFor))
	return Occurrence{
		ID:              "sha256:" + fmt.Sprintf("%x", hasher.Sum(nil)),
		IdentityVersion: OccurrenceIdentityVersion,
		ConfigID:        configID,
		OrgID:           orgID,
		JobID:           jobID,
		ScheduledFor:    scheduledFor,
		ObservedAt:      observedAt.UTC(),
		NextRunAt:       nextRunAt.UTC(),
	}
}

// The inner join deliberately excludes configs without a persisted sync marker.
// Both rows are locked so multiple scheduler replicas cannot hand off the same
// occurrence. SQL gates reduce lock contention; evaluateContext is the final
// source of timing truth after the locks are held.
const schedulerHandoffCandidatesSQL = `
SELECT
    config.id::text,
    config.org_id,
    config.is_active,
    config.sync_options->>'schedule_cron',
    COALESCE(config.sync_options->>'timezone', ''),
    config.last_sync_at,
    config.created_at,
    job.id::text,
    job.schedule_cron,
    job.timezone,
    job.status,
    job.is_running,
    job.last_run_at,
    job.updated_at,
    job.next_run_at
FROM public.sync_configurations AS config
JOIN public.scheduled_jobs AS job
    ON job.org_id = config.org_id
    AND job.sync_config_id = config.id
    AND job.job_type = 'sync'
WHERE config.is_active = TRUE
    AND COALESCE(config.sync_options->>'schedule_cron', '') <> ''
    AND job.status = 0
    AND (job.next_run_at IS NULL OR job.next_run_at <= $1)
    AND (
        job.is_running = FALSE
        OR COALESCE(job.last_run_at, job.updated_at) IS NULL
        OR COALESCE(job.last_run_at, job.updated_at) < $1 - INTERVAL '2 hours'
    )
ORDER BY COALESCE(job.next_run_at, config.last_sync_at, config.created_at), config.id
FOR UPDATE OF config, job SKIP LOCKED
LIMIT $2
`

const schedulerAdvanceMarkerSQL = `
UPDATE public.scheduled_jobs
SET next_run_at = $1, updated_at = $2
WHERE id = $3
`
