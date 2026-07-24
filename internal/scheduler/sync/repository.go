package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository reads the legacy scheduler tables for shadow comparison and
// exposes a separate dormant transaction kernel. Snapshot never locks or
// writes.
type Repository struct {
	pool      *pgxpool.Pool
	begin     beginSchedulerTransaction
	ownership OwnershipPolicy
}

type candidateRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	return newRepositoryWithOwnership(pool, DefaultOwnershipPolicy())
}

// NewMutationRepository is the source-reviewed production composition seam
// for transferring schedule-marker ownership to Go. Calling it is not an
// environment-level activation: the scheduler command additionally requires
// its checked-in ownership and coordinator-parity gates before this repository
// can be constructed.
func NewMutationRepository(pool *pgxpool.Pool) (*Repository, error) {
	return newRepositoryWithOwnership(pool, reviewedGoMutationOwnershipPolicy())
}

// newRepositoryWithOwnership constructs the scheduler repository with an
// explicit, validated owner/mode policy. Keeping this package-private makes a
// future mutation activation require an audited source change in this package.
func newRepositoryWithOwnership(pool *pgxpool.Pool, ownership OwnershipPolicy) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("scheduler shadow repository requires a pool")
	}
	if err := ownership.Validate(); err != nil {
		return nil, err
	}
	return &Repository{
		pool:      pool,
		ownership: ownership,
		begin: func(ctx context.Context) (schedulerTransaction, error) {
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				return nil, err
			}
			return postgresSchedulerTransaction{Tx: tx}, nil
		},
	}, nil
}

// Snapshot reads one bounded active-config window then evaluates it locally.
func (repository *Repository) Snapshot(ctx context.Context, observedAt time.Time, limit int) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, fmt.Errorf("scheduler shadow snapshot context is required")
	}
	if repository == nil || repository.pool == nil {
		return Snapshot{}, fmt.Errorf("scheduler shadow repository is not initialized")
	}
	if limit < minimumSnapshotLimit || limit > maximumSnapshotLimit {
		return Snapshot{}, fmt.Errorf("snapshot limit must be between %d and %d", minimumSnapshotLimit, maximumSnapshotLimit)
	}
	rows, err := repository.pool.Query(ctx, schedulerSnapshotSQL, limit+1)
	if err != nil {
		return Snapshot{}, err
	}
	defer rows.Close()
	candidates, err := readCandidates(ctx, rows, limit+1)
	if err != nil {
		return Snapshot{}, err
	}
	return buildSnapshotContext(ctx, observedAt, limit, candidates)
}

func readCandidates(ctx context.Context, rows candidateRows, capacity int) ([]Candidate, error) {
	if ctx == nil {
		return nil, fmt.Errorf("scheduler shadow row-scan context is required")
	}
	candidates := make([]Candidate, 0, capacity)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !rows.Next() {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var candidate Candidate
		var job Job
		var configCron, configTimezone, jobID, jobCron, jobTimezone *string
		var jobStatus *int
		var jobIsRunning *bool
		var lastSyncAt, lastRunAt, updatedAt, nextRunAt *time.Time
		if err := rows.Scan(
			&candidate.ConfigID, &candidate.Active, &configCron, &configTimezone,
			&lastSyncAt, &candidate.CreatedAt,
			&jobID, &jobCron, &jobTimezone, &jobStatus, &jobIsRunning,
			&lastRunAt, &updatedAt, &nextRunAt,
		); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		candidate.LastSyncAt = lastSyncAt
		if configCron != nil {
			candidate.ScheduleCron = *configCron
		}
		if configTimezone != nil {
			candidate.ScheduleTZ = *configTimezone
		}
		if jobID != nil {
			if jobCron == nil || jobTimezone == nil || jobStatus == nil || jobIsRunning == nil {
				return nil, fmt.Errorf("sync job join returned incomplete row")
			}
			job.ID, job.ScheduleCron, job.Timezone = *jobID, *jobCron, *jobTimezone
			job.Status, job.IsRunning = *jobStatus, *jobIsRunning
			job.LastRunAt, job.UpdatedAt, job.NextRunAt = lastRunAt, updatedAt, nextRunAt
			candidate.Job = &job
		}
		candidates = append(candidates, candidate)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// schedulerSnapshotSQL is tied to SyncConfiguration, ScheduledJob, and the
// PostgreSQL-only uq_scheduled_job_org_sync_config_type uniqueness migration.
// The join reproduces the Python lookup exactly: same org/config and job_type
// sync. It intentionally has no transaction modifiers or write statements.
const schedulerSnapshotSQL = `
SELECT
    config.id::text,
    config.is_active,
    config.sync_options->>'schedule_cron',
    config.sync_options->>'timezone',
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
LEFT JOIN public.scheduled_jobs AS job
    ON job.org_id = config.org_id
    AND job.sync_config_id = config.id
    AND job.job_type = 'sync'
WHERE config.is_active = TRUE
ORDER BY config.id
LIMIT $1
`
