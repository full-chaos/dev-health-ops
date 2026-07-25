package sync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrOccurrenceConflict means a deterministic occurrence identity is
	// already bound to different scheduling inputs.
	ErrOccurrenceConflict = errors.New("scheduled sync occurrence conflicts with persisted inputs")
)

// OccurrenceCoordinator persists the language-neutral scheduled planning
// handoff. It deliberately does not plan a SyncRun: the active Python
// coordinator consumes the same row while Celery remains schedule owner.
type OccurrenceCoordinator struct{}

// NewOccurrenceCoordinator constructs the dormant PostgreSQL coordinator.
func NewOccurrenceCoordinator() Coordinator {
	return OccurrenceCoordinator{}
}

// Handoff inserts or verifies the occurrence through the scheduler's locking
// transaction. A matching row is an idempotent success.
func (OccurrenceCoordinator) Handoff(
	ctx context.Context,
	transaction HandoffTransaction,
	occurrence Occurrence,
) error {
	if ctx == nil || transaction == nil || occurrence.ID == "" ||
		occurrence.IdentityVersion != OccurrenceIdentityVersion ||
		occurrence.ConfigID == "" || occurrence.OrgID == "" ||
		occurrence.JobID == "" || occurrence.ScheduledFor.IsZero() ||
		occurrence.ObservedAt.IsZero() {
		return ErrInvalidTransactionRequest
	}
	var insertedID string
	err := transaction.QueryRow(
		ctx,
		schedulerInsertOccurrenceSQL,
		occurrence.ID,
		occurrence.IdentityVersion,
		occurrence.OrgID,
		occurrence.ConfigID,
		occurrence.JobID,
		occurrence.ScheduledFor.UTC(),
		occurrence.ObservedAt.UTC(),
	).Scan(&insertedID)
	if err == nil {
		if insertedID != occurrence.ID {
			return ErrOccurrenceConflict
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("insert scheduled sync occurrence: %w", err)
	}

	var identityVersion, orgID, configID, jobID string
	var scheduledFor time.Time
	if err := transaction.QueryRow(
		ctx,
		schedulerSelectOccurrenceSQL,
		occurrence.ID,
	).Scan(
		&identityVersion,
		&orgID,
		&configID,
		&jobID,
		&scheduledFor,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOccurrenceConflict
		}
		return fmt.Errorf("verify scheduled sync occurrence: %w", err)
	}
	if identityVersion != occurrence.IdentityVersion ||
		orgID != occurrence.OrgID ||
		configID != occurrence.ConfigID ||
		jobID != occurrence.JobID ||
		!scheduledFor.Equal(occurrence.ScheduledFor) {
		return ErrOccurrenceConflict
	}
	return nil
}

const schedulerInsertOccurrenceSQL = `
INSERT INTO public.scheduled_sync_occurrences (
    occurrence_id,
    identity_version,
    org_id,
    sync_config_id,
    scheduled_job_id,
    scheduled_for,
    created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT DO NOTHING
RETURNING occurrence_id
`

const schedulerSelectOccurrenceSQL = `
SELECT
    identity_version,
    org_id,
    sync_config_id::text,
    scheduled_job_id::text,
    scheduled_for
FROM public.scheduled_sync_occurrences
WHERE occurrence_id = $1
FOR UPDATE
`
