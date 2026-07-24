package fixed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrOccurrenceConflict identifies a persisted occurrence whose identity
	// fields disagree with the derived occurrence. It is never retried: the
	// identity derivation itself is wrong and replaying would compound it.
	ErrOccurrenceConflict = errors.New("fixed schedule occurrence conflicts with persisted identity")
	// ErrLedgerUnavailable identifies a ledger that cannot be read or written.
	ErrLedgerUnavailable = errors.New("fixed schedule occurrence ledger is unavailable")
)

// Occurrence ledger statuses.
const (
	// OccurrenceMaterialized means the producer persisted at least one durable
	// handoff for this occurrence.
	OccurrenceMaterialized = "materialized"
	// OccurrenceSkipped means the producer found no work. The occurrence is
	// still durable so the due time is never retried as if it were missed.
	OccurrenceSkipped = "skipped"
)

// ClaimResult reports what one claim attempt did.
type ClaimResult string

const (
	// ClaimInserted means this replica won the occurrence.
	ClaimInserted ClaimResult = "inserted"
	// ClaimDuplicate means another replica or an earlier tick already owns it.
	ClaimDuplicate ClaimResult = "duplicate"
)

// Ledger is the durable fixed-schedule occurrence record. It is the identity
// of record: River retention must never be the only thing keeping two replicas
// from producing the same due time twice.
//
// Every method takes the caller's transaction. The claim, the producer's
// domain writes, and the completion update all commit together or not at all.
type Ledger interface {
	// Claim inserts the occurrence identity. A duplicate insert verifies the
	// persisted identity and reports ClaimDuplicate.
	Claim(ctx context.Context, tx pgx.Tx, occurrence Occurrence) (ClaimResult, error)
	// Complete records the producer outcome for a claimed occurrence.
	Complete(ctx context.Context, tx pgx.Tx, occurrence Occurrence, status string, handoffs int, skipReason string) error
	// LastOccurrence returns the newest recorded occurrence for a schedule.
	// A false second result means no occurrence exists.
	LastOccurrence(ctx context.Context, tx pgx.Tx, scheduleID string) (Anchor, bool, error)
}

// PostgresLedger is the production ledger.
type PostgresLedger struct{}

// NewPostgresLedger constructs the PostgreSQL occurrence ledger.
func NewPostgresLedger() Ledger { return PostgresLedger{} }

// Claim persists one occurrence identity exactly once.
//
// Two scheduler replicas racing the same due time both reach this statement.
// The primary key serializes them: the loser's INSERT returns no row after the
// winner commits, and the verification read proves the persisted identity is
// the same occurrence rather than a hash collision or an identity-version
// change. That is the whole replica-safety mechanism; nothing downstream needs
// its own lock.
func (PostgresLedger) Claim(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
) (ClaimResult, error) {
	if ctx == nil || tx == nil {
		return "", ErrLedgerUnavailable
	}
	if err := validateOccurrence(occurrence); err != nil {
		return "", err
	}
	var claimed string
	err := tx.QueryRow(
		ctx,
		insertOccurrenceSQL,
		occurrence.Key,
		occurrence.IdentityVersion,
		occurrence.ScheduleID,
		occurrence.TargetKind,
		occurrence.ScheduledFor,
		occurrence.ObservedAt,
	).Scan(&claimed)
	if err == nil {
		if claimed != occurrence.Key {
			return "", ErrOccurrenceConflict
		}
		return ClaimInserted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("claim fixed schedule occurrence: %w", err)
	}

	var identityVersion, scheduleID, targetKind string
	var scheduledFor time.Time
	if err := tx.QueryRow(ctx, selectOccurrenceSQL, occurrence.Key).Scan(
		&identityVersion,
		&scheduleID,
		&targetKind,
		&scheduledFor,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The insert was skipped but no row exists: the unique constraint
			// on (schedule_id, scheduled_for) rejected a different key for the
			// same due time, which means the identity derivation changed.
			return "", ErrOccurrenceConflict
		}
		return "", fmt.Errorf("verify fixed schedule occurrence: %w", err)
	}
	if identityVersion != occurrence.IdentityVersion ||
		scheduleID != occurrence.ScheduleID ||
		targetKind != occurrence.TargetKind ||
		!scheduledFor.Equal(occurrence.ScheduledFor) {
		return "", ErrOccurrenceConflict
	}
	return ClaimDuplicate, nil
}

// Complete records the producer outcome. It requires the row to still be
// unfinished so a late writer cannot overwrite a committed result.
func (PostgresLedger) Complete(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
	status string,
	handoffs int,
	skipReason string,
) error {
	if ctx == nil || tx == nil || handoffs < 0 {
		return ErrLedgerUnavailable
	}
	if err := validateOccurrence(occurrence); err != nil {
		return err
	}
	switch status {
	case OccurrenceMaterialized:
		if handoffs < 1 {
			return fmt.Errorf("%w: materialized occurrence has no handoff", ErrLedgerUnavailable)
		}
		skipReason = ""
	case OccurrenceSkipped:
		if handoffs != 0 {
			return fmt.Errorf("%w: skipped occurrence recorded handoffs", ErrLedgerUnavailable)
		}
		if skipReason == "" {
			return fmt.Errorf("%w: skipped occurrence has no reason", ErrLedgerUnavailable)
		}
		if len(skipReason) > 64 {
			skipReason = skipReason[:64]
		}
	default:
		return fmt.Errorf("%w: unknown occurrence status %q", ErrLedgerUnavailable, status)
	}
	var reason *string
	if skipReason != "" {
		reason = &skipReason
	}
	command, err := tx.Exec(
		ctx,
		completeOccurrenceSQL,
		status,
		handoffs,
		reason,
		occurrence.ObservedAt,
		occurrence.Key,
	)
	if err != nil {
		return fmt.Errorf("complete fixed schedule occurrence: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: occurrence %s was not claimable", ErrLedgerUnavailable, occurrence.Key)
	}
	return nil
}

// Anchor is the newest durable occurrence for one schedule. It carries both the
// canonical due time and the instant that occurrence was observed, because
// those answer different questions: the due time says which grid point is
// already owned, and the observation instant says when this schedule actually
// began running. An interval cadence needs the second one, since its grid
// points carry no meaning of their own.
type Anchor struct {
	ScheduledFor time.Time
	ObservedAt   time.Time
}

// LastOccurrence supports both due-ness anchoring and missed-occurrence
// alerting without replaying work.
func (PostgresLedger) LastOccurrence(
	ctx context.Context,
	tx pgx.Tx,
	scheduleID string,
) (Anchor, bool, error) {
	if ctx == nil || tx == nil || scheduleID == "" {
		return Anchor{}, false, ErrLedgerUnavailable
	}
	var anchor Anchor
	if err := tx.QueryRow(ctx, selectLastOccurrenceSQL, scheduleID).Scan(
		&anchor.ScheduledFor, &anchor.ObservedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Anchor{}, false, nil
		}
		return Anchor{}, false, fmt.Errorf("read last fixed schedule occurrence: %w", err)
	}
	anchor.ScheduledFor = anchor.ScheduledFor.UTC()
	anchor.ObservedAt = anchor.ObservedAt.UTC()
	return anchor, true, nil
}

func validateOccurrence(occurrence Occurrence) error {
	if occurrence.Key == "" || occurrence.IdentityVersion != OccurrenceIdentityVersion ||
		occurrence.ScheduleID == "" || occurrence.TargetKind == "" ||
		occurrence.ScheduledFor.IsZero() || occurrence.ObservedAt.IsZero() {
		return fmt.Errorf("%w: incomplete occurrence", ErrLedgerUnavailable)
	}
	return nil
}

const insertOccurrenceSQL = `
INSERT INTO public.fixed_schedule_occurrences (
    occurrence_key,
    identity_version,
    schedule_id,
    target_kind,
    scheduled_for,
    observed_at,
    status,
    handoff_count,
    created_at,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, 'claimed', 0, $6, $6)
ON CONFLICT DO NOTHING
RETURNING occurrence_key
`

const selectOccurrenceSQL = `
SELECT identity_version, schedule_id, target_kind, scheduled_for
FROM public.fixed_schedule_occurrences
WHERE occurrence_key = $1
FOR UPDATE
`

const completeOccurrenceSQL = `
UPDATE public.fixed_schedule_occurrences
SET status = $1,
    handoff_count = $2,
    skip_reason = $3,
    completed_at = $4,
    updated_at = $4
WHERE occurrence_key = $5
  AND status = 'claimed'
`

const selectLastOccurrenceSQL = `
SELECT scheduled_for, observed_at
FROM public.fixed_schedule_occurrences
WHERE schedule_id = $1
ORDER BY scheduled_for DESC
LIMIT 1
`
