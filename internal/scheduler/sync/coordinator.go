package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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
// transaction. A matching row is an idempotent success, reported as
// OccurrenceRepeated so the caller can tell a window that produced work from
// one that only re-confirmed a row an earlier window already wrote.
func (OccurrenceCoordinator) Handoff(
	ctx context.Context,
	transaction HandoffTransaction,
	occurrence Occurrence,
) (HandoffOutcome, error) {
	if ctx == nil || transaction == nil || occurrence.ID == "" ||
		occurrence.IdentityVersion != OccurrenceIdentityVersion ||
		occurrence.ConfigID == "" || occurrence.OrgID == "" ||
		occurrence.JobID == "" || occurrence.ScheduledFor.IsZero() ||
		occurrence.ObservedAt.IsZero() {
		return "", ErrInvalidTransactionRequest
	}
	// Non-timing eligibility, which this type's interface doc names as its own
	// (transaction.go, Coordinator): the timing kernel refuses to do business
	// lookups, so if these are not done here they are not done before minting at
	// all. Both mirror a Python guard that ran BEFORE anything was written.
	eligible, refusal, err := coordinatorEligibility(ctx, transaction, occurrence)
	if err != nil {
		return "", err
	}
	if !eligible {
		return refusal, nil
	}

	var insertedID string
	err = transaction.QueryRow(
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
			return "", ErrOccurrenceConflict
		}
		return OccurrenceMinted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("insert scheduled sync occurrence: %w", err)
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
			return "", ErrOccurrenceConflict
		}
		return "", fmt.Errorf("verify scheduled sync occurrence: %w", err)
	}
	if identityVersion != occurrence.IdentityVersion ||
		orgID != occurrence.OrgID ||
		configID != occurrence.ConfigID ||
		jobID != occurrence.JobID ||
		!scheduledFor.Equal(occurrence.ScheduledFor) {
		return "", ErrOccurrenceConflict
	}
	return OccurrenceRepeated, nil
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

// coordinatorEligibility applies the two pre-mint gates Python applied and the
// Go scheduler did not. It returns (true, "", nil) to proceed, or (false, the
// refusal outcome, nil) to skip.
//
// Both gates read through the scheduler's own locking transaction, so they see
// the same snapshot as the FOR UPDATE lock already held on the configuration
// and its marker. A refusal therefore cannot race a concurrent re-enable into
// minting anyway.
func coordinatorEligibility(
	ctx context.Context,
	transaction HandoffTransaction,
	occurrence Occurrence,
) (bool, HandoffOutcome, error) {
	organizationPresent, err := organizationExists(ctx, transaction, occurrence.OrgID)
	if err != nil {
		return false, "", err
	}
	if !organizationPresent {
		return false, OccurrenceRefusedOrgMissing, nil
	}

	targets, err := configuredSyncTargets(ctx, transaction, occurrence)
	if err != nil {
		return false, "", err
	}
	if !syncTargetsRequireCanonicalIncident(targets) {
		// Python only consults the feature when the targets are gated ones
		// (sync/canonical_incident_gate.py:37-42, applied at
		// workers/sync_scheduler.py:207). Consulting it unconditionally would
		// turn one feature flag into a fleet-wide scheduling outage.
		return true, "", nil
	}
	allowed, err := canonicalIncidentAllowed(ctx, transaction, occurrence.OrgID, occurrence.ObservedAt, false)
	if err != nil {
		return false, "", err
	}
	if !allowed {
		return false, OccurrenceRefusedFeatureDisabled, nil
	}
	return true, "", nil
}

// organizationExists mirrors workers/org_guard.py:14-36 branch for branch: an
// empty org_id, the literal "default", and any org_id that is not a UUID are
// all admitted WITHOUT a lookup, because Python admits them; only a UUID
// org_id is required to have a row.
//
// REACHABILITY, so the empty-org branch is not mistaken for a live path:
// Handoff rejects an occurrence with an empty OrgID before it ever calls this
// (see the validation at the top of Handoff), so via the scheduler the
// orgID == "" branch cannot be reached. It is kept because this function
// states Python's contract and callers other than Handoff must get Python's
// answer, not a subset of it -- but no test can exercise it through Handoff,
// and a reader should not go looking for one. See
// TestOrganizationGuardMirrorsPythonBranchForBranch.
//
// DIVERGENCE, deliberate. Python's guard also fails OPEN on a database error
// (org_guard.py:31-36 catches SQLAlchemyError and returns True), so an org it
// could not verify still gets scheduled. This returns the error instead. In Go
// that aborts the whole window before any marker advances, so the schedule
// stays due and the next tick retries -- nothing is lost, and refusing to mint
// for an organization we could not verify is strictly safer than minting for
// one that may already be deleted. Recorded in the parity table rather than
// left as an unremarked difference.
func organizationExists(
	ctx context.Context,
	transaction HandoffTransaction,
	orgID string,
) (bool, error) {
	if orgID == "" || orgID == "default" {
		return true, nil
	}
	if _, err := uuid.Parse(orgID); err != nil {
		return true, nil
	}
	var id string
	err := transaction.QueryRow(
		ctx,
		`SELECT id::text FROM public.organizations WHERE id = $1::uuid`,
		orgID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify scheduled sync organization: %w", err)
	}
	return true, nil
}

// configuredSyncTargets reads the schedule's legacy sync targets, which decide
// whether the canonical-incident feature applies at all.
func configuredSyncTargets(
	ctx context.Context,
	transaction HandoffTransaction,
	occurrence Occurrence,
) ([]string, error) {
	var encoded []byte
	err := transaction.QueryRow(
		ctx,
		`SELECT sync_targets::jsonb FROM public.sync_configurations
		 WHERE id = $1::uuid AND org_id = $2`,
		occurrence.ConfigID,
		occurrence.OrgID,
	).Scan(&encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		// The configuration vanished between the locked candidate read and
		// here. Treat it as ungated rather than inventing an entitlement
		// refusal; the occurrence's own downstream checks will reject it.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read scheduled sync targets: %w", err)
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	// Decoded as []any, then coerced, because Python coerces: it builds its
	// target list with `[str(target) for target in (config.sync_targets or [])]`
	// (workers/sync_scheduler.py:206), so a non-string element is stringified
	// rather than rejected. Decoding straight into []string instead would turn
	// one malformed row into a decode error, and because this runs inside the
	// window's single transaction that error aborts the WHOLE window -- every
	// other due config in it included. Python explicitly refuses that failure
	// mode, isolating each config so "one bad config ... must not abort
	// dispatch for the remaining configs" (:415-421). A malformed row must cost
	// its own schedule at most, never the fleet's.
	var decoded []any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		// Not even a JSON array. Treat it as ungated rather than failing the
		// window: the gate decides whether an ENTITLEMENT applies, and it is
		// not the right place to adjudicate malformed configuration.
		return nil, nil
	}
	targets := make([]string, 0, len(decoded))
	for _, value := range decoded {
		if value == nil {
			continue
		}
		targets = append(targets, fmt.Sprint(value))
	}
	return targets, nil
}
