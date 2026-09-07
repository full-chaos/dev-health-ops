package operational

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CHAOS-5353 / CHAOS-3952: the billing completion fence, ported from
// `system_ops.py`'s `_claim/_mark/_release_billing_notification_*` helpers.
//
// The fence exists because the send is not transactional with the row. A
// read-then-send-then-write ordering leaves two windows open: two concurrent
// attempts can both observe completed_at IS NULL and both send, and a crash
// between a successful send and the write lets a retry duplicate it. Claiming
// FIRST, via a single atomic `UPDATE ... WHERE claimed_at IS NULL`, makes the
// decision itself atomic -- the UPDATE's row count, not a separate read, says
// who won -- and closes both.
//
// The residual risk moves rather than disappears: a crash strictly between the
// claim committing and the send starting now SKIPS the email instead of
// duplicating it. For a billing email that is the intended trade (a rare
// silent miss over a rare duplicate), and it is deliberately observable rather
// than invisible: claimed_at and completed_at are separate columns precisely
// so "claimed but never completed" is a queryable state, surfaced by
// FenceOutcomeStaleClaim below rather than masquerading as a duplicate.

// StaleClaimThreshold is the age past which a claim with no completion is
// reported as stale rather than treated as an ordinary in-flight duplicate.
// River's backoff for this kind tops out well under it, so a claim still
// uncompleted past this window is far more likely a crashed attempt than a
// slow one. Nothing reaps or auto-resends on it -- it is surfaced for an
// operator, exactly as the Python implementation left it.
const StaleClaimThreshold = 900 * time.Second

// FenceOutcome is the closed vocabulary of billing-fence results, carried on
// every billing log line as claim_outcome. It is the same set the retired
// Python `BILLING_NOTIFICATION_COMPLETION_FENCE_TOTAL` counter labelled, so
// operational queries written against those labels still read the same words.
type FenceOutcome string

const (
	// FenceOutcomeSent is the ordinary success: claim won, email sent,
	// completed_at recorded.
	FenceOutcomeSent FenceOutcome = "sent"
	// FenceOutcomeSentFenceWriteFailed means the email WAS sent but the
	// completed_at write failed. Never released and never retried -- either
	// would duplicate a delivered email. It resurfaces later as a stale claim.
	FenceOutcomeSentFenceWriteFailed FenceOutcome = "sent_fence_write_failed"
	// FenceOutcomeDuplicateSuppressed means the claim was already held by a
	// completed or still in-flight attempt; no send was made.
	FenceOutcomeDuplicateSuppressed FenceOutcome = "duplicate_suppressed"
	// FenceOutcomeStaleClaim means a claim is held, uncompleted, and older
	// than StaleClaimThreshold. We do NOT know whether the email went out, so
	// this is reported as its own outcome rather than as a success.
	FenceOutcomeStaleClaim FenceOutcome = "stale_claim_detected"
	// FenceOutcomeKeyMismatch means the queue arguments' idempotency key
	// disagreed with the durable row's; dropped before any claim attempt.
	FenceOutcomeKeyMismatch FenceOutcome = "key_mismatch"
	// FenceOutcomePermanentDrop means the claim was WON and then permanently
	// dropped (malformed stored attributes, unknown email type, invalid
	// org id); the claim was released. Operationally the most interesting
	// case: a row was claimed and nothing was ever sent for it.
	FenceOutcomePermanentDrop FenceOutcome = "permanent_drop"
	// FenceOutcomeReleasedForRetry means a transient failure (owner lookup or
	// the provider) happened under a held claim: nothing was sent, the claim
	// was released, and the job retries. Distinct from permanent_drop, which
	// never retries, and emphatically not "sent".
	FenceOutcomeReleasedForRetry FenceOutcome = "released_for_retry"
	// FenceOutcomeNoOwner means the organization has no owner membership to
	// address, so there was nothing to send. Python returned "sent" here and
	// recorded completion; this names that case instead of hiding it inside
	// the ordinary success count.
	FenceOutcomeNoOwner FenceOutcome = "no_owner"
)

// ClaimResult describes one claim attempt. Claimed true means this caller won
// and must send. Otherwise ClaimedAt/CompletedAt describe the row AS OF the
// failed claim, so the caller can tell a genuine prior success (CompletedAt
// set) from an unresolved claim (ClaimedAt set, CompletedAt still NULL).
type ClaimResult struct {
	Claimed     bool
	ClaimedAt   *time.Time
	CompletedAt *time.Time
}

// Stale reports whether a lost claim is old enough to be treated as abandoned.
func (result ClaimResult) Stale(now time.Time) bool {
	return result.ClaimedAt != nil && result.CompletedAt == nil &&
		now.Sub(*result.ClaimedAt) > StaleClaimThreshold
}

// BillingFence owns the claim/complete/release writes for one billing row.
type BillingFence interface {
	Claim(ctx context.Context, notificationID string, now time.Time) (ClaimResult, error)
	MarkCompleted(ctx context.Context, notificationID string, now time.Time) error
	// ReleaseClaim undoes a claim this attempt made but never delivered on.
	// It reports its own failure rather than raising over the caller's
	// original error; see the handler for why that distinction matters.
	ReleaseClaim(ctx context.Context, notificationID string) error
}

// Claim atomically takes the fence for one row.
//
// PostgreSQL serializes concurrent updates to the same row, so exactly one
// caller's UPDATE matches and every other matches zero. On a lost claim one
// follow-up read reports the row's current claimed_at/completed_at so the
// caller can classify why it lost.
func (store *PostgresStore) Claim(
	ctx context.Context, notificationID string, now time.Time,
) (ClaimResult, error) {
	if store == nil || store.pool == nil {
		return ClaimResult{}, errors.New("billing store is unavailable")
	}
	parsed, err := uuid.Parse(notificationID)
	if err != nil || parsed.String() != notificationID {
		return ClaimResult{}, ErrDeliveryInvalid
	}
	tag, err := store.pool.Exec(ctx, `
		UPDATE public.billing_notifications SET claimed_at = $2
		WHERE id = $1 AND claimed_at IS NULL`, parsed, now.UTC())
	if err != nil {
		return ClaimResult{}, errors.New("billing claim is unavailable")
	}
	if tag.RowsAffected() == 1 {
		claimedAt := now.UTC()
		return ClaimResult{Claimed: true, ClaimedAt: &claimedAt}, nil
	}
	var claimedAt, completedAt *time.Time
	err = store.pool.QueryRow(ctx, `
		SELECT claimed_at, completed_at FROM public.billing_notifications
		WHERE id = $1`, parsed,
	).Scan(&claimedAt, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// The row vanished between the load and the claim. Nothing to send.
		return ClaimResult{}, ErrDeliveryNotFound
	}
	if err != nil {
		return ClaimResult{}, errors.New("billing claim is unavailable")
	}
	return ClaimResult{ClaimedAt: claimedAt, CompletedAt: completedAt}, nil
}

// MarkCompleted records that the send this attempt claimed actually went out.
// Only called after the send returned without error. Separate from the claim
// so a claim with no matching completion stays a queryable fact.
func (store *PostgresStore) MarkCompleted(
	ctx context.Context, notificationID string, now time.Time,
) error {
	if store == nil || store.pool == nil {
		return errors.New("billing store is unavailable")
	}
	parsed, err := uuid.Parse(notificationID)
	if err != nil || parsed.String() != notificationID {
		return ErrDeliveryInvalid
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE public.billing_notifications SET completed_at = $2
		WHERE id = $1`, parsed, now.UTC()); err != nil {
		return errors.New("billing completion write is unavailable")
	}
	return nil
}

// ReleaseClaim restores claimed_at to NULL so a later attempt can claim and
// actually send. Called from every failure exit after a claim is held, and
// never after a successful send.
func (store *PostgresStore) ReleaseClaim(ctx context.Context, notificationID string) error {
	if store == nil || store.pool == nil {
		return errors.New("billing store is unavailable")
	}
	parsed, err := uuid.Parse(notificationID)
	if err != nil || parsed.String() != notificationID {
		return ErrDeliveryInvalid
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE public.billing_notifications SET claimed_at = NULL
		WHERE id = $1`, parsed); err != nil {
		return errors.New("billing claim release is unavailable")
	}
	return nil
}

// OwnerLookup resolves the organization owner one notification addresses.
type OwnerLookup interface {
	// LoadOrgOwner returns the owner contact, or ErrOrgOwnerNotFound when the
	// organization has no owner membership.
	LoadOrgOwner(ctx context.Context, orgID string) (OwnerContact, error)
}

// ErrOrgOwnerNotFound means the organization has no owner membership to
// address. Python logged a warning and returned without sending; this names
// the same condition so the handler can do so deliberately rather than by
// falling through an empty address.
var ErrOrgOwnerNotFound = errors.New("operational billing organization has no owner")

// LoadOrgOwner ports `billing_emails.get_org_owner_email`: the earliest OWNER
// membership for the organization, plus the organization's own name.
func (store *PostgresStore) LoadOrgOwner(ctx context.Context, orgID string) (OwnerContact, error) {
	if store == nil || store.pool == nil {
		return OwnerContact{}, errors.New("billing store is unavailable")
	}
	parsed, err := uuid.Parse(orgID)
	if err != nil || parsed.String() != orgID {
		return OwnerContact{}, ErrDeliveryInvalid
	}
	var email string
	var fullName *string
	err = store.pool.QueryRow(ctx, `
		SELECT u.email, u.full_name
		FROM public.users u
		JOIN public.memberships m ON m.user_id = u.id
		WHERE m.org_id = $1 AND m.role = 'owner'
		ORDER BY m.created_at
		LIMIT 1`, parsed,
	).Scan(&email, &fullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return OwnerContact{}, ErrOrgOwnerNotFound
	}
	if err != nil {
		return OwnerContact{}, errors.New("billing owner lookup is unavailable")
	}
	// Python's fallbacks: a NULL/empty full_name greets "there", and a
	// missing organization row renders an empty org name rather than failing.
	name := "there"
	if fullName != nil && *fullName != "" {
		name = *fullName
	}
	var orgName *string
	err = store.pool.QueryRow(ctx,
		`SELECT name FROM public.organizations WHERE id = $1`, parsed).Scan(&orgName)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return OwnerContact{}, errors.New("billing organization lookup is unavailable")
	}
	contact := OwnerContact{Email: email, FullName: name}
	if orgName != nil {
		contact.OrgName = *orgName
	}
	return contact, nil
}

// Compile-time proof the production store satisfies both new interfaces.
var (
	_ BillingFence = (*PostgresStore)(nil)
	_ OwnerLookup  = (*PostgresStore)(nil)
)
