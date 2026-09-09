package operational

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// BillingHandler renders and sends one billing notification natively.
//
// CHAOS-5353 replaced the HTTP bridge to Python's
// `/api/internal/worker-operational/billing` with this handler. The ordering
// it enforces -- claim, then send, then record completion -- is the CHAOS-3952
// completion fence ported verbatim from `system_ops.send_billing_notification`;
// see billingfence.go for why the claim must come first.
type BillingHandler struct {
	store  DeliveryStore
	fence  BillingFence
	owners OwnerLookup
	sender EmailSender
	// appBaseURL is captured at construction rather than read per render, so
	// a mid-flight environment change cannot make two attempts at the same
	// notification produce different links.
	appBaseURL string
	// now is injectable purely so the stale-claim threshold is testable
	// without sleeping for fifteen minutes.
	now func() time.Time
}

func NewBillingHandler(
	store DeliveryStore,
	fence BillingFence,
	owners OwnerLookup,
	sender EmailSender,
	appBaseURL string,
) (*BillingHandler, error) {
	if store == nil || fence == nil || owners == nil || sender == nil {
		return nil, errors.New("complete billing dependencies are required")
	}
	return &BillingHandler{
		store:      store,
		fence:      fence,
		owners:     owners,
		sender:     sender,
		appBaseURL: normalizeAppBaseURL(appBaseURL),
		now:        func() time.Time { return time.Now().UTC() },
	}, nil
}

func (handler *BillingHandler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.BillingNotificationArgs],
) error {
	if handler == nil || handler.store == nil || handler.fence == nil ||
		handler.owners == nil || handler.sender == nil || execution == nil {
		return jobruntime.Permanent(errors.New("billing handler is not configured"))
	}
	id := execution.Args.Payload.NotificationID
	if execution.Envelope.Domain.ID != "" && execution.Envelope.Domain.ID != id {
		slog.ErrorContext(ctx, "billing notification: envelope domain disagrees with the payload",
			"notification_id", id, "envelope_domain_id", execution.Envelope.Domain.ID,
			"claim_outcome", string(FenceOutcomePermanentDrop))
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}
	notification, err := handler.store.LoadBilling(ctx, id)
	if err != nil {
		slog.ErrorContext(ctx, "billing notification: durable row could not be loaded",
			"notification_id", id, "error", err)
		return classifyStoreError(err)
	}
	if notification.ID != id || execution.OrganizationID == nil ||
		notification.OrganizationID != *execution.OrganizationID ||
		notification.NotificationType == "" || notification.IdempotencyKey == "" {
		slog.ErrorContext(ctx, "billing notification: durable row failed its identity checks",
			"notification_id", id, "claim_outcome", string(FenceOutcomePermanentDrop))
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}

	logger := slog.With(
		"kind", "operational.billing_notification",
		"notification_id", notification.ID,
		"organization_id", notification.OrganizationID,
		"email_type", notification.NotificationType,
		"provider", handler.sender.Name(),
	)

	// Identity fence, BEFORE any claim (CHAOS-3952, restored in CHAOS-5353
	// r1). The job envelope carries its own copy of the durable row's
	// idempotency key. Two sides that disagree about which row this job is
	// for must never act: a stale, corrupted or misrouted job would
	// otherwise send the email for whichever row the payload id happened to
	// name. The key stays OPTIONAL on the envelope -- an older producer that
	// never set it degrades to "no cross-check", not to "billing mail
	// stops", which is the rolling-deploy hazard the Python bridge model
	// documented. Only a PRESENT and DIFFERENT key is a refusal.
	if envelopeKey := execution.Args.IdempotencyKey; envelopeKey != "" &&
		envelopeKey != notification.IdempotencyKey {
		// Neither key is logged: both embed the organization id and the
		// notification type. The field that tripped it is named instead.
		logger.ErrorContext(ctx,
			"billing notification: the job envelope's idempotency key disagrees with "+
				"the durable row's; refusing to send",
			"field", "idempotency_key", "claim_outcome", string(FenceOutcomeKeyMismatch))
		return jobruntime.Permanent(fmt.Errorf(
			"%w: idempotency key mismatch", ErrDeliveryInvalid))
	}

	now := handler.now()
	claim, err := handler.fence.Claim(ctx, notification.ID, now)
	if err != nil {
		if errors.Is(err, ErrDeliveryNotFound) || errors.Is(err, ErrDeliveryInvalid) {
			logger.ErrorContext(ctx, "billing notification: the row vanished before it could be claimed",
				"error", err, "claim_outcome", string(FenceOutcomePermanentDrop))
			return jobruntime.Permanent(err)
		}
		logger.ErrorContext(ctx, "billing notification: the completion fence could not be claimed",
			"error", err)
		return jobruntime.Retryable(err)
	}
	if !claim.Claimed {
		return handler.reportLostClaim(ctx, logger, claim, now)
	}

	// Everything from here to the send runs with a claim HELD. Every exit
	// that knows nothing was sent -- error or deliberate drop -- must
	// release it, or a retry meeting the still-fresh, unresolved claim would
	// report a duplicate for an email that was never attempted. `deliver`
	// funnels all of them through one return value so no path can skip the
	// release below. The one deliberate exception is FenceOutcomeAmbiguous
	// (CHAOS-5399): there, whether something was sent is exactly what is NOT
	// known, and releasing would risk the opposite mistake -- a retry
	// duplicating a message that already went out.
	sent, outcome, deliverErr := handler.deliver(ctx, logger, notification)
	if deliverErr != nil {
		if outcome == FenceOutcomeAmbiguous {
			// CHAOS-5399: do NOT release. The provider result is genuinely
			// unknown -- the message may already be out -- so releasing
			// would let a retry send a duplicate exactly the way an
			// unresolved crash would. Retaining the claim means a later
			// attempt meets a still-held, uncompleted claim and
			// reportLostClaim suppresses it as a duplicate instead of
			// sending again.
			//
			// CHAOS-5399 r1 (codex P1): plain Retryable is wrong here. This
			// job kind's max_attempts is 4 (contracts/jobs/v1/registry.json),
			// with bounded_exponential_jitter backoff that finishes in well
			// under StaleClaimThreshold (15m) -- an ordinary Retryable
			// return would burn attempts 2-4 doing nothing but bouncing off
			// the duplicate_suppressed branch, River would then discard the
			// job entirely, and NOTHING would ever run Work() for this
			// notification again: the claim stays held forever with no
			// automated path to the staleness alert at all. RetryableAfter
			// schedules the one follow-up attempt to land AFTER
			// StaleClaimThreshold has genuinely elapsed, so it reaches
			// FenceOutcomeStaleClaim (Permanent, operator-visible)
			// deterministically -- and its snooze does NOT consume the
			// job's bounded attempt budget, so this costs nothing against
			// the normal retry allowance for attempt 1 itself.
			//
			// CHAOS-5399 r2 (codex P1): deliverErr is passed by TEXT, not by
			// %w-wrapping (errors.New(deliverErr.Error()), not deliverErr
			// itself) -- deliberately severing its cause chain here.
			// jobruntime.classify used to check
			// errors.Is(err, context.DeadlineExceeded) BEFORE it ever
			// inspected the RetryableAfter snooze marker; the most common
			// ambiguous shape (an http.Client response timeout) wraps exactly
			// that, so passing the chain through had classify silently take
			// the CategoryTimeout branch instead -- an ordinary retry that
			// DOES consume the attempt budget, defeating this whole fix.
			//
			// CHAOS-5455 fixed that ordering at its source: classify now
			// answers a snooze marker ahead of both context branches, for
			// every job kind, so the flattening is no longer load-bearing.
			// It is kept because it is still the right thing to hand a
			// framework-facing error: all the descriptive detail already
			// reached the log line above, and a flattened error cannot
			// accidentally re-acquire a meaning classify keys on later.
			//
			// The r3 residual is CLOSED by the same change. classify also
			// checked errors.Is(ctx.Err(), context.Canceled) against the LIVE
			// context, independent of whatever this function returned, so a
			// worker draining at the exact moment an ambiguous Send() result
			// was handled bypassed the snooze regardless -- costing the
			// guaranteed, timely path to the staleness alert (never
			// duplicate-send safety: the claim is never released for
			// FenceOutcomeAmbiguous, unconditionally, above). Pinned by
			// internal/jobruntime/classify_snooze_order_test.go and by
			// TestAmbiguousOutcomeDuringADrainStillSnoozesAndHoldsTheClaim.
			return jobruntime.RetryableAfter(errors.New(deliverErr.Error()), AmbiguousReconciliationDelay)
		}
		handler.releaseClaim(ctx, logger, notification.ID, outcome)
		if outcome == FenceOutcomePermanentDrop {
			return jobruntime.Permanent(deliverErr)
		}
		return jobruntime.Retryable(deliverErr)
	}

	// The send succeeded, or there was no owner to send to. Recording
	// completion is bookkeeping ON TOP of that fact, never a gate on it --
	// but what a FAILED completion write means depends entirely on whether
	// an email actually went out, and the two cases are opposites.
	if err := handler.fence.MarkCompleted(ctx, notification.ID, handler.now()); err != nil {
		if sent {
			// An email IS out. Releasing or retrying would duplicate it, so
			// we do neither. The claim stays held with completed_at unset --
			// exactly the stale-claim state a later contending attempt
			// classifies and surfaces. Bookkeeping is stuck; delivery is not.
			logger.ErrorContext(ctx,
				"billing notification: the email was sent but its completion write "+
					"failed; the claim stays held and will surface as a stale claim",
				"error", err, "sent", true,
				"claim_outcome", string(FenceOutcomeSentFenceWriteFailed))
			return nil
		}
		// Nothing was sent (the organization has no owner), so there is
		// nothing to duplicate and the duplicate-avoidance argument above
		// does not apply. CHAOS-5353 r2 (P1): returning nil here anyway was
		// the sibling of the r1 defect -- it stranded the row behind an
		// uncompleted claim while River recorded success, so no retry could
		// ever finish the bookkeeping. Release and retry instead: the next
		// attempt re-checks the owner (one may exist by then) and completes.
		logger.ErrorContext(ctx,
			"billing notification: nothing was sent and the completion write failed; "+
				"releasing the claim so a retry can resolve it",
			"error", err, "sent", false,
			"claim_outcome", string(FenceOutcomeReleasedForRetry))
		handler.releaseClaim(ctx, logger, notification.ID, FenceOutcomeReleasedForRetry)
		return jobruntime.Retryable(err)
	}
	logger.InfoContext(ctx, "billing notification: delivered",
		"sent", sent, "claim_outcome", string(outcome))
	return nil
}

// deliver renders and sends under a held claim. It returns the outcome so the
// caller can release the claim exactly once, and reports whether an email
// actually went out (an organization with no owner is a successful no-send,
// matching Python, which returned "sent" and recorded completion for it).
func (handler *BillingHandler) deliver(
	ctx context.Context, logger *slog.Logger, notification BillingNotification,
) (bool, FenceOutcome, error) {
	if _, err := uuid.Parse(notification.OrganizationID); err != nil {
		// Seen with Stripe TEST webhooks carrying fixture ids like "org-abc".
		// A malformed id is permanently bad; retrying cannot repair it.
		logger.ErrorContext(ctx, "billing notification: organization identifier is not a UUID",
			"error", err, "claim_outcome", string(FenceOutcomePermanentDrop))
		return false, FenceOutcomePermanentDrop,
			fmt.Errorf("%w: organization identifier is invalid", ErrDeliveryInvalid)
	}
	if !SupportedEmailType(notification.NotificationType) {
		// notification_type originates in webhook payload data, so the value
		// itself stays out of the message; it is already a structured field.
		logger.ErrorContext(ctx, "billing notification: unsupported email type",
			"claim_outcome", string(FenceOutcomePermanentDrop))
		return false, FenceOutcomePermanentDrop, ErrUnknownEmailType
	}
	attributes, err := DecodeBillingAttributes(notification.Attributes)
	if err != nil {
		logger.ErrorContext(ctx, "billing notification: stored attributes are malformed",
			"error", err, "claim_outcome", string(FenceOutcomePermanentDrop))
		return false, FenceOutcomePermanentDrop, err
	}

	owner, err := handler.owners.LoadOrgOwner(ctx, notification.OrganizationID)
	if errors.Is(err, ErrOrgOwnerNotFound) {
		// Python logged a warning and returned without sending, which the
		// caller then recorded as a completed notification. Same outcome, but
		// named so it is distinguishable from a real delivery in the logs.
		logger.WarnContext(ctx, "billing notification: organization has no owner; nothing to send",
			"claim_outcome", string(FenceOutcomeNoOwner))
		return false, FenceOutcomeNoOwner, nil
	}
	if err != nil {
		logger.ErrorContext(ctx, "billing notification: owner lookup failed",
			"error", err)
		return false, FenceOutcomeReleasedForRetry, err
	}

	rendered, err := RenderBillingEmail(
		notification.NotificationType, attributes, owner, handler.appBaseURL)
	if err != nil {
		// A render failure under a valid type means the stored data cannot
		// fill the template -- permanent, not transient.
		logger.ErrorContext(ctx, "billing notification: rendering failed",
			"error", err, "claim_outcome", string(FenceOutcomePermanentDrop))
		return false, FenceOutcomePermanentDrop, err
	}
	if err := handler.sender.Send(ctx, EmailMessage{
		To: owner.Email, Subject: rendered.Subject, HTML: rendered.HTML,
	}); err != nil {
		var ambiguous *AmbiguousSendError
		if errors.As(err, &ambiguous) {
			// CHAOS-5399: an ambiguous provider result -- e.g. Resend
			// accepted the request and the response timed out, or an SMTP
			// connection dropped after DATA but before the final reply --
			// is NOT "nothing was sent". Every path logs at the failure
			// site with its outcome class; this one additionally carries
			// the provider message id when the sender had one.
			logger.ErrorContext(ctx,
				"billing notification: the provider result is ambiguous; the claim will "+
					"NOT be released, to avoid sending a message that may already be out",
				"error", err, "provider_message_id", ambiguous.ProviderMessageID,
				"claim_outcome", string(FenceOutcomeAmbiguous))
			return false, FenceOutcomeAmbiguous, err
		}
		logger.ErrorContext(ctx, "billing notification: the provider rejected the message",
			"error", err, "claim_outcome", string(FenceOutcomeReleasedForRetry))
		return false, FenceOutcomeReleasedForRetry, err
	}
	return true, FenceOutcomeSent, nil
}

// reportLostClaim classifies a claim this attempt did not win.
func (handler *BillingHandler) reportLostClaim(
	ctx context.Context, logger *slog.Logger, claim ClaimResult, now time.Time,
) error {
	if claim.CompletedAt != nil {
		logger.InfoContext(ctx, "billing notification: already completed by another attempt",
			"claim_outcome", string(FenceOutcomeDuplicateSuppressed))
		return nil
	}
	if claim.Stale(now) {
		// A claim this old with no completion did not merely lose a
		// concurrent race -- that resolves in seconds. The attempt that made
		// it almost certainly died before sending. We do NOT know whether the
		// email went out, so this is reported as its own non-success outcome
		// rather than masquerading as a duplicate. Nothing reaps or resends
		// it here; it is surfaced for an operator.
		logger.ErrorContext(ctx,
			"billing notification: claim is stale -- claimed but never completed",
			"claimed_at", claim.ClaimedAt, "claim_outcome", string(FenceOutcomeStaleClaim))
		return jobruntime.Permanent(errors.New("operational billing notification claim is stale"))
	}
	// Inside the normal in-flight window. This is NOT reported as success.
	//
	// CHAOS-5353 r1 (P1): returning nil here was the one path that could
	// record an email as delivered when none was ever sent. If an attempt
	// failed its send AND then failed to release its claim, the very next
	// retry met its own still-fresh, uncompleted claim, called it a
	// duplicate, and returned success -- so the swallowed release error was
	// never actually observable, despite the comment that said it would
	// surface as a stale claim. It could not surface: nothing retried again.
	//
	// A held claim with completed_at NULL means "not finished", which is
	// precisely what Retryable means -- so that is what we return. The
	// genuine concurrent-duplicate case is unaffected in outcome, only in
	// timing: once the winner records completion, the next retry takes the
	// completed_at branch above and succeeds. A winner that died instead
	// crosses the staleness threshold and is reported there. Neither path
	// can report a delivery that did not happen.
	logger.InfoContext(ctx, "billing notification: another attempt holds an uncompleted claim",
		"claimed_at", claim.ClaimedAt, "claim_outcome", string(FenceOutcomeDuplicateSuppressed))
	return jobruntime.Retryable(errors.New(
		"operational billing notification is claimed by an attempt that has not completed"))
}

// releaseClaimAttempts bounds the inline retry below. A release failure is
// almost always a transient database blip, and each extra try costs one small
// UPDATE -- cheap next to the alternative, which is a notification stranded
// behind its own claim until the staleness threshold expires.
const releaseClaimAttempts = 3

// releaseClaim undoes a claim whose delivery never happened.
//
// The release is retried inline (CHAOS-5353 r1, P1) rather than attempted
// once. The single-shot version left the claim held on any transient failure,
// and because the next retry then met a fresh uncompleted claim, the
// notification could never be sent again within the staleness window.
//
// A release failure that outlasts every attempt is still logged and swallowed
// rather than propagated: this runs inside the handling of an error the caller
// is about to return, and letting a second error replace the first would hide
// why the delivery failed at all. What changed is that the leftover state is
// now genuinely observable -- reportLostClaim no longer reports an uncompleted
// claim as success, so the job keeps retrying until the claim goes stale and
// is reported as such, instead of terminating with a false delivery.
func (handler *BillingHandler) releaseClaim(
	ctx context.Context, logger *slog.Logger, notificationID string, outcome FenceOutcome,
) {
	var err error
	for attempt := 1; attempt <= releaseClaimAttempts; attempt++ {
		if err = handler.fence.ReleaseClaim(ctx, notificationID); err == nil {
			logger.InfoContext(ctx, "billing notification: claim released without a delivery",
				"claim_outcome", string(outcome), "release_attempts", attempt)
			return
		}
		logger.WarnContext(ctx, "billing notification: claim release failed, retrying",
			"error", err, "attempt", attempt, "of", releaseClaimAttempts,
			"claim_outcome", string(outcome))
		// Stop early if the caller's context is already done; further
		// attempts would fail for that reason alone and say nothing new.
		if ctx.Err() != nil {
			break
		}
	}
	logger.ErrorContext(ctx,
		"billing notification: claim release failed on every attempt; the claim stays "+
			"held and this notification cannot be sent until it goes stale",
		"error", err, "release_attempts", releaseClaimAttempts,
		"claim_outcome", string(outcome))
}
