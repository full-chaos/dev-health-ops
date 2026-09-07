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

	// Everything from here to the send runs with a claim HELD. Every exit --
	// error or deliberate drop -- must release it, or a retry meeting the
	// still-fresh, unresolved claim would report a duplicate for an email
	// that was never attempted. `deliver` funnels all of them through one
	// return value so no path can skip the release below.
	sent, outcome, deliverErr := handler.deliver(ctx, logger, notification)
	if deliverErr != nil {
		handler.releaseClaim(ctx, logger, notification.ID, outcome)
		if outcome == FenceOutcomePermanentDrop {
			return jobruntime.Permanent(deliverErr)
		}
		return jobruntime.Retryable(deliverErr)
	}

	// The send succeeded (or there was no owner to send to). Recording
	// completion is bookkeeping ON TOP of that fact, never a gate on it: a
	// failed completion write must NOT release the claim and must NOT retry,
	// because either would duplicate a delivered email. The claim stays held
	// with completed_at unset -- exactly the stale-claim state a later
	// contending attempt classifies and surfaces.
	if err := handler.fence.MarkCompleted(ctx, notification.ID, handler.now()); err != nil {
		logger.ErrorContext(ctx,
			"billing notification: the email was sent but its completion write failed; "+
				"the claim stays held and will surface as a stale claim",
			"error", err, "sent", sent,
			"claim_outcome", string(FenceOutcomeSentFenceWriteFailed))
		return nil
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
		logger.ErrorContext(ctx, "billing notification: the provider rejected the message",
			"error", err)
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
	// Inside the normal in-flight window: an ordinary duplicate suppression.
	// If the other attempt is itself stuck, a later retry's claim will find
	// the same claimed_at and cross the staleness threshold above instead.
	logger.InfoContext(ctx, "billing notification: another attempt holds the claim",
		"claimed_at", claim.ClaimedAt, "claim_outcome", string(FenceOutcomeDuplicateSuppressed))
	return nil
}

// releaseClaim undoes a claim whose delivery never happened.
//
// A release failure is logged and swallowed rather than propagated: this runs
// inside the handling of an error the caller is about to return, so letting a
// second error replace the first would hide why the delivery failed at all.
// The claim is then left held, which the stale-claim detection surfaces on a
// later contending attempt -- observable, not silently lost.
func (handler *BillingHandler) releaseClaim(
	ctx context.Context, logger *slog.Logger, notificationID string, outcome FenceOutcome,
) {
	if err := handler.fence.ReleaseClaim(ctx, notificationID); err != nil {
		logger.ErrorContext(ctx,
			"billing notification: claim release failed; the claim stays held and "+
				"will surface as a stale claim on a later attempt",
			"error", err, "claim_outcome", string(outcome))
		return
	}
	logger.InfoContext(ctx, "billing notification: claim released without a delivery",
		"claim_outcome", string(outcome))
}
