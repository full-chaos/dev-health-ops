// Package operational owns low-volume webhook and billing job handlers.
// Queue arguments contain durable row identifiers only; PostgreSQL remains the
// source of truth for payloads, recipient resolution, and delivery identity.
package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

var (
	ErrDeliveryNotFound  = errors.New("operational delivery not found")
	ErrDeliveryInvalid   = errors.New("operational delivery is invalid")
	ErrDispatchPermanent = errors.New("operational delivery was permanently rejected")
)

type WebhookDelivery struct {
	ID            string
	Provider      string
	DeliveryKey   string
	EventType     string
	Organization  string
	Repository    string
	Payload       []byte
	PayloadSHA256 string
	// CreatedAt is this delivery's own receipt time (webhook_deliveries.
	// created_at) -- stable across every retry of this same delivery, unlike
	// time.Now(). CHAOS-5319's native sync-dispatch path uses it as the
	// scheduled_sync_occurrences.scheduled_for value so a retried Work()
	// call recomputes the SAME occurrence_id (scheduledSyncOccurrenceIdentity
	// is a pure function of (config_id, scheduled_for)) instead of minting a
	// second, duplicate sync for one delivery.
	CreatedAt time.Time
}

type BillingNotification struct {
	ID               string
	OrganizationID   string
	NotificationType string
	IdempotencyKey   string
	// Attributes is the raw `billing_notifications.attributes` JSONB. Go
	// renders the email from it directly since CHAOS-5353; before that it was
	// read only by the Python bridge receiver.
	Attributes []byte
}

type DeliveryStore interface {
	LoadWebhook(context.Context, string) (WebhookDelivery, error)
	LoadBilling(context.Context, string) (BillingNotification, error)
}

type WebhookHandler struct {
	store DeliveryStore
}

func NewWebhookHandler(store DeliveryStore) (*WebhookHandler, error) {
	if store == nil {
		return nil, errors.New("complete webhook dependencies are required")
	}
	return &WebhookHandler{store: store}, nil
}

func (handler *WebhookHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.WebhookDeliveryArgs]) error {
	if handler == nil || handler.store == nil || execution == nil {
		return jobruntime.Permanent(errors.New("webhook handler is not configured"))
	}
	id := execution.Args.Payload.DeliveryID
	if execution.Envelope.Domain.ID != "" && execution.Envelope.Domain.ID != id {
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}
	delivery, err := handler.store.LoadWebhook(ctx, id)
	if err != nil {
		return classifyStoreError(err)
	}
	if delivery.ID != id || !validWebhook(delivery) {
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}
	// CHAOS-5318 PR1: GitHub App installation/marketplace_purchase events are
	// handled entirely natively when the store supports it -- no HTTP
	// dispatch to the Python bridge at all for these two event types. Every
	// other event (all of gitlab/jira, and every other github event type)
	// falls through to the native sync-dispatch check below (CHAOS-5320
	// deleted the HTTP bridge entirely -- there is no HTTP fallback left).
	if delivery.Provider == "github" && isNativeGithubAppEvent(delivery.EventType) {
		if writer, ok := handler.store.(InstallationWriter); ok {
			if _, err := writer.UpsertGithubAppEvent(ctx, delivery.EventType, delivery.Payload, time.Now().UTC()); err != nil {
				return jobruntime.Retryable(err)
			}
			return nil
		}
	}
	// CHAOS-5319/CHAOS-5320: the same recognised event types Python's
	// _process_github_event/_process_gitlab_event/_process_jira_event
	// already route to a sync (see isNativeSyncDispatchEvent's doc comment
	// for the exact list, per provider) are, when the store supports it,
	// triggered natively via the scheduled_sync_occurrences/
	// sync_manual_triggers "Sync Now" mechanism instead of the HTTP bridge
	// -- no Python sync entrypoint is ever called from this branch.
	if isNativeSyncDispatchEvent(delivery.Provider, delivery.EventType) {
		if writer, ok := handler.store.(SyncDispatchWriter); ok {
			result, err := writer.TriggerScopedSync(ctx, delivery.Provider, delivery.EventType, delivery.Payload, delivery.CreatedAt)
			if err != nil {
				return jobruntime.Retryable(err)
			}
			if !result.Processed {
				return jobruntime.Permanent(fmt.Errorf("%w: %s", ErrWebhookSyncUnroutable, result.Reason))
			}
			return nil
		}
	}
	// CHAOS-5320: the Python HTTP bridge is gone -- there is no fallback
	// path left. Every event type this handler can receive is now either
	// routed natively above, or an EXPLICIT, counted, logged ignore here
	// (never a silent drop): deployment_status/check_run/check_suite and any
	// other recognised-but-not-yet-native event type, plus anything a future
	// provider integration adds before its own native route lands.
	recordIgnoredWebhookEvent(ctx, delivery.Provider, delivery.EventType, delivery.ID)
	return nil
}

func classifyStoreError(err error) error {
	if errors.Is(err, ErrDeliveryNotFound) || errors.Is(err, ErrDeliveryInvalid) {
		return jobruntime.Permanent(err)
	}
	return jobruntime.Retryable(err)
}

func validWebhook(delivery WebhookDelivery) bool {
	if delivery.ID == "" || delivery.DeliveryKey == "" || delivery.EventType == "" ||
		(delivery.Provider != "github" && delivery.Provider != "gitlab" && delivery.Provider != "jira") ||
		len(delivery.Payload) == 0 || len(delivery.PayloadSHA256) != 64 {
		return false
	}
	sum := sha256.Sum256(delivery.Payload)
	return strings.EqualFold(hex.EncodeToString(sum[:]), delivery.PayloadSHA256)
}
