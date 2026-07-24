// Package operational owns low-volume webhook and billing job handlers.
// Queue arguments contain durable row identifiers only; PostgreSQL remains the
// source of truth for payloads, recipient resolution, and delivery identity.
package operational

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

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
}

type BillingNotification struct {
	ID               string
	OrganizationID   string
	NotificationType string
	IdempotencyKey   string
}

type DeliveryStore interface {
	LoadWebhook(context.Context, string) (WebhookDelivery, error)
	LoadBilling(context.Context, string) (BillingNotification, error)
}

// Dispatcher is a concrete effect boundary. The production HTTP adapter sends
// durable references to the existing internal operational services; it never
// sends provider bodies, billing attributes, or recipient addresses.
type Dispatcher interface {
	DispatchWebhook(context.Context, WebhookDelivery) error
	DispatchBilling(context.Context, BillingNotification) error
}

type WebhookHandler struct {
	store      DeliveryStore
	dispatcher Dispatcher
}

func NewWebhookHandler(store DeliveryStore, dispatcher Dispatcher) (*WebhookHandler, error) {
	if store == nil || dispatcher == nil {
		return nil, errors.New("complete webhook dependencies are required")
	}
	return &WebhookHandler{store: store, dispatcher: dispatcher}, nil
}

func (handler *WebhookHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.WebhookDeliveryArgs]) error {
	if handler == nil || handler.store == nil || handler.dispatcher == nil || execution == nil {
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
	if err := handler.dispatcher.DispatchWebhook(ctx, delivery); err != nil {
		if errors.Is(err, ErrDispatchPermanent) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	return nil
}

type BillingHandler struct {
	store      DeliveryStore
	dispatcher Dispatcher
}

func NewBillingHandler(store DeliveryStore, dispatcher Dispatcher) (*BillingHandler, error) {
	if store == nil || dispatcher == nil {
		return nil, errors.New("complete billing dependencies are required")
	}
	return &BillingHandler{store: store, dispatcher: dispatcher}, nil
}

func (handler *BillingHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.BillingNotificationArgs]) error {
	if handler == nil || handler.store == nil || handler.dispatcher == nil || execution == nil {
		return jobruntime.Permanent(errors.New("billing handler is not configured"))
	}
	id := execution.Args.Payload.NotificationID
	if execution.Envelope.Domain.ID != "" && execution.Envelope.Domain.ID != id {
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}
	notification, err := handler.store.LoadBilling(ctx, id)
	if err != nil {
		return classifyStoreError(err)
	}
	if notification.ID != id || execution.OrganizationID == nil ||
		notification.OrganizationID != *execution.OrganizationID ||
		notification.NotificationType == "" || notification.IdempotencyKey == "" {
		return jobruntime.Permanent(ErrDeliveryInvalid)
	}
	if err := handler.dispatcher.DispatchBilling(ctx, notification); err != nil {
		if errors.Is(err, ErrDispatchPermanent) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
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
