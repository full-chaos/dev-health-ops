package operational

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("domain PostgreSQL pool is required")
	}
	return &PostgresStore{pool: pool}, nil
}

func (store *PostgresStore) LoadWebhook(ctx context.Context, id string) (WebhookDelivery, error) {
	if store == nil || store.pool == nil {
		return WebhookDelivery{}, errors.New("webhook store is unavailable")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return WebhookDelivery{}, ErrDeliveryInvalid
	}
	var delivery WebhookDelivery
	var payload json.RawMessage
	err = store.pool.QueryRow(ctx, `
		SELECT id::text, provider, delivery_key, event_type,
			COALESCE(org_ref, ''), COALESCE(repo_name, ''), payload, payload_sha256
		FROM public.webhook_deliveries WHERE id = $1`, parsed,
	).Scan(
		&delivery.ID, &delivery.Provider, &delivery.DeliveryKey, &delivery.EventType,
		&delivery.Organization, &delivery.Repository, &payload, &delivery.PayloadSHA256,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return WebhookDelivery{}, ErrDeliveryNotFound
	}
	if err != nil {
		return WebhookDelivery{}, errors.New("webhook store is unavailable")
	}
	delivery.Payload = append([]byte(nil), payload...)
	return delivery, nil
}

func (store *PostgresStore) LoadBilling(ctx context.Context, id string) (BillingNotification, error) {
	if store == nil || store.pool == nil {
		return BillingNotification{}, errors.New("billing store is unavailable")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return BillingNotification{}, ErrDeliveryInvalid
	}
	var notification BillingNotification
	err = store.pool.QueryRow(ctx, `
		SELECT id::text, org_id::text, notification_type, idempotency_key
		FROM public.billing_notifications WHERE id = $1`, parsed,
	).Scan(
		&notification.ID, &notification.OrganizationID,
		&notification.NotificationType, &notification.IdempotencyKey,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return BillingNotification{}, ErrDeliveryNotFound
	}
	if err != nil {
		return BillingNotification{}, errors.New("billing store is unavailable")
	}
	return notification, nil
}
