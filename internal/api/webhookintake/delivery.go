package webhookintake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// webhookEvent is router.py's WebhookEvent, the normalized shape every
// provider handler builds before persisting.
type webhookEvent struct {
	Provider      string // "github" | "gitlab" | "jira"
	EventType     eventType
	RawEventType  string
	DeliveryID    string // provider's own delivery id, "" if none
	OrgID         *string
	RepoID        *string
	RepoName      *string
	Payload       []byte // the raw, already-validated JSON body
	PayloadParsed pyjson.Value
}

// deliveryKey ports router.py's _delivery_key: the provider's own delivery
// id when present, else a sha256 of the canonical payload.
func deliveryKey(event webhookEvent) (string, error) {
	if event.DeliveryID != "" {
		return event.DeliveryID, nil
	}
	canonical, err := pyjson.MarshalCanonical(event.PayloadParsed)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// persistWebhookDelivery ports _persist_webhook_delivery: insert the
// durable row, and on a (provider, delivery_key) conflict -- a genuine
// provider retry, not an error -- resolve to the existing row instead.
func persistWebhookDelivery(ctx context.Context, pool *pgxpool.Pool, event webhookEvent) (uuid.UUID, error) {
	key, err := deliveryKey(event)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("compute delivery key: %w", err)
	}
	canonical, err := pyjson.MarshalCanonical(event.PayloadParsed)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("canonicalize payload: %w", err)
	}
	digestBytes := sha256.Sum256(canonical)
	digest := hex.EncodeToString(digestBytes[:])
	id := uuid.New()

	_, err = pool.Exec(ctx, `
INSERT INTO public.webhook_deliveries
    (id, provider, delivery_key, event_type, raw_event_type, org_ref, repo_name, payload, payload_sha256, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, now())`,
		id, event.Provider, key, string(event.EventType), event.RawEventType,
		event.OrgID, event.RepoName, event.Payload, digest,
	)
	if err == nil {
		return id, nil
	}
	if !isUniqueViolation(err) {
		return uuid.UUID{}, fmt.Errorf("persist webhook delivery: %w", err)
	}
	var existing uuid.UUID
	scanErr := pool.QueryRow(ctx, `
SELECT id FROM public.webhook_deliveries WHERE provider = $1 AND delivery_key = $2`,
		event.Provider, key,
	).Scan(&existing)
	if errors.Is(scanErr, pgx.ErrNoRows) {
		return uuid.UUID{}, fmt.Errorf("persist webhook delivery: %w", err)
	}
	if scanErr != nil {
		return uuid.UUID{}, fmt.Errorf("resolve conflicting webhook delivery: %w", scanErr)
	}
	return existing, nil
}

func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}

// errRouteUnavailable mirrors the 503 router.py's _dispatch_webhook_task
// raises when routing/enqueue fails: the durable delivery row already
// exists (persistWebhookDelivery is idempotent on (provider, delivery_key)),
// so a caller retry safely re-resolves to it.
var errRouteUnavailable = errors.New("webhook delivery could not be routed; retry")

// dispatchWebhookDelivery ports _dispatch_webhook_task: persist first (a
// durable receipt survives even if the outbox publish fails), then publish
// to the job outbox. KindWebhookDelivery is fully go_implemented/route=river
// (contracts/jobs/v1/migration-state.json), so this always calls Publish,
// never a deferred/Celery route -- Python's own allow_deferred_route=True
// existed only for the since-deleted Celery fallback window.
func dispatchWebhookDelivery(ctx context.Context, pool *pgxpool.Pool, producer *joboutbox.Producer, event webhookEvent) (uuid.UUID, error) {
	if producer == nil {
		return uuid.UUID{}, errors.New("job outbox producer is not configured")
	}
	deliveryID, err := persistWebhookDelivery(ctx, pool, event)
	if err != nil {
		return uuid.UUID{}, err
	}
	key, err := deliveryKey(event)
	if err != nil {
		return deliveryID, err
	}
	digest := sha256.Sum256([]byte(event.Provider + ":" + key))
	idempotencyKey := "webhook:" + hex.EncodeToString(digest[:])
	deliveryIDText := deliveryID.String()
	envelope := jobcontract.Envelope{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "webhook-delivery:" + deliveryIDText,
		IdempotencyKey:  idempotencyKey,
		Domain:          jobcontract.DomainLink{Type: "webhook_delivery", ID: deliveryIDText},
		Payload:         jobcontract.WebhookDeliveryPayload{DeliveryID: deliveryIDText},
	}
	publishErr := producer.PublishStandalone(ctx, jobcontract.KindWebhookDelivery, envelope)
	if !joboutbox.IsPublished(publishErr) {
		return deliveryID, fmt.Errorf("%w: %v", errRouteUnavailable, publishErr)
	}
	return deliveryID, nil
}
