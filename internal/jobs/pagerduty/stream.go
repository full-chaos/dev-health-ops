// Package pagerduty provides the dormant, crash-safe admission boundary for
// PagerDuty Redis Stream entries. It deliberately relies on streamrunner for
// ACK-after-commit and never changes the current Celery route.
package pagerduty

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

var errUnavailable = errors.New("pagerduty admission storage unavailable")

// Event carries parsed stream fields only. Raw payload is passed only to the
// authoritative reconciler and never becomes a metric, queue argument, or log
// label.
type Event struct {
	BindingID string
	EventID   string
	Payload   json.RawMessage
	Received  time.Time
	ReceiptID string
}

// ReceiptStore is the durable authority for the reconcile/ACK crash window.
// Begin returns false for a completed receipt or a live claim; its lease must
// be recoverable so a process death can be retried from the pending stream.
type ReceiptStore interface {
	Begin(context.Context, string) (ReceiptClaim, error)
	Complete(context.Context, ReceiptClaim) error
}

// ReceiptClaim fences a stale reconciler from completing a receipt reclaimed
// after its lease. Token must be matched by the durable receipt store.
type ReceiptClaim struct {
	ReceiptID, Token string
	Proceed          bool
}

// Reconciler must commit the locked graph mutation before it returns nil. The
// receipt ID is stable across stream redelivery, allowing that mutation to
// deduplicate the narrow crash window after a durable write but before ACK.
type Reconciler interface {
	Reconcile(context.Context, Event) error
}

type Handler struct {
	receipts   ReceiptStore
	reconciler Reconciler
}

func NewHandler(receipts ReceiptStore, reconciler Reconciler) (*Handler, error) {
	if receipts == nil || reconciler == nil {
		return nil, errUnavailable
	}
	return &Handler{receipts: receipts, reconciler: reconciler}, nil
}

// Handle is intentionally compatible with streamrunner.Handler. A transient
// failure is returned without ACK; a malformed input is quarantined by the
// runner only after it has written the bounded quarantine record.
func (handler *Handler) Handle(ctx context.Context, message streamrunner.Message) error {
	if handler == nil || handler.receipts == nil || handler.reconciler == nil {
		return errUnavailable
	}
	event, err := parse(message)
	if err != nil {
		return &streamrunner.PermanentError{Reason: "pagerduty_schema_invalid"}
	}
	claim, err := handler.receipts.Begin(ctx, event.ReceiptID)
	if err != nil {
		return fmt.Errorf("claim pagerduty receipt: %w", err)
	}
	if !claim.Proceed {
		return nil
	}
	if err := handler.reconciler.Reconcile(ctx, event); err != nil {
		return fmt.Errorf("reconcile pagerduty event: %w", err)
	}
	if err := handler.receipts.Complete(ctx, claim); err != nil {
		return fmt.Errorf("complete pagerduty receipt: %w", err)
	}
	return nil
}

func parse(message streamrunner.Message) (Event, error) {
	bindingID := strings.TrimSpace(message.Fields["binding_id"])
	payload := strings.TrimSpace(message.Fields["payload"])
	receivedAt := strings.TrimSpace(message.Fields["received_at"])
	if bindingID == "" || payload == "" || receivedAt == "" {
		return Event{}, errors.New("required stream field missing")
	}
	received, err := time.Parse(time.RFC3339, receivedAt)
	if err != nil || received.Location() != time.UTC || !json.Valid([]byte(payload)) {
		return Event{}, errors.New("invalid stream payload")
	}
	var body struct {
		Event struct {
			ID string `json:"id"`
		} `json:"event"`
	}
	if err := json.Unmarshal([]byte(payload), &body); err != nil {
		return Event{}, err
	}
	eventID := strings.TrimSpace(body.Event.ID)
	digest := sha256.Sum256([]byte(payload))
	identity := eventID
	if identity == "" {
		identity = hex.EncodeToString(digest[:])
	}
	return Event{
		BindingID: bindingID,
		EventID:   eventID,
		Payload:   json.RawMessage(payload),
		Received:  received,
		ReceiptID: "pagerduty:" + bindingID + ":" + identity,
	}, nil
}
