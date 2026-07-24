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
// Begin must distinguish a completed receipt from one another process still
// holds, because only the first may be acknowledged. Release returns a claimed
// receipt so a transient failure can be retried on the next reclaim instead of
// waiting out the lease.
type ReceiptStore interface {
	Begin(context.Context, string) (ReceiptClaim, error)
	Complete(context.Context, ReceiptClaim) error
	Release(context.Context, ReceiptClaim) error
}

// ReceiptState is why Begin did or did not hand over the receipt.
type ReceiptState int

const (
	// ReceiptClaimed means this process owns the receipt and must reconcile.
	ReceiptClaimed ReceiptState = iota
	// ReceiptCompleted means the canonical effect already committed. Replay is
	// a bounded no-op and the entry is safe to acknowledge.
	ReceiptCompleted
	// ReceiptInFlight means another process holds a live lease. The entry must
	// NOT be acknowledged: that process may still fail, and acknowledging here
	// would drop the event with no canonical effect and no dead-letter record.
	ReceiptInFlight
)

// ReceiptClaim fences a stale reconciler from completing a receipt reclaimed
// after its lease. Token must be matched by the durable receipt store.
type ReceiptClaim struct {
	ReceiptID, Token string
	State            ReceiptState
}

// Proceed reports whether this process owns the receipt.
func (claim ReceiptClaim) Proceed() bool { return claim.State == ReceiptClaimed }

// Reconciler must commit the locked graph mutation before it returns nil. The
// receipt ID is stable across stream redelivery, allowing that mutation to
// deduplicate the narrow crash window after a durable write but before ACK.
type Reconciler interface {
	Reconcile(context.Context, Event) error
}

// errReceiptInFlight keeps an entry pending without spending its permanent
// budget: it is deliberately not a streamrunner.PermanentError.
var errReceiptInFlight = errors.New("pagerduty receipt is held by another consumer")

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
	switch claim.State {
	case ReceiptCompleted:
		// The canonical effect is durable. Replay is a bounded no-op and the
		// entry may be acknowledged.
		return nil
	case ReceiptInFlight:
		// Another process holds a live lease. Returning nil here would ACK an
		// event whose reconciliation may still fail, losing it with no
		// canonical effect and no dead-letter record. Stay unacknowledged and
		// let a later reclaim past the lease decide.
		return errReceiptInFlight
	}
	if err := handler.reconciler.Reconcile(ctx, event); err != nil {
		// Release before returning so the retry does not have to wait out the
		// lease. A permanent bridge verdict still releases: the runner writes
		// the dead-letter record, and a receipt left running would block the
		// terminal outcome from ever being re-evaluated.
		if releaseErr := handler.receipts.Release(ctx, claim); releaseErr != nil {
			return fmt.Errorf("release pagerduty receipt: %w", releaseErr)
		}
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
