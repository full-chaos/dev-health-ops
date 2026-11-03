// Package pushexpiry emits the expired lifecycle event of External Push
// credentials whose expires_at passed and that were never revoked
// (external-push-lifecycle-event.v1).
//
// The sweep takes no row lock and writes no token column: it reads five token
// columns and calls the auth schema's append function. Two sweeps, a sweep and
// a revoke, or a repeat all end in one terminal event, because the function
// answers a second terminal event for a credential with its current revision
// and writes nothing (migration 0052). Nothing here logs, returns or records a
// token, its name, prefix or hash; a run names tokens by id and counts only.
package pushexpiry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const (
	// BatchLimit is the tokens one scan page reads.
	BatchLimit = 100
	// RunCap bounds the work of one run: the tokens it submits plus the tokens
	// that fail. A token the handler already submitted (see Handler.ended) is
	// skipped and costs no cap, so a table with more than RunCap tokens in the
	// window is swept over several runs. A run that ends on the cap reports
	// outcome=capped; the next run starts from the beginning (no persisted
	// cursor) and skips what this process already ended.
	RunCap = 10000
	// ExpiryWindow bounds the scan: only tokens that expired within it are read,
	// so the scan does not grow with every token that ever expired. Accepted
	// limit: a token that expired longer ago than this and never got its expired
	// event is not swept, and the sweeper cannot count those (a count needs a
	// scan). Projection against domain store drift (CHAOS-10897) reconciles them.
	ExpiryWindow = 30 * 24 * time.Hour
)

// Token is the stored state the sweep needs: nothing that names or
// authenticates the credential.
type Token struct {
	ID        uuid.UUID
	OrgID     string
	SourceID  *uuid.UUID
	Scopes    []byte
	ExpiresAt time.Time
}

// Cursor is the keyset position of a scan: the last token read.
type Cursor struct {
	ExpiresAt time.Time
	ID        uuid.UUID
}

// Store reads the expired, unrevoked tokens and sends one expired event each.
type Store interface {
	// Page returns up to limit tokens after the cursor that expired after
	// notBefore, in (expires_at, id) order.
	Page(ctx context.Context, after Cursor, limit int, notBefore time.Time) ([]Token, error)
	// Send submits the token's expired event in its own transaction. A nil
	// result means the credential is ended: the writer appended the event, or
	// the credential was already ended and it wrote nothing (an idempotent no-op
	// the caller cannot tell apart from an append).
	Send(ctx context.Context, token Token) error
}

// Handler binds the scheduled sweep to the River job runtime.
type Handler struct {
	store   Store
	on      bool
	logger  *slog.Logger
	metrics instruments
	now     func() time.Time

	// ended holds the ids of tokens whose Send returned nil, with their
	// expires_at. It is per process and in memory: after a restart (or on
	// another replica) the first run re-submits up to RunCap idempotent
	// no-ops, then progresses. It is pruned on every run to the window, so it
	// holds at most the tokens that expired within ExpiryWindow. A token whose
	// Send failed is never added.
	mu    sync.Mutex
	ended map[uuid.UUID]time.Time
}

// NewHandler constructs the sync.external_push_expiry_sweep job owner. on is
// the AUTH_PUSH_LIFECYCLE_EVENTS flag: off, a run touches nothing.
func NewHandler(store Store, on bool, logger *slog.Logger) (*Handler, error) {
	if store == nil || logger == nil {
		return nil, errors.New("push expiry store and logger are required")
	}
	return &Handler{store: store, on: on, logger: logger, metrics: defaultInstruments, now: time.Now,
		ended: map[uuid.UUID]time.Time{}}, nil
}

// ExpiredEvent is the expired event of a stored token, with the snapshot the
// writer needs when the credential has no event yet.
func ExpiredEvent(token Token) (audit.PushLifecycleEvent, error) {
	organization, binding, source, err := pushevent.WireIDs(token.OrgID, token.SourceID)
	if err != nil {
		return audit.PushLifecycleEvent{}, err
	}
	names, err := pushevent.ScopeNames(token.Scopes)
	if err != nil {
		return audit.PushLifecycleEvent{}, err
	}
	snapshot := pushevent.CreatedChange(token.ID, names, &token.ExpiresAt)
	return audit.PushLifecycleEvent{EventType: externalpush.Expired, AggregateID: token.ID.String(),
		OrganizationID: organization, Binding: binding, SourceID: source,
		Change:   externalpush.Change{Status: "expired", ExpiresAt: pushevent.Expiry(&token.ExpiresAt)},
		Snapshot: &snapshot}, nil
}

type tally struct{ scanned, skipped, submitted, failed int }

// prune drops the skip-set ids that left the window and cannot be read again.
func (handler *Handler) prune(notBefore time.Time) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for id, expires := range handler.ended {
		if !expires.After(notBefore) {
			delete(handler.ended, id)
		}
	}
}

func (handler *Handler) isEnded(id uuid.UUID) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	_, ok := handler.ended[id]
	return ok
}

func (handler *Handler) markEnded(token Token) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.ended[token.ID] = token.ExpiresAt
}

// Work submits the expired event of the tokens that expired within
// ExpiryWindow, one transaction each, until the run's cap (RunCap submitted
// plus failed) is reached. "Submitted" is not "appended": the writer answers a
// repeat terminal event with the existing revision and writes nothing, so a
// submitted event may be an idempotent no-op.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ExternalPushExpirySweepArgs],
) error {
	if handler == nil || handler.store == nil || execution == nil {
		return jobruntime.Permanent(errors.New("push expiry handler is not configured"))
	}
	scheduledFor, err := time.Parse(time.RFC3339, execution.Args.Payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("push expiry request is invalid"))
	}
	if !handler.on {
		handler.logger.InfoContext(ctx, "external_push_expiry_sweep.run", slog.String("outcome", "off"))
		return nil
	}
	var counts tally
	var after Cursor
	notBefore := handler.now().Add(-ExpiryWindow)
	handler.prune(notBefore)
	for counts.submitted+counts.failed < RunCap {
		limit := min(BatchLimit, RunCap-counts.submitted-counts.failed)
		page, err := handler.store.Page(ctx, after, limit, notBefore)
		if err != nil {
			handler.logRun(ctx, "scan_failed", counts)
			return jobruntime.Retryable(fmt.Errorf("scan expired push tokens: %w", err))
		}
		if len(page) == 0 {
			break
		}
		for _, token := range page {
			counts.scanned++
			if handler.isEnded(token.ID) {
				counts.skipped++
				continue
			}
			if err := handler.store.Send(ctx, token); err != nil {
				if ctx.Err() != nil {
					handler.logRun(ctx, "cancelled", counts)
					return jobruntime.Retryable(ctx.Err())
				}
				counts.failed++
				handler.metrics.tokens.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "failed")))
				handler.logger.WarnContext(ctx, "external_push_expiry_sweep.token_failed", slog.String("token_id", token.ID.String()))
				continue
			}
			handler.markEnded(token)
			counts.submitted++
			handler.metrics.tokens.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "submitted")))
		}
		last := page[len(page)-1]
		after = Cursor{ExpiresAt: last.ExpiresAt, ID: last.ID}
		if len(page) < limit {
			break
		}
	}
	outcome := "swept"
	switch {
	case counts.submitted+counts.failed >= RunCap:
		outcome = "capped"
	case counts.failed > 0:
		outcome = "partial"
	}
	handler.logRun(ctx, outcome, counts)
	if counts.failed > 0 {
		return jobruntime.Retryable(fmt.Errorf("%d push tokens failed", counts.failed))
	}
	return nil
}

// logRun writes the one line per run: counts only.
func (handler *Handler) logRun(ctx context.Context, outcome string, counts tally) {
	handler.logger.InfoContext(ctx, "external_push_expiry_sweep.run",
		slog.String("outcome", outcome),
		slog.Int("scanned", counts.scanned),
		slog.Int("skipped", counts.skipped),
		slog.Int("submitted", counts.submitted),
		slog.Int("failed", counts.failed))
}

const tokensInstrument = "external_push_expiry_sweep_tokens_total"

type instruments struct{ tokens metric.Int64Counter }

var defaultInstruments = newInstruments(otel.Meter("github.com/full-chaos/dev-health-ops/internal/jobs/pushexpiry"))

func newInstruments(meter metric.Meter) instruments {
	tokens, err := meter.Int64Counter(tokensInstrument,
		metric.WithDescription("Expired External Push tokens the sweep submitted an expired event for (the writer may have found the credential already ended and written nothing), or failed to, by outcome."))
	if err != nil {
		tokens, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(tokensInstrument)
	}
	return instruments{tokens: tokens}
}
