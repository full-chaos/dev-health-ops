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
	// RunCap bounds the tokens one run reads. Every run starts at the oldest
	// expired unrevoked token and the worker cannot read the auth schema to
	// skip one already ended (it costs one cheap function call), so a table
	// with more expired unrevoked tokens than RunCap would starve the rest.
	// The cap is far above any real count and sits inside the 300 s timeout.
	RunCap = 10000
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
	// Page returns up to limit tokens after the cursor, in (expires_at, id) order.
	Page(ctx context.Context, after Cursor, limit int) ([]Token, error)
	// Send appends the token's expired event in its own transaction.
	Send(ctx context.Context, token Token) error
}

// Handler binds the scheduled sweep to the River job runtime.
type Handler struct {
	store   Store
	on      bool
	logger  *slog.Logger
	metrics instruments
}

// NewHandler constructs the sync.external_push_expiry_sweep job owner. on is
// the AUTH_PUSH_LIFECYCLE_EVENTS flag: off, a run touches nothing.
func NewHandler(store Store, on bool, logger *slog.Logger) (*Handler, error) {
	if store == nil || logger == nil {
		return nil, errors.New("push expiry store and logger are required")
	}
	return &Handler{store: store, on: on, logger: logger, metrics: defaultInstruments}, nil
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

type tally struct{ scanned, sent, failed int }

// Work sends the expired event of up to RunCap tokens, one transaction each.
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
	for counts.scanned < RunCap {
		limit := min(BatchLimit, RunCap-counts.scanned)
		page, err := handler.store.Page(ctx, after, limit)
		if err != nil {
			handler.logRun(ctx, "scan_failed", counts)
			return jobruntime.Retryable(fmt.Errorf("scan expired push tokens: %w", err))
		}
		if len(page) == 0 {
			break
		}
		for _, token := range page {
			counts.scanned++
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
			counts.sent++
			handler.metrics.tokens.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "sent")))
		}
		last := page[len(page)-1]
		after = Cursor{ExpiresAt: last.ExpiresAt, ID: last.ID}
		if len(page) < limit {
			break
		}
	}
	if counts.failed > 0 {
		handler.logRun(ctx, "partial", counts)
		return jobruntime.Retryable(fmt.Errorf("%d push tokens failed", counts.failed))
	}
	handler.logRun(ctx, "swept", counts)
	return nil
}

// logRun writes the one line per run: counts only.
func (handler *Handler) logRun(ctx context.Context, outcome string, counts tally) {
	handler.logger.InfoContext(ctx, "external_push_expiry_sweep.run",
		slog.String("outcome", outcome),
		slog.Int("scanned", counts.scanned),
		slog.Int("sent", counts.sent),
		slog.Int("failed", counts.failed))
}

const tokensInstrument = "external_push_expiry_sweep_tokens_total"

type instruments struct{ tokens metric.Int64Counter }

var defaultInstruments = newInstruments(otel.Meter("github.com/full-chaos/dev-health-ops/internal/jobs/pushexpiry"))

func newInstruments(meter metric.Meter) instruments {
	tokens, err := meter.Int64Counter(tokensInstrument,
		metric.WithDescription("Expired External Push tokens the sweep sent an expired event for, or failed to, by outcome."))
	if err != nil {
		tokens, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(tokensInstrument)
	}
	return instruments{tokens: tokens}
}
