// Package providerrevocation sends the pending provider OAuth revocation rows
// (public.provider_oauth_revocations) that no route sends: a row is otherwise
// sent only by a disconnect or a callback of the same credential, so a row for a
// credential that is gone (one the restore fence requeued, CHAOS-11835) would
// stay owed forever (CHAOS-12010).
//
// A row exists exactly while its token is owed a revoke: a row is deleted only
// after PagerDuty accepted the revoke, and a refused one keeps its place with
// attempts+1 and a reason code, and is claimed again after a backoff. Delivery
// is at least once: a lost delete (the DELETE after an accepted revoke does not
// commit) may send the same revoke again, which PagerDuty answers as for a
// revoked token (RFC 7009 section 2.2: 200). At MaxAttempts the worker stops claiming it;
// the row stays pending (a disconnect, callback or org deletion still sends it)
// and is counted and named by id. Nothing here logs, returns or records a
// token, a ciphertext or a provider answer.
package providerrevocation

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

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// BatchLimit bounds the rows one run sends: each is one provider call of
	// at most 10 s, inside the kind's 300 s timeout.
	BatchLimit = 25
	// MaxAttempts is where the worker stops claiming a row (after the first
	// refusal the backoff is 1, 2, 4 ... 256 min, capped at 6 h: about 8.5 h).
	MaxAttempts = 10
	// SetupStaleAfter is the callback's own cutoff
	// (admin.setupRevocationStaleAfter): a setup row that never failed a
	// revoke and is younger belongs to a callback still in flight.
	SetupStaleAfter = 15 * time.Minute

	ReasonRemoteRevokeFailed = "remote_revoke_failed"
	ReasonTokenDecryptFailed = "token_decrypt_failed"

	// ExhaustedNameLimit bounds the row ids one run names in its exhausted-rows
	// WARN line; the line also carries the total.
	ExhaustedNameLimit = 25
)

// ExhaustedRow is one pending row at MaxAttempts, named by id and the class of
// its last refusal (never its text).
type ExhaustedRow struct {
	ID     uuid.UUID
	Reason string
}

// Row is one claimed revocation row. Ciphertext never leaves this package.
type Row struct {
	ID         uuid.UUID
	Ciphertext string
	// KeyVersion names what the sealed value holds (providerfoundation.PagerDutyRevocationToken).
	KeyVersion string
}

// Claim is one row held by its own transaction until Sent, Failed or Release.
type Claim interface {
	Row() Row
	// Sent deletes the row and commits.
	Sent(ctx context.Context) error
	// Failed records one refused attempt with reason and commits; it returns
	// the row's attempts after the update.
	Failed(ctx context.Context, reason string) (int, error)
	// Release rolls the claim back.
	Release(ctx context.Context)
}

// Store claims due rows and counts them.
type Store interface {
	// Next claims one due row, skipping a row another sender holds; ok is
	// false when no row is due.
	Next(ctx context.Context) (Claim, bool, error)
	// Totals counts the pending rows and the ones at MaxAttempts.
	Totals(ctx context.Context) (pending, exhausted int64, err error)
	// Exhausted lists up to limit pending rows at MaxAttempts, by id, whoever
	// pushed them there (the worker, or a disconnect or callback sender).
	Exhausted(ctx context.Context, limit int) ([]ExhaustedRow, error)
}

// Decryptor opens a row's sealed token.
type Decryptor interface {
	Decrypt(ciphertext secrets.Value) ([]byte, error)
}

// Revoker sends one revoke to the provider.
type Revoker func(ctx context.Context, token string) error

// Handler binds the scheduled send to the River job runtime.
type Handler struct {
	store      Store
	decryptor  Decryptor
	revoke     Revoker
	configured bool
	logger     *slog.Logger
	metrics    instruments
}

// NewHandler constructs the sync.provider_oauth_revocations job owner.
// configured is false when the PagerDuty client id is unset: a run then
// touches no row (the row waits for the configuration, CHAOS-6619).
func NewHandler(store Store, decryptor Decryptor, revoke Revoker, configured bool, logger *slog.Logger) (*Handler, error) {
	if store == nil || decryptor == nil || revoke == nil || logger == nil {
		return nil, errors.New("provider revocation store, decryptor, revoker and logger are required")
	}
	return &Handler{store: store, decryptor: decryptor, revoke: revoke, configured: configured, logger: logger, metrics: defaultInstruments}, nil
}

type tally struct {
	claimed, revoked, retried, exhaustedNow, decryptFailed int
}

// Work sends up to BatchLimit due rows, one transaction each.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ProviderOAuthRevocationsArgs],
) error {
	if handler == nil || handler.store == nil || execution == nil {
		return jobruntime.Permanent(errors.New("provider revocation handler is not configured"))
	}
	scheduledFor, err := time.Parse(time.RFC3339, execution.Args.Payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("provider revocation request is invalid"))
	}
	if !handler.configured {
		pending, exhausted, err := handler.store.Totals(ctx)
		if err != nil {
			handler.logger.ErrorContext(ctx, "provider_oauth_revocations.run", slog.String("outcome", "count_failed"))
			return jobruntime.Retryable(fmt.Errorf("count provider revocations: %w", err))
		}
		handler.metrics.exhausted.Record(ctx, exhausted)
		handler.logger.WarnContext(ctx, "provider_oauth_revocations.run",
			slog.String("outcome", "unconfigured"),
			slog.Int64("skipped_unconfigured", pending),
			slog.Int64("exhausted_total", exhausted))
		return handler.nameExhausted(ctx, exhausted)
	}
	var counts tally
	for counts.claimed < BatchLimit {
		claim, ok, err := handler.store.Next(ctx)
		if err != nil {
			handler.logRun(ctx, "claim_failed", counts, -1)
			return jobruntime.Retryable(fmt.Errorf("claim provider revocation: %w", err))
		}
		if !ok {
			break
		}
		counts.claimed++
		if err := handler.send(ctx, claim, &counts); err != nil {
			handler.logRun(ctx, "write_failed", counts, -1)
			return jobruntime.Retryable(err)
		}
	}
	_, exhausted, err := handler.store.Totals(ctx)
	if err != nil {
		handler.logRun(ctx, "count_failed", counts, -1)
		return jobruntime.Retryable(fmt.Errorf("count provider revocations: %w", err))
	}
	handler.metrics.exhausted.Record(ctx, exhausted)
	handler.logRun(ctx, "sent", counts, exhausted)
	return handler.nameExhausted(ctx, exhausted)
}

// nameExhausted writes the run's one WARN line naming the pending rows at
// MaxAttempts: the total and up to ExhaustedNameLimit "id:reason class"
// entries in id order, whoever pushed the rows there (the worker, or a
// disconnect or callback sender, which the worker never claims). No line when
// there is none.
func (handler *Handler) nameExhausted(ctx context.Context, total int64) error {
	if total <= 0 {
		return nil
	}
	rows, err := handler.store.Exhausted(ctx, ExhaustedNameLimit)
	if err != nil {
		handler.logger.ErrorContext(ctx, "provider_oauth_revocations.run", slog.String("outcome", "name_failed"))
		return jobruntime.Retryable(fmt.Errorf("list exhausted provider revocations: %w", err))
	}
	named := make([]string, 0, len(rows))
	for _, row := range rows {
		named = append(named, row.ID.String()+":"+row.Reason)
	}
	handler.logger.WarnContext(ctx, "provider_oauth_revocations.exhausted_rows",
		slog.Int64("exhausted_total", total),
		slog.Int("listed", len(named)),
		slog.Any("rows", named))
	return nil
}

func (handler *Handler) send(ctx context.Context, claim Claim, counts *tally) error {
	row := claim.Row()
	reason := ""
	plaintext, err := handler.decryptor.Decrypt(secrets.NewValue(row.Ciphertext))
	var token string
	if err == nil {
		token, err = providerfoundation.PagerDutyRevocationToken(row.KeyVersion, plaintext)
	}
	if err != nil {
		reason = ReasonTokenDecryptFailed
	} else if err := handler.revoke(ctx, token); err != nil {
		reason = ReasonRemoteRevokeFailed
	}
	if reason == "" {
		if err := claim.Sent(ctx); err != nil {
			claim.Release(ctx)
			return fmt.Errorf("delete sent provider revocation: %w", err)
		}
		counts.revoked++
		handler.metrics.rows.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "revoked")))
		return nil
	}
	attempts, err := claim.Failed(ctx, reason)
	if err != nil {
		claim.Release(ctx)
		return fmt.Errorf("record refused provider revocation: %w", err)
	}
	if reason == ReasonTokenDecryptFailed {
		counts.decryptFailed++
	}
	if attempts >= MaxAttempts {
		counts.exhaustedNow++
		handler.metrics.rows.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "exhausted")))
		handler.logger.WarnContext(ctx, "provider_oauth_revocations.exhausted",
			slog.String("revocation_id", row.ID.String()),
			slog.Int("attempts", attempts),
			slog.String("reason", reason))
		return nil
	}
	counts.retried++
	handler.metrics.rows.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "retried")))
	return nil
}

// logRun writes the one line per run: counts only.
func (handler *Handler) logRun(ctx context.Context, outcome string, counts tally, exhaustedTotal int64) {
	handler.logger.InfoContext(ctx, "provider_oauth_revocations.run",
		slog.String("outcome", outcome),
		slog.Int("claimed", counts.claimed),
		slog.Int("revoked", counts.revoked),
		slog.Int("retried", counts.retried),
		slog.Int("exhausted_now", counts.exhaustedNow),
		slog.Int("decrypt_failed", counts.decryptFailed),
		slog.Int64("exhausted_total", exhaustedTotal))
}

const (
	rowsInstrument      = "provider_oauth_revocations_total"
	exhaustedInstrument = "provider_oauth_revocations_exhausted"
)

type instruments struct {
	rows      metric.Int64Counter
	exhausted metric.Int64Gauge
}

var defaultInstruments = newInstruments(otel.Meter("github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"))

func newInstruments(meter metric.Meter) instruments {
	rows, err := meter.Int64Counter(rowsInstrument,
		metric.WithDescription("Provider OAuth revocation rows the worker sent, retried or stopped retrying, by outcome."))
	if err != nil {
		rows, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(rowsInstrument)
	}
	exhausted, err := meter.Int64Gauge(exhaustedInstrument,
		metric.WithDescription("Pending provider OAuth revocation rows at the worker's attempt limit."))
	if err != nil {
		exhausted, _ = otel.GetMeterProvider().Meter("noop").Int64Gauge(exhaustedInstrument)
	}
	return instruments{rows: rows, exhausted: exhausted}
}
