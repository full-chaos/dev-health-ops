// Package pushprojection is the auth-service reader of External Push lifecycle
// events: it reads external_push_credential rows of the auth outbox and hands
// each to the credential projection engine (outboxprojection). It never
// validates or mutates a credential and imports neither the customer-push
// domain nor the public store.
//
// The read does not rest on an id cursor: bigserial ids commit out of order, so
// a pure cursor could pass a lower id that commits late. A row is read while
// no digest row of the engine names its (credential, revision) and it is not
// parked, in id order from a low-water mark. The mark moves only over rows
// older than the horizon and only across rows that are decided or parked; a row that commits later than that falls to the next
// revision's gap and to the drift alert.
package pushprojection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	// Stream is the reader's key in projection_cursor and projection_retry.
	Stream = "external_push"
	// CheckName is the readiness check name (a metric label value).
	CheckName = "push_projection_reader"

	degradeAfter = 3
	maxBatch     = 1000
	baseDelay    = 5 * time.Second
	maxDelay     = 5 * time.Minute
	markEvery    = time.Minute
)

// ErrUnfenced: the database is on a timeline with no restore-fence record; the step did nothing.
var ErrUnfenced = errors.New("pushprojection: the timeline has no restore-fence record")

// ErrInvalidConfig reports an unusable Config.
var ErrInvalidConfig = errors.New("pushprojection: invalid configuration")

// Projector is the engine's one call.
type Projector interface {
	Project(ctx context.Context, in outboxprojection.Input) (outboxprojection.Result, error)
}

// Config builds a Reader.
type Config struct {
	Pool         *pgxpool.Pool
	Schema       authschema.ValidatedIdentifier
	Engine       Projector
	Registry     *health.Registry
	Logger       *slog.Logger
	Meter        metric.Meter
	PollInterval time.Duration
	// BatchLimit bounds the rows one step reads and the parked rows one step retries (1..1000).
	BatchLimit int
	// Horizon is how long a row is kept above the low-water mark (up to 24 h).
	Horizon time.Duration
}

// DefaultConfig is the production shape.
func DefaultConfig(schema authschema.ValidatedIdentifier, registry *health.Registry) Config {
	return Config{Schema: schema, Registry: registry, PollInterval: time.Second, BatchLimit: 50, Horizon: time.Hour}
}

func (c Config) validateShape() error {
	if c.PollInterval < 10*time.Millisecond || c.PollInterval > 15*time.Minute ||
		c.BatchLimit < 1 || c.BatchLimit > maxBatch || c.Horizon <= 0 || c.Horizon > 24*time.Hour {
		return ErrInvalidConfig
	}
	return nil
}

// OutboxRow is one external_push_credential row of the auth outbox.
type OutboxRow struct {
	ID             int64
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}

// EventFromRow maps an outbox row onto the engine's input. It checks only that
// the row is decodable; an event the engine cannot key (no source_id) decodes
// and is refused there as malformed.
func EventFromRow(row OutboxRow) (outboxprojection.Input, error) {
	if row.AggregateID == "" {
		return outboxprojection.Input{}, fmt.Errorf("%w: no aggregate id", outboxprojection.ErrMalformed)
	}
	var payload contracts.ExternalPushLifecyclePayload
	if err := json.Unmarshal(row.Payload, &payload); err != nil {
		return outboxprojection.Input{}, fmt.Errorf("%w: payload", outboxprojection.ErrMalformed)
	}
	if payload.Revision < 1 {
		return outboxprojection.Input{}, fmt.Errorf("%w: revision", outboxprojection.ErrMalformed)
	}
	return outboxprojection.Input{Domain: outboxprojection.DomainExternalPush, Event: contracts.ExternalPushLifecycleEvent{
		AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: row.AggregateID, EventType: row.EventType,
		IdempotencyKey: row.IdempotencyKey, CreatedAt: contracts.Timestamp{Time: row.CreatedAt.UTC()}, Payload: payload,
	}}, nil
}

// RetryDelay is the wait before a parked row is tried again: 5 s doubling to 5 min.
func RetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts >= 6 {
		return maxDelay
	}
	if d := baseDelay << attempts; d < maxDelay {
		return d
	}
	return maxDelay
}

// StepReport is what one step did.
type StepReport struct {
	Seen     int
	Applied  int
	Parked   int
	Retried  int
	Released int
	Final    int
	Mark     int64
}

// Reader is the lifecycle component that runs the steps.
type Reader struct {
	cfg    Config
	q      string
	logger *slog.Logger

	ready    atomic.Bool
	failures atomic.Int64
	lag      atomic.Int64
	parked   atomic.Int64
	final    atomic.Int64

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewReader returns a Reader and registers its readiness check.
func NewReader(cfg Config) (*Reader, error) {
	if cfg.Pool == nil || cfg.Engine == nil || cfg.validateShape() != nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Reader{cfg: cfg, q: authschema.Quote(cfg.Schema), logger: cfg.Logger}
	if cfg.Registry != nil {
		if err := cfg.Registry.RegisterRequired(CheckName, r.readiness); err != nil {
			return nil, fmt.Errorf("register push projection readiness: %w", err)
		}
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/pushprojection")
	}
	if err := r.registerGauges(meter); err != nil {
		return nil, err
	}
	return r, nil
}

// registerGauges creates the reader's instruments with literal names, so the label census can read them.
func (r *Reader) registerGauges(meter metric.Meter) error {
	observe := func(value *atomic.Int64) metric.Int64Callback {
		return func(_ context.Context, o metric.Int64Observer) error { o.Observe(value.Load()); return nil }
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_push_projection_lag",
		metric.WithDescription("Highest external_push_credential outbox id minus the reader's low-water mark at the last step."),
		metric.WithInt64Callback(observe(&r.lag))); err != nil {
		return fmt.Errorf("register the lag gauge: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_push_projection_parked_rows",
		metric.WithDescription("Rows parked for a retry (a gap or a parent not seen yet) at the last step."),
		metric.WithInt64Callback(observe(&r.parked))); err != nil {
		return fmt.Errorf("register the parked-rows gauge: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_push_projection_final_rows",
		metric.WithDescription("Rows parked for good (malformed, unknown organization, conflict) at the last step; the drift alert owns them."),
		metric.WithInt64Callback(observe(&r.final))); err != nil {
		return fmt.Errorf("register the final-rows gauge: %w", err)
	}
	return nil
}

func (r *Reader) readiness(context.Context) error {
	if !r.ready.Load() {
		return errors.New("push projection reader has not completed a successful step")
	}
	return nil
}

// Name identifies the component to the lifecycle runtime.
func (*Reader) Name() string { return "auth-push-projection" }

// Start launches the polling goroutine and returns at once; readiness opens after the first successful step.
func (r *Reader) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("pushprojection: reader already started")
	}
	r.started = true
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel
	r.done = make(chan struct{})
	go r.run(loopCtx, r.done)
	return nil
}

func (r *Reader) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	r.stepOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.stepOnce(ctx)
		}
	}
}

func (r *Reader) stepOnce(ctx context.Context) {
	_, err := r.Step(ctx, time.Now())
	switch {
	case err == nil:
		r.failures.Store(0)
		r.ready.Store(true)
	case ctx.Err() != nil:
	default:
		n := r.failures.Add(1)
		if n >= degradeAfter {
			r.ready.Store(false)
		}
		r.logger.ErrorContext(ctx, "push projection step failed", "consecutive_failures", n, logging.ErrorAttr(err))
	}
}

// Shutdown stops the goroutine and waits for the step in flight, within ctx.
func (r *Reader) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pushprojection: reader did not stop: %w", ctx.Err())
	}
}

// revisionExpr is the payload revision as a bigint, or NULL when it is outside the wire range 1..2^53-1 (16 digits at most).
const revisionExpr = `(CASE WHEN o.payload ->> 'revision' ~ '^[0-9]{1,16}$' THEN
		(CASE WHEN (o.payload ->> 'revision')::bigint <= 9007199254740991 THEN (o.payload ->> 'revision')::bigint END) END)`

// Step reads, projects and parks one batch, retries the parked rows that are
// due, and moves the low-water mark. It does nothing on an unfenced timeline.
func (r *Reader) Step(ctx context.Context, now time.Time) (StepReport, error) {
	var rep StepReport
	if err := restorefence.Check(ctx, r.cfg.Pool, r.cfg.Schema); err != nil {
		if errors.Is(err, restorefence.ErrNotFenced) {
			return rep, ErrUnfenced
		}
		return rep, err
	}
	mark, markedAt, err := r.readMark(ctx)
	if err != nil {
		return rep, err
	}
	rows, err := r.scan(ctx, mark)
	if err != nil {
		return rep, err
	}
	held := map[string]bool{}
	for _, row := range rows {
		// A later row of an aggregate waits behind a row parked in this same pass, as it does behind one parked before.
		if held[row.AggregateID] {
			continue
		}
		rep.Seen++
		before := rep.Parked + rep.Final
		if err := r.handle(ctx, row, now, &rep); err != nil {
			return rep, err
		}
		held[row.AggregateID] = rep.Parked+rep.Final > before
	}
	if err := r.retryDue(ctx, now, &rep); err != nil {
		return rep, err
	}
	rep.Mark = mark
	next, newest, err := r.nextMark(ctx, now, mark)
	if err != nil {
		return rep, err
	}
	if next > mark && (mark == 0 || now.Sub(markedAt) >= markEvery) {
		if err := r.advance(ctx, next); err != nil {
			return rep, err
		}
		rep.Mark = next
	}
	r.lag.Store(max(newest-rep.Mark, 0))
	return rep, r.countParked(ctx)
}

func (r *Reader) readMark(ctx context.Context) (int64, time.Time, error) {
	var mark int64
	var updated time.Time
	err := r.cfg.Pool.QueryRow(ctx, `SELECT low_water, updated_at FROM `+r.q+`.projection_cursor WHERE stream = $1`, Stream).Scan(&mark, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := r.mutate(ctx, "auth.projection_cursor.created", 0, func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO `+r.q+`.projection_cursor (stream) VALUES ($1) ON CONFLICT DO NOTHING`, Stream)
			return err
		}); err != nil && !errors.Is(err, audit.ErrNoStateChange) {
			return 0, time.Time{}, err
		}
		return 0, time.Time{}, nil
	}
	return mark, updated, err
}

func (r *Reader) scan(ctx context.Context, mark int64) ([]OutboxRow, error) {
	rows, err := r.cfg.Pool.Query(ctx, `
		SELECT o.id, o.aggregate_id, o.event_type, o.idempotency_key, o.payload, o.created_at
		FROM `+r.q+`.auth_outbox_events AS o
		WHERE o.aggregate_type = 'external_push_credential' AND o.id > $1
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.credential_projection_digest d
		                  WHERE d.domain_owner = 'external_push' AND d.external_reference = o.aggregate_id AND d.revision = `+revisionExpr+`)
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $2 AND p.outbox_id = o.id)
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $2 AND p.aggregate_id = o.aggregate_id AND p.revision < `+revisionExpr+`)
		ORDER BY o.id
		LIMIT $3`, mark, Stream, r.cfg.BatchLimit)
	if err != nil {
		return nil, fmt.Errorf("pushprojection: reading the outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var row OutboxRow
		if err := rows.Scan(&row.ID, &row.AggregateID, &row.EventType, &row.IdempotencyKey, &row.Payload, &row.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// finalOutcomes are the outcomes the engine leaves no digest row for and that no retry changes.
func finalWithoutDigest(o outboxprojection.Outcome) bool {
	return o == outboxprojection.OutcomeMalformed || o == outboxprojection.OutcomeOrganizationUnknown || o == outboxprojection.OutcomeConflict
}

func (r *Reader) handle(ctx context.Context, row OutboxRow, now time.Time, rep *StepReport) error {
	in, err := EventFromRow(row)
	if err != nil {
		rep.Final++
		return r.park(ctx, row.ID, row.AggregateID, 1, row.Payload, "malformed", true, now, 0)
	}
	res, err := r.cfg.Engine.Project(ctx, in)
	if err != nil {
		return err
	}
	revision := int64(in.Event.Payload.Revision)
	body, _ := json.Marshal(in.Event)
	switch {
	case res.Class == outboxprojection.ClassUnfenced:
		return ErrUnfenced
	case res.Outcome == pushlifecycle.Applied:
		rep.Applied++
	case res.Class == outboxprojection.ClassRetry:
		rep.Parked++
		return r.park(ctx, row.ID, row.AggregateID, revision, body, string(res.Outcome), false, now, 0)
	case finalWithoutDigest(res.Outcome):
		rep.Final++
		return r.park(ctx, row.ID, row.AggregateID, revision, body, string(res.Outcome), true, now, 0)
	}
	return nil
}

// park writes (or, for a row already parked, re-times) one retry row. A final
// row waits at infinity: no step reads it again.
func (r *Reader) park(ctx context.Context, outboxID int64, aggregate string, revision int64, body []byte, outcome string, final bool, now time.Time, attempts int) error {
	next := now.Add(RetryDelay(attempts)).UTC().Format(time.RFC3339Nano)
	if final {
		next = "infinity"
	}
	return r.mutate(ctx, "auth.credential_projection.parked", revision, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO `+r.q+`.projection_retry AS p (stream, outbox_id, aggregate_id, revision, body, attempts, next_at, last_outcome)
			VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::timestamptz, $8)
			ON CONFLICT (stream, outbox_id) DO UPDATE SET attempts = p.attempts + 1, next_at = EXCLUDED.next_at, last_outcome = EXCLUDED.last_outcome`,
			Stream, outboxID, aggregate, revision, string(body), attempts, next, outcome)
		return err
	})
}

func (r *Reader) release(ctx context.Context, outboxID int64, revision int64) error {
	return r.mutate(ctx, "auth.credential_projection.released", revision, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `DELETE FROM `+r.q+`.projection_retry WHERE stream = $1 AND outbox_id = $2`, Stream, outboxID)
		return err
	})
}

type dueRow struct {
	outboxID  int64
	aggregate string
	revision  int64
	body      []byte
	attempts  int
}

func (r *Reader) retryDue(ctx context.Context, now time.Time, rep *StepReport) error {
	rows, err := r.cfg.Pool.Query(ctx, `
		SELECT r.outbox_id, r.aggregate_id, r.revision, r.body, r.attempts FROM `+r.q+`.projection_retry AS r
		WHERE r.stream = $1 AND r.next_at <= $2
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = r.stream AND p.aggregate_id = r.aggregate_id AND p.revision < r.revision)
		ORDER BY r.outbox_id LIMIT $3`, Stream, now.UTC(), r.cfg.BatchLimit)
	if err != nil {
		return fmt.Errorf("pushprojection: reading the retry table: %w", err)
	}
	var due []dueRow
	for rows.Next() {
		var d dueRow
		if err := rows.Scan(&d.outboxID, &d.aggregate, &d.revision, &d.body, &d.attempts); err != nil {
			rows.Close()
			return err
		}
		due = append(due, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, d := range due {
		var event contracts.ExternalPushLifecycleEvent
		if err := json.Unmarshal(d.body, &event); err != nil {
			rep.Final++
			if err := r.park(ctx, d.outboxID, d.aggregate, d.revision, d.body, "malformed", true, now, d.attempts); err != nil {
				return err
			}
			continue
		}
		res, err := r.cfg.Engine.Project(ctx, outboxprojection.Input{Domain: outboxprojection.DomainExternalPush, Event: event})
		if err != nil {
			return err
		}
		switch {
		case res.Class == outboxprojection.ClassUnfenced:
			return ErrUnfenced
		case res.Class == outboxprojection.ClassRetry:
			rep.Retried++
			err = r.park(ctx, d.outboxID, d.aggregate, d.revision, d.body, string(res.Outcome), false, now, d.attempts+1)
		case finalWithoutDigest(res.Outcome):
			rep.Final++
			err = r.park(ctx, d.outboxID, d.aggregate, d.revision, d.body, string(res.Outcome), true, now, d.attempts)
		default:
			if res.Outcome == pushlifecycle.Applied {
				rep.Applied++
			}
			rep.Released++
			err = r.release(ctx, d.outboxID, d.revision)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// nextMark is the highest id the mark may move to: every push row younger than
// the horizon stays above it, it never passes the newest row older than the
// horizon, and it moves only across a contiguous prefix of rows that are durably
// decided (a digest row) or parked: the first row above the mark that is neither
// (never scanned, or held behind a parked row of its aggregate) stops it, so the
// reaper cannot delete a row the reader has not decided. newest is the highest
// push id (for the lag gauge).
func (r *Reader) nextMark(ctx context.Context, now time.Time, mark int64) (next, newest int64, err error) {
	cutoff := now.Add(-r.cfg.Horizon).UTC()
	err = r.cfg.Pool.QueryRow(ctx, `
		SELECT LEAST(
		    (SELECT COALESCE(MAX(id), 0) FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'external_push_credential' AND created_at < $1),
		    (SELECT COALESCE(MIN(id), 9223372036854775807) - 1 FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'external_push_credential' AND created_at >= $1),
		    (SELECT COALESCE(MIN(o.id), 9223372036854775807) - 1 FROM `+r.q+`.auth_outbox_events AS o
		      WHERE o.aggregate_type = 'external_push_credential' AND o.id > $2
		        AND NOT EXISTS (SELECT 1 FROM `+r.q+`.credential_projection_digest d
		                        WHERE d.domain_owner = 'external_push' AND d.external_reference = o.aggregate_id AND d.revision = `+revisionExpr+`)
		        AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $3 AND p.outbox_id = o.id))),
		    (SELECT COALESCE(MAX(id), 0) FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'external_push_credential')`, cutoff, mark, Stream).Scan(&next, &newest)
	return next, newest, err
}

func (r *Reader) advance(ctx context.Context, mark int64) error {
	return r.mutate(ctx, "auth.projection_cursor.advanced", 0, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `UPDATE `+r.q+`.projection_cursor SET low_water = GREATEST(low_water, $2), updated_at = clock_timestamp()
			WHERE stream = $1 AND low_water < $2`, Stream, mark)
		return err
	})
}

func (r *Reader) countParked(ctx context.Context) error {
	var parked, final int64
	err := r.cfg.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE next_at < 'infinity'), count(*) FILTER (WHERE next_at = 'infinity')
		FROM `+r.q+`.projection_retry WHERE stream = $1`, Stream).Scan(&parked, &final)
	if err != nil {
		return err
	}
	r.parked.Store(parked)
	r.final.Store(final)
	return nil
}

func mutationRow(eventType string, revision int64) audit.AuditEvent {
	attrs := map[string]any{"target_kind": "credential", "credential_class": "external_push"}
	if revision > 0 {
		attrs["revision"] = revision
	}
	return audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, Attributes: attrs}
}

// mutate runs one guarded write inside audit.Commit with one audit row and no outbox event.
func (r *Reader) mutate(ctx context.Context, eventType string, revision int64, apply func(context.Context, audit.TxOps) error) error {
	err := audit.Commit(ctx, r.cfg.Pool, r.cfg.Schema, audit.Mutation{
		Apply: apply, Audit: mutationRow(eventType, revision),
		OffStream: audit.OffStreamCredentialProjection,
	})
	if err != nil && !errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	return nil
}
