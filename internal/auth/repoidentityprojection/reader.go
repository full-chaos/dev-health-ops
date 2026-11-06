// Package repoidentityprojection is the auth-service reader of repository identity events: it reads the
// repository_identity rows of the auth outbox (the provider sync appends them through the 0057 function) and upserts
// auth.repository_identities from each, the copy of the provider repository ids (D6651; ClickHouse repos stays the
// source).
//
// An event is the full state of one repository, so there is no gap rule: an event whose revision is at or below the
// revision the stored row was written from is decided and changes nothing. A larger revision moves the row to that
// revision; it takes the event's path and observed_at only when the event was observed no earlier than the stored
// row, so a sync that fetched first and committed last cannot undo a rename.
//
// The read does not rest on an id cursor (bigserial ids commit out of order): a row is read while it is undecided and
// not parked, in id order from a low-water mark. The mark moves only over rows older than the horizon and only across
// rows that are decided or parked, so the reaper cannot delete a row this reader has not decided.
package repoidentityprojection

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentity"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	// Stream is the reader's key in projection_cursor and projection_retry (the reaper names it too).
	Stream = "repository_identity"
	// CheckName is the readiness check name (a metric label value).
	CheckName = "repository_identity_projection_reader"

	degradeAfter = 3
	maxBatch     = 1000
	baseDelay    = 5 * time.Second
	maxDelay     = 5 * time.Minute
	markEvery    = time.Minute
)

// Outcomes a parked row records.
const (
	OutcomeMalformed           = "malformed"
	OutcomeOrganizationUnknown = "organization_unknown"
)

// ErrUnfenced: the database is on a timeline with no restore-fence record; the step did nothing.
var ErrUnfenced = errors.New("repoidentityprojection: the timeline has no restore-fence record")

// ErrInvalidConfig reports an unusable Config.
var ErrInvalidConfig = errors.New("repoidentityprojection: invalid configuration")

// ErrMalformed: the outbox row is not a repository identity event this reader can apply.
var ErrMalformed = errors.New("repoidentityprojection: malformed event")

// Config builds a Reader.
type Config struct {
	Pool     *pgxpool.Pool
	Schema   authschema.ValidatedIdentifier
	Registry *health.Registry
	Logger   *slog.Logger
	Meter    metric.Meter
	// ProjectOrganization copies an organization into auth.organizations when an event names one the copy lacks; it
	// reports whether the organization exists. Nil parks such a row until the copy appears.
	ProjectOrganization func(context.Context, uuid.UUID) (bool, error)
	PollInterval        time.Duration
	// BatchLimit bounds the rows one step reads and the parked rows one step retries (1..1000).
	BatchLimit int
	// Horizon is how long a row is kept above the low-water mark (up to 24 h).
	Horizon time.Duration
}

// DefaultConfig is the production shape.
func DefaultConfig(schema authschema.ValidatedIdentifier, registry *health.Registry) Config {
	return Config{Schema: schema, Registry: registry, PollInterval: 5 * time.Second, BatchLimit: 200, Horizon: time.Hour}
}

func (c Config) validateShape() error {
	if c.PollInterval < 10*time.Millisecond || c.PollInterval > 15*time.Minute ||
		c.BatchLimit < 1 || c.BatchLimit > maxBatch || c.Horizon <= 0 || c.Horizon > 24*time.Hour {
		return ErrInvalidConfig
	}
	return nil
}

// OutboxRow is one repository_identity row of the auth outbox.
type OutboxRow struct {
	ID             int64
	AggregateID    string
	EventType      string
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
}

// Event is one decoded repository identity event.
type Event struct {
	OrganizationID uuid.UUID
	Provider       string
	ProviderRepoID string
	Path           string
	ObservedAt     time.Time
	Revision       int64
}

var (
	providerShape = regexp.MustCompile(`^[a-z]{1,16}$`)
	repoIDShape   = regexp.MustCompile(`^[0-9]{1,20}$`)
	pathShape     = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*$`)
	uuidShape     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// EventFromRow decodes and checks an outbox row: the event type, the closed payload of repository-identity-event.v1,
// the aggregate id (it must name the payload's repository) and the idempotency key.
func EventFromRow(row OutboxRow) (Event, error) {
	if row.EventType != contracts.RepositoryIdentityObserved {
		return Event{}, fmt.Errorf("%w: event type", ErrMalformed)
	}
	var payload contracts.RepositoryIdentityPayload
	decoder := json.NewDecoder(bytes.NewReader(row.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || decoder.More() {
		return Event{}, fmt.Errorf("%w: payload", ErrMalformed)
	}
	change := payload.Change
	switch {
	case payload.Revision < 1:
		return Event{}, fmt.Errorf("%w: revision", ErrMalformed)
	case !uuidShape.MatchString(payload.OrganizationID):
		return Event{}, fmt.Errorf("%w: organization id", ErrMalformed)
	case !providerShape.MatchString(change.Provider), !repoIDShape.MatchString(change.ProviderRepoID):
		return Event{}, fmt.Errorf("%w: provider or provider id", ErrMalformed)
	case len(change.Path) > 255 || !pathShape.MatchString(change.Path):
		return Event{}, fmt.Errorf("%w: path", ErrMalformed)
	case change.ObservedAt.IsZero():
		return Event{}, fmt.Errorf("%w: observed_at", ErrMalformed)
	case row.AggregateID != payload.AggregateID():
		return Event{}, fmt.Errorf("%w: the aggregate id does not name the payload's repository", ErrMalformed)
	case row.IdempotencyKey != fmt.Sprintf("%s:%s:%d", contracts.RepositoryIdentityAggregate, row.AggregateID, payload.Revision):
		return Event{}, fmt.Errorf("%w: idempotency key", ErrMalformed)
	}
	return Event{
		OrganizationID: uuid.MustParse(payload.OrganizationID), Provider: change.Provider, ProviderRepoID: change.ProviderRepoID,
		Path: change.Path, ObservedAt: change.ObservedAt.UTC(), Revision: int64(payload.Revision),
	}, nil
}

// RetryDelay is the wait before a parked row is tried again: 5 s doubling to 5 min.
func RetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts >= 6 {
		return maxDelay
	}
	return min(baseDelay<<attempts, maxDelay)
}

// StepReport is what one step did.
type StepReport struct {
	Seen     int
	Applied  int
	Decided  int
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
	if cfg.Pool == nil || cfg.validateShape() != nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Reader{cfg: cfg, q: authschema.Quote(cfg.Schema), logger: cfg.Logger}
	if cfg.Registry != nil {
		if err := cfg.Registry.RegisterRequired(CheckName, r.readiness); err != nil {
			return nil, fmt.Errorf("register repository identity projection readiness: %w", err)
		}
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/repoidentityprojection")
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
	if _, err := meter.Int64ObservableGauge("devhealth_auth_repository_identity_projection_lag",
		metric.WithDescription("Highest repository_identity outbox id minus the reader's low-water mark at the last step."),
		metric.WithInt64Callback(observe(&r.lag))); err != nil {
		return fmt.Errorf("register the lag gauge: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_repository_identity_projection_parked_rows",
		metric.WithDescription("Rows parked for a retry (an organization the auth copy lacks yet) at the last step."),
		metric.WithInt64Callback(observe(&r.parked))); err != nil {
		return fmt.Errorf("register the parked-rows gauge: %w", err)
	}
	if _, err := meter.Int64ObservableGauge("devhealth_auth_repository_identity_projection_final_rows",
		metric.WithDescription("Rows parked for good (malformed) at the last step."),
		metric.WithInt64Callback(observe(&r.final))); err != nil {
		return fmt.Errorf("register the final-rows gauge: %w", err)
	}
	return nil
}

func (r *Reader) readiness(context.Context) error {
	if !r.ready.Load() {
		return errors.New("repository identity projection reader has not completed a successful step")
	}
	return nil
}

// Name identifies the component to the lifecycle runtime.
func (*Reader) Name() string { return "auth-repository-identity-projection" }

// Start launches the polling goroutine and returns at once; readiness opens after the first successful step.
func (r *Reader) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("repoidentityprojection: reader already started")
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
		r.logger.ErrorContext(ctx, "repository identity projection step failed", "consecutive_failures", n, logging.ErrorAttr(err))
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
		return fmt.Errorf("repoidentityprojection: reader did not stop: %w", ctx.Err())
	}
}

// revisionExpr is the payload revision as a bigint, or NULL when it is outside the wire range 1..2^53-1.
const revisionExpr = `(CASE WHEN o.payload ->> 'revision' ~ '^[0-9]{1,16}$' THEN
		(CASE WHEN (o.payload ->> 'revision')::bigint <= 9007199254740991 THEN (o.payload ->> 'revision')::bigint END) END)`

// decidedExpr holds for an outbox row o whose repository row was written from its revision or a later one. A row with
// a malformed organization, provider or id names no repository row and is never decided here (it is parked).
func (r *Reader) decidedExpr() string {
	return `EXISTS (SELECT 1 FROM ` + r.q + `.repository_identities i
		WHERE i.organization_id = (CASE WHEN o.payload ->> 'organization_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
		                               THEN (o.payload ->> 'organization_id')::uuid END)
		  AND i.provider = o.payload -> 'change' ->> 'provider'
		  AND i.provider_repo_id = o.payload -> 'change' ->> 'provider_repo_id'
		  AND i.source_revision >= ` + revisionExpr + `)`
}

// Step reads and applies one batch, retries the parked rows that are due, and moves the low-water mark. It does
// nothing on an unfenced timeline.
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
		}); err != nil {
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
		WHERE o.aggregate_type = 'repository_identity' AND o.id > $1
		  AND NOT `+r.decidedExpr()+`
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $2 AND p.outbox_id = o.id)
		  AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $2 AND p.aggregate_id = o.aggregate_id AND p.revision < `+revisionExpr+`)
		ORDER BY o.id
		LIMIT $3`, mark, Stream, r.cfg.BatchLimit)
	if err != nil {
		return nil, fmt.Errorf("repoidentityprojection: reading the outbox: %w", err)
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

// outcome is what applying one event did.
type outcome int

const (
	applied outcome = iota
	decided
	organizationMissing
)

func (r *Reader) handle(ctx context.Context, row OutboxRow, now time.Time, rep *StepReport) error {
	event, err := EventFromRow(row)
	if err != nil {
		rep.Final++
		return r.park(ctx, row.ID, row.AggregateID, 1, rowBody(row), OutcomeMalformed, true, now, 0)
	}
	got, err := r.applyWithOrganization(ctx, event)
	if err != nil {
		return err
	}
	switch got {
	case applied:
		rep.Applied++
	case decided:
		rep.Decided++
	case organizationMissing:
		rep.Parked++
		return r.park(ctx, row.ID, row.AggregateID, event.Revision, rowBody(row), OutcomeOrganizationUnknown, false, now, 0)
	}
	return nil
}

// rowBody is what a parked row keeps: the outbox row itself, so a retry needs no outbox read.
func rowBody(row OutboxRow) []byte {
	body, _ := json.Marshal(row)
	return body
}

// applyWithOrganization applies the event; when the auth copy lacks the organization it projects it once (if a
// projector is set) and tries again.
func (r *Reader) applyWithOrganization(ctx context.Context, event Event) (outcome, error) {
	got, err := r.apply(ctx, event)
	if err != nil || got != organizationMissing || r.cfg.ProjectOrganization == nil {
		return got, err
	}
	exists, err := r.cfg.ProjectOrganization(ctx, event.OrganizationID)
	if err != nil || !exists {
		return organizationMissing, err
	}
	return r.apply(ctx, event)
}

// apply moves the repository row to the event's revision in one guarded write and then gives the organization's
// live repository grants the id of their path (repoidentity.FillGrantRepoIDs, the backfill's rule). A revision at or
// below the stored one changes nothing, fills nothing and writes no audit row.
func (r *Reader) apply(ctx context.Context, event Event) (outcome, error) {
	err := audit.Commit(ctx, r.cfg.Pool, r.cfg.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			args := []any{event.OrganizationID, event.Provider, event.ProviderRepoID, event.Path, event.Revision, event.ObservedAt}
			// The commit guard counts a row when its BEFORE trigger fires, and an INSERT fires it even when ON
			// CONFLICT then does nothing. So a decided event must reach no INSERT: the UPDATE first (it fires only for
			// a row its WHERE keeps), then a read, and the INSERT only for a repository with no row.
			tag, err := tx.Exec(ctx, `UPDATE `+r.q+`.repository_identities AS i SET `+moveTo+`
WHERE i.organization_id = $1 AND i.provider = $2 AND i.provider_repo_id = $3 AND i.source_revision < $5`, args...)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				var exists bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+r.q+`.repository_identities
WHERE organization_id = $1 AND provider = $2 AND provider_repo_id = $3)`, args[:3]...).Scan(&exists); err != nil {
					return err
				}
				if exists {
					return nil
				}
				// A replica that inserts the same repository first turns this into the guarded UPDATE.
				if _, err := tx.Exec(ctx, `INSERT INTO `+r.q+`.repository_identities AS i
    (organization_id, provider, provider_repo_id, current_path, source_revision, observed_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (organization_id, provider, provider_repo_id) DO UPDATE SET `+moveTo+`
WHERE i.source_revision < $5`, args...); err != nil {
					return err
				}
			}
			return repoidentity.FillGrantRepoIDs(ctx, tx, r.cfg.Schema, event.OrganizationID)
		},
		Audit:     observedRow(event),
		OffStream: audit.OffStreamRepositoryIdentity,
	})
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return applied, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return decided, nil
	case errors.As(err, &pgErr) && pgErr.Code == "23503":
		return organizationMissing, nil
	}
	return 0, err
}

// moveTo moves a stored row i to the event ($4 path, $5 revision, $6 observed_at): it always takes the revision; it
// takes the path only when the event was observed no earlier than the row, and updated_at moves only with the path.
const moveTo = `current_path = CASE WHEN i.observed_at IS NULL OR i.observed_at <= $6 THEN $4 ELSE i.current_path END,
    updated_at = CASE WHEN (i.observed_at IS NULL OR i.observed_at <= $6) AND i.current_path <> $4 THEN now() ELSE i.updated_at END,
    observed_at = GREATEST(i.observed_at, $6),
    source_revision = $5`

// park writes (or, for a row already parked, re-times) one retry row. A final row waits at infinity: no step reads
// it again.
func (r *Reader) park(ctx context.Context, outboxID int64, aggregate string, revision int64, body []byte, outcome string, final bool, now time.Time, attempts int) error {
	next := now.Add(RetryDelay(attempts)).UTC().Format(time.RFC3339Nano)
	if final {
		next = "infinity"
	}
	return r.mutate(ctx, "auth.repository_identity.parked", revision, func(ctx context.Context, tx audit.TxOps) error {
		_, err := tx.Exec(ctx, `INSERT INTO `+r.q+`.projection_retry AS p (stream, outbox_id, aggregate_id, revision, body, attempts, next_at, last_outcome)
			VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::timestamptz, $8)
			ON CONFLICT (stream, outbox_id) DO UPDATE SET attempts = p.attempts + 1, next_at = EXCLUDED.next_at, last_outcome = EXCLUDED.last_outcome`,
			Stream, outboxID, aggregate, revision, string(body), attempts, next, outcome)
		return err
	})
}

func (r *Reader) release(ctx context.Context, outboxID int64, revision int64) error {
	return r.mutate(ctx, "auth.repository_identity.released", revision, func(ctx context.Context, tx audit.TxOps) error {
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
		return fmt.Errorf("repoidentityprojection: reading the retry table: %w", err)
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
		var row OutboxRow
		var event Event
		err := json.Unmarshal(d.body, &row)
		if err == nil {
			event, err = EventFromRow(row)
		}
		if err != nil {
			rep.Final++
			if err := r.park(ctx, d.outboxID, d.aggregate, d.revision, d.body, OutcomeMalformed, true, now, d.attempts); err != nil {
				return err
			}
			continue
		}
		got, err := r.applyWithOrganization(ctx, event)
		if err != nil {
			return err
		}
		switch got {
		case organizationMissing:
			rep.Retried++
			err = r.park(ctx, d.outboxID, d.aggregate, d.revision, d.body, OutcomeOrganizationUnknown, false, now, d.attempts+1)
		default:
			if got == applied {
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

// nextMark is the highest id the mark may move to: every repository identity row younger than the horizon stays
// above it, it never passes the newest row older than the horizon, and it moves only across a contiguous prefix of
// rows that are decided or parked. newest is the highest repository identity id (for the lag gauge).
func (r *Reader) nextMark(ctx context.Context, now time.Time, mark int64) (next, newest int64, err error) {
	cutoff := now.Add(-r.cfg.Horizon).UTC()
	err = r.cfg.Pool.QueryRow(ctx, `
		SELECT LEAST(
		    (SELECT COALESCE(MAX(id), 0) FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'repository_identity' AND created_at < $1),
		    (SELECT COALESCE(MIN(id), 9223372036854775807) - 1 FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'repository_identity' AND created_at >= $1),
		    (SELECT COALESCE(MIN(o.id), 9223372036854775807) - 1 FROM `+r.q+`.auth_outbox_events AS o
		      WHERE o.aggregate_type = 'repository_identity' AND o.id > $2
		        AND NOT `+r.decidedExpr()+`
		        AND NOT EXISTS (SELECT 1 FROM `+r.q+`.projection_retry p WHERE p.stream = $3 AND p.outbox_id = o.id))),
		    (SELECT COALESCE(MAX(id), 0) FROM `+r.q+`.auth_outbox_events WHERE aggregate_type = 'repository_identity')`, cutoff, mark, Stream).Scan(&next, &newest)
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

// mutate runs one guarded write of the reader's own bookkeeping inside audit.Commit with one audit row and no outbox
// event. A write that changed nothing is not an error.
func (r *Reader) mutate(ctx context.Context, eventType string, revision int64, apply func(context.Context, audit.TxOps) error) error {
	err := audit.Commit(ctx, r.cfg.Pool, r.cfg.Schema, audit.Mutation{
		Apply: apply, Audit: bookkeepingRow(eventType, revision),
		OffStream: audit.OffStreamRepositoryIdentity,
	})
	if err != nil && !errors.Is(err, audit.ErrNoStateChange) {
		return err
	}
	return nil
}

// observedRow is the audit row of an applied event: its organization and revision.
func observedRow(event Event) audit.AuditEvent {
	org := event.OrganizationID.String()
	return audit.AuditEvent{EventType: "auth.repository_identity.observed", Outcome: audit.OutcomeAllowed, OrganizationID: &org,
		Attributes: map[string]any{"revision": event.Revision}}
}

// bookkeepingRow is the audit row of the reader's own cursor and retry writes.
func bookkeepingRow(eventType string, revision int64) audit.AuditEvent {
	attrs := map[string]any{}
	if revision > 0 {
		attrs["revision"] = revision
	}
	return audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, Attributes: attrs}
}
