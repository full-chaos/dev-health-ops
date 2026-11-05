package customerpush

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// BackfillReport is what a backfill run counts. It holds counts only.
type BackfillReport struct {
	Candidates       int
	SourceBound      int
	OrganizationWide int
	Revoked          int
	Written          int
	Skipped          int
	Failed           int
}

type backfillCandidate struct {
	id      uuid.UUID
	orgWide bool
	revoked bool
}

// BackfillLifecycle emits the lifecycle events of tokens that predate the
// events. It reads only the token table and never reads the auth schema: the
// writer function is the only statement that names it. Without apply it only
// counts the tokens. With apply, each token is written in its own transaction
// under the token row's lock, through the same AppendPushLifecycle as the
// routes, inside a savepoint. The writer refuses a created event for a token
// that has an aggregate (SQLSTATE 55000): that token is skipped, so a rerun
// finishes what a failed run left and a token a route already wrote is
// skipped. Any other error counts the token as failed and the run goes on.
func BackfillLifecycle(ctx context.Context, pool *pgxpool.Pool, schema string, apply bool, batch int) (BackfillReport, error) {
	return backfillLifecycle(ctx, pool, schema, apply, batch)
}

// backfillDB is the part of a pool the backfill uses.
type backfillDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func backfillLifecycle(ctx context.Context, pool backfillDB, schema string, apply bool, batch int) (BackfillReport, error) {
	ident, err := authschema.NewValidatedIdentifier(schema)
	if err != nil {
		return BackfillReport{}, err
	}
	if batch < 1 {
		return BackfillReport{}, errors.New("customerpush: backfill batch must be positive")
	}
	var report BackfillReport
	var after uuid.UUID
	for {
		page, err := backfillPage(ctx, pool, after, batch)
		if err != nil {
			return report, err
		}
		if len(page) == 0 {
			return report, nil
		}
		for _, c := range page {
			report.Candidates++
			if c.orgWide {
				report.OrganizationWide++
			} else {
				report.SourceBound++
			}
			if c.revoked {
				report.Revoked++
			}
			if !apply {
				continue
			}
			outcome := backfillOne(ctx, pool, ident, c.id)
			switch {
			case ctx.Err() != nil:
				return report, ctx.Err()
			case outcome == backfillWritten:
				report.Written++
			case outcome == backfillSkipped:
				report.Skipped++
			default:
				report.Failed++
			}
		}
		after = page[len(page)-1].id
	}
}

func backfillPage(ctx context.Context, pool backfillDB, after uuid.UUID, batch int) ([]backfillCandidate, error) {
	rows, err := pool.Query(ctx, `SELECT t.id, t.source_id IS NULL, t.revoked_at IS NOT NULL
		FROM external_ingest_tokens t WHERE t.id > $1 ORDER BY t.id LIMIT $2`, after, batch)
	if err != nil {
		return nil, fmt.Errorf("customerpush: reading backfill candidates: %w", err)
	}
	defer rows.Close()
	var page []backfillCandidate
	for rows.Next() {
		var c backfillCandidate
		if err := rows.Scan(&c.id, &c.orgWide, &c.revoked); err != nil {
			return nil, err
		}
		page = append(page, c)
	}
	return page, rows.Err()
}

type backfillOutcome int

const (
	backfillFailed backfillOutcome = iota
	backfillWritten
	backfillSkipped
)

// isExistingAggregate reports the writer's refusal of a created event for a
// credential that has an earlier event (SQLSTATE 55000).
func isExistingAggregate(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55000"
}

// backfillOne writes one token's events in its own transaction, in a
// savepoint, under the token row's lock. It skips a token that is gone or that
// has an aggregate (the writer refuses with 55000); any other error is failed
// and the savepoint is rolled back.
func backfillOne(ctx context.Context, pool backfillDB, schema authschema.ValidatedIdentifier, id uuid.UUID) backfillOutcome {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return backfillFailed
	}
	defer func() { _ = tx.Rollback(ctx) }()
	token, err := scanToken(tx.QueryRow(ctx, `SELECT `+tokenColumns+` FROM external_ingest_tokens WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return backfillSkipped
	}
	if err != nil {
		return backfillFailed
	}
	events, err := storedLifecycleEvents(token)
	if err != nil {
		return backfillFailed
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT backfill_token`); err != nil {
		return backfillFailed
	}
	if err := audit.AppendPushLifecycle(ctx, tx, schema, events); err != nil {
		_, _ = tx.Exec(ctx, `ROLLBACK TO SAVEPOINT backfill_token`)
		if isExistingAggregate(err) {
			return backfillSkipped
		}
		return backfillFailed
	}
	if _, err := tx.Exec(ctx, `RELEASE SAVEPOINT backfill_token`); err != nil {
		return backfillFailed
	}
	if err := tx.Commit(ctx); err != nil {
		return backfillFailed
	}
	return backfillWritten
}

// storedLifecycleEvents are the events of a stored token that has none: the
// created event first, so an existing aggregate is refused at the first event,
// then for a revoked token the revoked event. Expiry is not an event here.
func storedLifecycleEvents(t tokenRow) ([]audit.PushLifecycleEvent, error) {
	organization, binding, source, err := pushevent.WireIDs(t.OrgID, t.SourceID)
	if err != nil {
		return nil, err
	}
	names, err := pushevent.ScopeNames(t.Scopes)
	if err != nil {
		return nil, err
	}
	var expires *time.Time
	if t.ExpiresAt != nil {
		at := t.ExpiresAt.UTC()
		expires = &at
	}
	events := []audit.PushLifecycleEvent{{EventType: externalpush.Created, AggregateID: t.ID.String(),
		OrganizationID: organization, Binding: binding, SourceID: source, Change: pushevent.CreatedChange(t.ID, names, expires)}}
	if t.RevokedAt != nil {
		revoked, err := revokedEvent(t)
		if err != nil {
			return nil, err
		}
		events = append(events, revoked)
	}
	return events, nil
}
