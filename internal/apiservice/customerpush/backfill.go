package customerpush

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// BackfillLifecycle emits the lifecycle events of tokens that have no aggregate
// (they predate the events). Without apply it only counts. With apply, each
// token is written in its own transaction under the token row's lock, through
// the same AppendPushLifecycle as the routes, so a rerun finishes what a failed
// run left and a token a route already wrote is skipped. A token that fails is
// counted and the run goes on.
func BackfillLifecycle(ctx context.Context, pool *pgxpool.Pool, schema string, apply bool, batch int) (BackfillReport, error) {
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
		page, err := backfillPage(ctx, pool, ident, after, batch)
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
			wrote, err := backfillOne(ctx, pool, ident, c.id)
			switch {
			case ctx.Err() != nil:
				return report, ctx.Err()
			case err != nil:
				report.Failed++
			case wrote:
				report.Written++
			default:
				report.Skipped++
			}
		}
		after = page[len(page)-1].id
	}
}

func backfillPage(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, after uuid.UUID, batch int) ([]backfillCandidate, error) {
	rows, err := pool.Query(ctx, `SELECT t.id, t.source_id IS NULL, t.revoked_at IS NOT NULL
		FROM external_ingest_tokens t
		WHERE t.id > $1 AND NOT EXISTS (SELECT 1 FROM `+authschema.Quote(schema)+`.aggregate_revisions a
			WHERE a.aggregate_type = $2 AND a.aggregate_id = t.id::text)
		ORDER BY t.id LIMIT $3`, after, externalpush.Aggregate, batch)
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

// backfillOne writes one token's events in its own transaction. It reports
// false when the token is gone or already has an aggregate once its row is locked.
func backfillOne(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, id uuid.UUID) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	token, err := scanToken(tx.QueryRow(ctx, `SELECT `+tokenColumns+` FROM external_ingest_tokens WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var has bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+authschema.Quote(schema)+`.aggregate_revisions
		WHERE aggregate_type = $1 AND aggregate_id = $2)`, externalpush.Aggregate, id.String()).Scan(&has); err != nil {
		return false, err
	}
	if has {
		return false, nil
	}
	events, err := storedLifecycleEvents(token)
	if err != nil {
		return false, err
	}
	if err := audit.AppendPushLifecycle(ctx, tx, schema, events); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// storedLifecycleEvents are the events of a stored token that has none: the
// created event, or for a revoked token the revoked event, whose snapshot makes
// the writer emit created first. Expiry is not an event here.
func storedLifecycleEvents(t tokenRow) ([]audit.PushLifecycleEvent, error) {
	if t.RevokedAt != nil {
		event, err := revokedEvent(t)
		if err != nil {
			return nil, err
		}
		return []audit.PushLifecycleEvent{event}, nil
	}
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
	return []audit.PushLifecycleEvent{{EventType: externalpush.Created, AggregateID: t.ID.String(),
		OrganizationID: organization, Binding: binding, SourceID: source, Change: pushevent.CreatedChange(t.ID, names, expires)}}, nil
}
