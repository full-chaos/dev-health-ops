package providersync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresTeamCatalogTurn gives one run of a (organization, provider) team
// catalog its turn (CHAOS-9140): a transaction-scoped advisory lock
// (pg_try_advisory_xact_lock) taken inside ONE transaction that stays open for
// the whole run. The read of the open membership rows and the insert of a new
// fact are not one step, so two overlapping runs each stamped a new fact and
// left two open rows.
//
// The pool is the DOMAIN pool, which may traverse a transaction-mode PgBouncer
// (config DomainTransactionPooler, compose PGBOUNCER_TRANSACTION_MODE). A
// SESSION advisory lock does not survive that: the server session is handed to
// another client after the statement and the unlock can land on another one.
// A transaction pins its server session for the transaction's life in
// transaction mode, so the xact lock is held by exactly the caller, and it is
// released by the transaction's end (Rollback), on any path, including a lost
// connection. Cost: one server connection of the domain pool is held for the
// length of a run (no other holder). A run that finds the turn taken ends its
// transaction and polls (holding no connection between tries) until the turn is
// free or its context ends: it is delayed, never skipped, so a delayed run still
// reads the rows the first run wrote and stamps by the earliest.
type PostgresTeamCatalogTurn struct {
	Pool *pgxpool.Pool
	// Poll is the wait between two tries; zero means 500 ms.
	Poll time.Duration
}

const teamCatalogTurnKeySQL = `hashtextextended('team_catalog:' || $1::text || ':' || $2::text, 0)`

func (turn PostgresTeamCatalogTurn) Serialize(ctx context.Context, orgID, provider string) (func(), error) {
	if turn.Pool == nil {
		return nil, errors.New("team catalog turn: no Postgres pool")
	}
	poll := turn.Poll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	for {
		tx, err := turn.Pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin the turn transaction: %w", err)
		}
		var got bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(`+teamCatalogTurnKeySQL+`)`, orgID, provider).Scan(&got); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, fmt.Errorf("try the turn: %w", err)
		}
		if got {
			return func() { releaseTurn(tx) }, nil
		}
		_ = tx.Rollback(context.Background())
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// releaseTurn ends the turn's transaction; the lock goes with it. A rollback
// that fails leaves the connection to the pool's own health check and the
// server's end of the session, which frees the lock either way.
func releaseTurn(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
