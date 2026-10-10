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
	pool  *pgxpool.Pool
	poll  time.Duration
	slots chan struct{}
}

// NewPostgresTeamCatalogTurn builds the turn of a pool. poll is the wait between two
// tries; zero means 500 ms. The in-process slots bound how many runs of this process
// hold a turn at once (holdersFor).
func NewPostgresTeamCatalogTurn(pool *pgxpool.Pool, poll time.Duration) *PostgresTeamCatalogTurn {
	turn := &PostgresTeamCatalogTurn{pool: pool, poll: poll}
	if pool != nil {
		if holders := holdersFor(pool); holders > 0 {
			turn.slots = make(chan struct{}, holders)
		}
	}
	return turn
}

// holdersFor is how many runs may hold a turn at once on a pool: one connection of the
// pool always stays free for the queries the holders themselves make (the collectors
// read and write Postgres through the SAME pool while they hold the turn: scope census,
// selections, clients). With every connection held by a turn, each holder would wait
// for a connection that only another holder can free (round 111: four sync workers,
// four domain connections). A pool of one connection cannot hold a turn at all.
func holdersFor(pool *pgxpool.Pool) int {
	return int(pool.Config().MaxConns) - 1
}

const teamCatalogTurnKeySQL = `hashtextextended('team_catalog:' || $1::text || ':' || $2::text, 0)`

func (turn *PostgresTeamCatalogTurn) Serialize(ctx context.Context, orgID, provider string) (func(), error) {
	if turn.pool == nil {
		return nil, errors.New("team catalog turn: no Postgres pool")
	}
	poll := turn.poll
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	if turn.slots == nil {
		return nil, errors.New("team catalog turn: the pool has one connection; a held turn would leave none for the run's own queries")
	}
	select {
	case turn.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	slotFree := func() { <-turn.slots }
	for {
		tx, err := turn.pool.Begin(ctx)
		if err != nil {
			slotFree()
			return nil, fmt.Errorf("begin the turn transaction: %w", err)
		}
		var got bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(`+teamCatalogTurnKeySQL+`)`, orgID, provider).Scan(&got); err != nil {
			_ = tx.Rollback(context.Background())
			slotFree()
			return nil, fmt.Errorf("try the turn: %w", err)
		}
		if got {
			return func() { releaseTurn(tx); slotFree() }, nil
		}
		_ = tx.Rollback(context.Background())
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			slotFree()
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
