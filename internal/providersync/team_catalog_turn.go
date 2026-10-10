package providersync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresTeamCatalogTurn gives one run of a (organization, provider) team catalog
// its turn (CHAOS-9140): a Postgres session advisory lock held on a pooled
// connection for the run. The read of the open membership rows and the insert
// of a new fact are not one step, so two overlapping runs each stamped a new
// fact and left two open rows. A run that finds the turn taken polls (the
// connection is released between polls, so a waiting run holds none) until the
// turn is free or its context ends: it is delayed, never skipped, so a delayed
// run still reads the rows the first run wrote and stamps by the earliest.
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
		conn, err := turn.Pool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquire connection: %w", err)
		}
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(`+teamCatalogTurnKeySQL+`)`, orgID, provider).Scan(&got); err != nil {
			conn.Release()
			return nil, fmt.Errorf("try the turn: %w", err)
		}
		if got {
			return func() {
				unlockCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var released bool
				err := conn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock(`+teamCatalogTurnKeySQL+`)`, orgID, provider).Scan(&released)
				if err != nil || !released {
					// A session lock that cannot be proven released leaves with its
					// connection: closing the session frees it.
					_ = conn.Conn().Close(unlockCtx)
				}
				conn.Release()
			}, nil
		}
		conn.Release()
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
