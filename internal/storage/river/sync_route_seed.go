package riverstore

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

// The outcomes a migrate run reports for one sync-dispatch route row.
const (
	// SyncRouteMoved: the row held the retired Celery seed and this run moved
	// it to river (generation + 1).
	SyncRouteMoved = "moved"
	// SyncRoutePresent: the row is on river, not paused, with no rollback
	// route. Not changed.
	SyncRoutePresent = "present"
	// SyncRouteHeld: the row is in any other state. Not changed.
	SyncRouteHeld = "held"
	// SyncRouteMissing: the kind has no row. None is created.
	SyncRouteMissing = "missing"
)

// SyncRouteOutcome is what one migrate run found for one sync-dispatch kind,
// and whether it moved the row. Transport, RollbackTransport, Paused and
// Generation are the row as the run FOUND it (the zero value for a missing
// row); a moved row is on river at Generation + 1 afterwards. LiveClaims is
// counted only when the row held the retired Celery seed.
type SyncRouteOutcome struct {
	Kind              string
	Outcome           string
	Transport         string
	RollbackTransport string
	Paused            bool
	Generation        int64
	LiveClaims        int64
}

// The vocabulary of public.sync_dispatch_transport_routes this file reads
// (internal/syncdispatchcontract owns the names; this package cannot import
// it, for the reason MigrationOptions.CoordinatorGrants gives).
const (
	syncRouteCelery     = "celery"
	syncRouteRiver      = "river"
	syncRouteNoRollback = "none"
)

// syncDispatchKindPattern is the grammar of a sync-dispatch kind
// (contracts/sync-dispatch/v1/transport-routes.schema.json names four).
var syncDispatchKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// syncRouteRow is one route row's state.
type syncRouteRow struct {
	transport         string
	rollbackTransport string
	paused            bool
	generation        int64
}

// retiredCelerySeed reports whether the row is exactly the seed a fresh
// database holds since the Celery rollback route of the sync-dispatch kinds
// was retired (application revision 0132): transport celery, no rollback
// route, not paused. No Celery producer or consumer exists for these kinds in
// any deployment, and the route controller refuses to move such a row
// (ApplyCheckedIn reads it as drift), so nothing but a migration can.
func (row syncRouteRow) retiredCelerySeed() bool {
	return row.transport == syncRouteCelery && row.rollbackTransport == syncRouteNoRollback && !row.paused
}

// onCheckedInRoute reports whether the row is already where the contract
// puts a kind that runs on river with no rollback route.
func (row syncRouteRow) onCheckedInRoute() bool {
	return row.transport == syncRouteRiver && row.rollbackTransport == syncRouteNoRollback && !row.paused
}

// classifySyncRoute decides one row. Only the retired Celery seed with no
// live claim is moved: a paused row, a row that still names a rollback
// route, and a row with a live outbox claim are an operator's or a running
// consumer's state and stay as found.
func classifySyncRoute(row syncRouteRow, liveClaims int64) string {
	switch {
	case row.onCheckedInRoute():
		return SyncRoutePresent
	case row.retiredCelerySeed() && liveClaims == 0:
		return SyncRouteMoved
	default:
		return SyncRouteHeld
	}
}

func readSyncRouteRows(ctx context.Context, tx pgx.Tx, kinds []string, lock bool) (map[string]syncRouteRow, error) {
	statement := `
SELECT kind, transport, rollback_transport, paused, generation
FROM public.sync_dispatch_transport_routes
WHERE kind = ANY($1)
ORDER BY kind`
	if lock {
		statement += " FOR UPDATE"
	}
	rows, err := tx.Query(ctx, statement, kinds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := make(map[string]syncRouteRow, len(kinds))
	for rows.Next() {
		var kind string
		var row syncRouteRow
		if err := rows.Scan(&kind, &row.transport, &row.rollbackTransport, &row.paused, &row.generation); err != nil {
			return nil, err
		}
		found[kind] = row
	}
	return found, rows.Err()
}

// convergeSyncDispatchRoutes moves every route row of kinds that still holds
// the retired Celery seed to river, inside the migration's privilege
// transaction, and reports what it found for every kind.
//
// A database whose rows are all off the seed (every deployed database) costs
// one unlocked read and nothing else. Only when a seed row exists does the run
// take the locks the route controller takes for the same transition (the
// route rows FOR UPDATE, then sync_dispatch_outbox in SHARE ROW EXCLUSIVE
// mode), read the rows again under them and count the live claims, so a claim
// that is live blocks the move exactly as it blocks ApplyCheckedIn.
func convergeSyncDispatchRoutes(ctx context.Context, tx pgx.Tx, kinds []string) ([]SyncRouteOutcome, bool, error) {
	if len(kinds) == 0 {
		return nil, false, nil
	}
	var routesExist, outboxExists bool
	if err := tx.QueryRow(ctx, `
SELECT to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL,
       to_regclass('public.sync_dispatch_outbox') IS NOT NULL`).Scan(&routesExist, &outboxExists); err != nil {
		return nil, false, err
	}
	if !routesExist || !outboxExists {
		return nil, true, nil
	}
	found, err := readSyncRouteRows(ctx, tx, kinds, false)
	if err != nil {
		return nil, false, err
	}
	var seeds []string
	for _, kind := range kinds {
		if row, ok := found[kind]; ok && row.retiredCelerySeed() {
			seeds = append(seeds, kind)
		}
	}
	claims := map[string]int64{}
	if len(seeds) > 0 {
		locked, err := readSyncRouteRows(ctx, tx, seeds, true)
		if err != nil {
			return nil, false, err
		}
		if _, err := tx.Exec(ctx, `LOCK TABLE public.sync_dispatch_outbox IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return nil, false, err
		}
		for _, kind := range seeds {
			row, ok := locked[kind]
			if !ok {
				delete(found, kind)
				continue
			}
			found[kind] = row
			var live int64
			if err := tx.QueryRow(ctx, `
SELECT count(*)
FROM public.sync_dispatch_outbox
WHERE kind = $1
  AND claim_token IS NOT NULL
  AND claim_expires_at > now()`, kind).Scan(&live); err != nil {
				return nil, false, err
			}
			claims[kind] = live
		}
	}
	outcomes := make([]SyncRouteOutcome, 0, len(kinds))
	for _, kind := range kinds {
		row, ok := found[kind]
		if !ok {
			outcomes = append(outcomes, SyncRouteOutcome{Kind: kind, Outcome: SyncRouteMissing})
			continue
		}
		outcome := SyncRouteOutcome{
			Kind: kind, Outcome: classifySyncRoute(row, claims[kind]),
			Transport: row.transport, RollbackTransport: row.rollbackTransport,
			Paused: row.paused, Generation: row.generation, LiveClaims: claims[kind],
		}
		if outcome.Outcome == SyncRouteMoved {
			tag, err := tx.Exec(ctx, `
UPDATE public.sync_dispatch_transport_routes
SET transport = $2, generation = generation + 1, updated_at = now()
WHERE kind = $1 AND generation = $3`, kind, syncRouteRiver, row.generation)
			if err != nil {
				return nil, false, fmt.Errorf("move route %s: %w", kind, err)
			}
			if tag.RowsAffected() != 1 {
				return nil, false, errors.New("move route " + kind + ": the locked row changed")
			}
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes, false, nil
}
