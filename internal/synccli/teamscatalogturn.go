package synccli

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

const (
	catalogTurnNotTakenEvent = "team catalog turn not taken"
	catalogTurnWaitEvent     = "waiting for the team catalog turn"
	catalogTurnTakenEvent    = "team catalog turn taken"

	catalogTurnReasonNoDSN      = "no_postgres_dsn"
	catalogTurnReasonOrgNotUUID = "org_not_uuid"

	catalogTurnNotExcluded = "a concurrent worker or CLI run of this organization and provider is not excluded"
)

// catalogTurn wraps the shared team catalog turn (jobruntime.TeamCatalogTurn, the lease the worker takes) for the CLI:
// it says that it waits and for how long, and remembers that taking the turn failed so the verb can tell that from a
// failed collection. It keeps the lease-loss signal of the turn it wraps.
type catalogTurn struct {
	inner    providersync.TeamCatalogSerializer
	logger   *slog.Logger
	provider string
	failed   atomic.Bool
}

func (turn *catalogTurn) acquireFailed() bool { return turn != nil && turn.failed.Load() }

func (turn *catalogTurn) Serialize(ctx context.Context, orgID, provider string) (func(), error) {
	release, _, err := turn.SerializeWithLoss(ctx, orgID, provider)
	return release, err
}

func (turn *catalogTurn) SerializeWithLoss(ctx context.Context, orgID, provider string) (func(), <-chan struct{}, error) {
	turn.logger.InfoContext(ctx, catalogTurnWaitEvent, "provider", provider, "org_id", orgID)
	started := time.Now()
	var (
		release func()
		lost    <-chan struct{}
		err     error
	)
	if aware, ok := turn.inner.(providersync.TeamCatalogLossAwareSerializer); ok {
		release, lost, err = aware.SerializeWithLoss(ctx, orgID, provider)
	} else {
		release, err = turn.inner.Serialize(ctx, orgID, provider)
	}
	if err != nil {
		turn.failed.Store(true)
		return nil, nil, err
	}
	turn.logger.InfoContext(ctx, catalogTurnTakenEvent, "provider", provider, "org_id", orgID, "waited_ms", time.Since(started).Milliseconds())
	return release, lost, nil
}

// openCatalogTurn gives the run the turn the worker takes for the same organization and provider, when it can: with a
// PostgreSQL DSN and a UUID organization it opens the pool the lease lives in; without either it warns what is not
// excluded and runs without one. A DSN that cannot be opened fails the run (turn_unavailable): a run that was asked to
// take the turn never runs without it. The returned func closes the pool; the nil turn is "no turn".
func openCatalogTurn(ctx context.Context, env cli.Env, d deps, request catalogRequest, logger *slog.Logger) (*catalogTurn, func(), int) {
	none := func() {}
	if request.pgDSN == "" {
		logger.WarnContext(ctx, catalogTurnNotTakenEvent, "provider", request.provider, "org_id", request.orgID,
			"reason", catalogTurnReasonNoDSN, "detail", "no PostgreSQL DSN (pass --db or set "+PostgresURIKey+"): "+catalogTurnNotExcluded)
		return nil, none, 0
	}
	if _, err := uuid.Parse(request.orgID); err != nil {
		logger.WarnContext(ctx, catalogTurnNotTakenEvent, "provider", request.provider, "org_id", request.orgID,
			"reason", catalogTurnReasonOrgNotUUID, "detail", "the organization id is not a UUID, so it cannot key a lease: "+catalogTurnNotExcluded)
		return nil, none, 0
	}
	pool, err := d.openPostgres(ctx, request.pgDSN)
	if err != nil {
		return nil, none, writeError(env.Stderr, cli.ExitFailure, "turn_unavailable", err.Error())
	}
	return &catalogTurn{inner: d.catalogTurn(pool), logger: logger, provider: request.provider}, pool.Close, 0
}
