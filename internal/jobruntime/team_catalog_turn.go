package jobruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// teamCatalogBudget is the one method of the worker's durable concurrency lease the turn needs
// (PostgresConcurrencyBudget): a test can stand in for it.
type teamCatalogBudget interface {
	Acquire(ctx context.Context, request BudgetRequest) (BudgetLease, error)
}

// TeamCatalogTurn gives one run of a (organization, provider) team catalog its turn
// (CHAOS-9140, D5917): a durable lease of the worker's own concurrency facility
// (worker_concurrency_leases: expiry, token-fenced release, renewal while the run lives,
// takeover after a crash). Two overlapping runs of one integration each read the open
// membership rows, found none for a NEW fact and each inserted one with its own clock:
// two open rows. A run that finds the turn taken polls until it is free or its context
// ends: delayed, never skipped, so it reads the rows of the first run and stamps by the
// earliest.
//
// NO connection is held while a run lives: acquiring is one short transaction (count and
// insert under an advisory xact lock, safe behind a transaction-mode PgBouncer), then the
// lease is a row and a renewal. So it works at a domain pool of one connection and costs
// no pooler budget. A lease the run LOSES (renewal fails) cancels the run (round 117 P1:
// an overlap is the defect this turn exists to prevent); the job retries.
type TeamCatalogTurn struct {
	budget teamCatalogBudget
}

// NewTeamCatalogTurn builds the turn of a domain pool. The worker and the operator CLI (`dho sync teams`) both take it, so they serialize on one key. A nil pool is a wiring error that
// every run reports, not a silent run without the turn.
func NewTeamCatalogTurn(pool *pgxpool.Pool) TeamCatalogTurn {
	if pool == nil {
		return TeamCatalogTurn{}
	}
	budget, err := NewPostgresConcurrencyBudget(pool, nil)
	if err != nil {
		return TeamCatalogTurn{}
	}
	return TeamCatalogTurn{budget: budget}
}

func (turn TeamCatalogTurn) Serialize(ctx context.Context, orgID, provider string) (func(), error) {
	release, _, err := turn.SerializeWithLoss(ctx, orgID, provider)
	return release, err
}

// leaseLoss is the optional signal of a budget lease whose renewal failed (jobruntime).
type leaseLoss interface{ Lost() <-chan struct{} }

// SerializeWithLoss is Serialize plus the loss signal of the lease: a run that LOSES its lease is
// cancelled by the carry wrapper (providersync.ErrTeamCatalogTurnLost), never left to overlap the
// next holder; the job retries. A budget that cannot lose its lease gives a nil signal.
func (turn TeamCatalogTurn) SerializeWithLoss(ctx context.Context, orgID, provider string) (func(), <-chan struct{}, error) {
	if turn.budget == nil {
		return nil, nil, errors.New("team catalog turn: no Postgres pool")
	}
	lease, err := turn.budget.Acquire(ctx, BudgetRequest{
		Kind:             "team_catalog." + provider,
		OrganizationID:   &orgID,
		ConcurrencyScope: "organization",
		ConcurrencyLimit: 1,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("acquire the catalog lease: %w", err)
	}
	var lost <-chan struct{}
	if withLoss, ok := lease.(leaseLoss); ok {
		lost = withLoss.Lost()
	}
	return lease.Release, lost, nil
}
