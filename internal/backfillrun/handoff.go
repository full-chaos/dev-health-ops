package backfillrun

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// The occurrence and manual-trigger write, and the bounded wait for the scheduler,
// are the shared seam in synchandoff (the integration sync and backfill routes use
// it too). These names keep the verb's own vocabulary.
type (
	Config  = synchandoff.Config
	Trigger = synchandoff.Trigger
	State   = synchandoff.State
	Outcome = synchandoff.Outcome
)

const (
	StatePending      = synchandoff.StatePending
	StateMaterialized = synchandoff.StateMaterialized
	StateQuarantined  = synchandoff.StateQuarantined
	StateTerminal     = synchandoff.StateTerminal
)

// LoadConfig reads the sync configuration the verb acts on.
func LoadConfig(ctx context.Context, tx pgx.Tx, configID string) (*Config, error) {
	return synchandoff.LoadConfig(ctx, tx, configID)
}

// Wait polls the occurrence until the scheduler has planned it, quarantined it,
// or the wait is over.
func Wait(ctx context.Context, pool *pgxpool.Pool, occurrenceID string, wait, poll time.Duration) (Outcome, error) {
	return synchandoff.Wait(ctx, pool, occurrenceID, wait, poll)
}

// Mint writes a backfill occurrence and its manual trigger in the transaction.
func Mint(ctx context.Context, tx pgx.Tx, config *Config, params Params, validated Validated, now time.Time) (Trigger, error) {
	since, before := params.Window.Since, params.Window.Before
	return synchandoff.Mint(ctx, tx, config, synchandoff.MintInput{
		Mode: "backfill", Since: &since, Before: &before,
		SourceIDs: validated.SourceIDs, DatasetKeys: validated.DatasetKeys, TriggeredBy: "backfill",
	}, now)
}
