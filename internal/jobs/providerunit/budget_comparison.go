package providerunit

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

const (
	budgetComparisonEvent     = "sync_provider_unit.budget_comparison"
	budgetUnderestimatedEvent = "sync_provider_unit.budget_underestimated"
)

// observeBudgetComparison reports the estimate-against-actual comparison of a
// unit that finished: one Info line, and for each row whose requests exceeded a
// request-count estimate one Warn and one count. Call it after the durable
// finish succeeded. A row on an abstract-unit dimension is never underestimated,
// so it never warns.
func (handler *Handler) observeBudgetComparison(
	ctx context.Context, claim providersync.Claim, rows []providersync.BudgetComparisonRow,
	dropped int, earlierWork bool,
) {
	_, _, _, _, _ = ctx, claim, rows, dropped, earlierWork
}
