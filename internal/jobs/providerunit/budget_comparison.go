package providerunit

import (
	"context"
	"log/slog"

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
	if handler == nil || len(rows) == 0 {
		return
	}
	assessable, underestimated := 0, 0
	for _, row := range rows {
		if row.UnderestimationAssessable {
			assessable++
		}
		if row.Underestimated {
			underestimated++
		}
	}
	logger := slog.Default()
	logger.InfoContext(ctx, budgetComparisonEvent,
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"rows", len(rows), "assessable", assessable, "underestimated", underestimated,
		"dropped_requests", dropped, "earlier_work", earlierWork,
		"attempt_scope", providersync.BudgetComparisonAttemptScope)
	for _, row := range rows {
		if !row.Underestimated {
			continue
		}
		reason := "underestimated"
		if row.UnbudgetedActual {
			reason = "unbudgeted_actual"
		}
		ratio := 0.0
		if row.Ratio != nil {
			ratio = *row.Ratio
		}
		handler.ProviderMetrics.RecordBudgetUnderestimate(claim.Provider, claim.Dataset, row.Dimension)
		logger.WarnContext(ctx, budgetUnderestimatedEvent,
			"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
			"route_family", row.RouteFamily, "dimension", row.Dimension,
			"estimated_units", row.EstimatedUnits, "actual_requests", row.ActualRequests,
			"ratio", ratio, "incomplete", row.Incomplete, "family_resolved", row.FamilyResolved,
			"reason", reason)
	}
}
