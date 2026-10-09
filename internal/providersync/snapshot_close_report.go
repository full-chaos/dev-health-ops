package providersync

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// snapshotCloseAbandonedName counts, per provider, fact kind and reason, the
// runs whose snapshot rule gave up the close of a kind and so kept open rows
// of it (PlanSnapshot): the kind's proof did not hold, or its answer was empty.
const snapshotCloseAbandonedName = "team_catalog_snapshot_close_abandoned_total"

// SnapshotCloseAbandonedLog is the message of the WARN line of an abandoned close.
const SnapshotCloseAbandonedLog = "team_catalog_snapshot_close_abandoned"

// ReportSnapshotPlan makes every abandoned close of the plan loud: one WARN
// line per kind and one count per reason. A kind whose answer was empty over a
// table that held none of its rows protected nothing and is not reported. It
// returns whether any kind was reported.
func ReportSnapshotPlan(ctx context.Context, provider, orgID string, plan SnapshotPlan) bool {
	abandoned := plan.Abandoned()
	if len(abandoned) == 0 {
		return false
	}
	var counter metric.Int64Counter
	if built, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		snapshotCloseAbandonedName,
		metric.WithDescription("Team catalog runs whose snapshot rule closed no row of a fact kind, by reason: the kind's walk end was not proven or its answer was empty.")); err == nil {
		counter = built
	}
	for _, outcome := range abandoned {
		slog.Default().WarnContext(ctx, SnapshotCloseAbandonedLog,
			"org_id", orgID, "provider", provider, "kind", outcome.Kind,
			"reasons", strings.Join(outcome.Abandoned, ","), "fresh_rows", outcome.Fresh, "open_rows_kept", outcome.Open,
			"open_rows_of_no_kind", plan.OpenOfNoKind)
		if counter == nil {
			continue
		}
		for _, reason := range outcome.Abandoned {
			counter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("provider", provider), attribute.String("kind", outcome.Kind), attribute.String("reason", reason)))
		}
	}
	return true
}
