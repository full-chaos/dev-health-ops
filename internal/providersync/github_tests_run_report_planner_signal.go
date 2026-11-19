package providersync

import (
	"context"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const githubTestsPlannerKeptAggregateName = "github_tests_run_report_planner_kept_aggregate_total"

// The outcomes of github_tests_run_report_planner_kept_aggregate_total: an aggregate artifact the planner kept with its parts
// without a proof that dropping it loses nothing.
const (
	githubTestsPlannerPartitionBoundExhausted = "partition_bound_exhausted"
	githubTestsPlannerDropUndoneByAdmission   = "drop_undone_by_admission"
)

var githubTestsPlannerKeptAggregate = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		githubTestsPlannerKeptAggregateName,
		metric.WithDescription("GitHub tests run report artifacts kept beside their parts because the partition search ran out of work or a drop was undone by the run budget."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(githubTestsPlannerKeptAggregateName)
	}
	return counter
}()

// reportGitHubTestsPlannerKeeps counts each artifact the plan kept silently and logs one WARN per run and outcome.
func reportGitHubTestsPlannerKeeps(ctx context.Context, provider, dataset, unit, repo, runID string, parsed []githubTestsParsedReport, plan githubTestsRunReportPlan) {
	for _, outcome := range []struct {
		name    string
		indexes []int
	}{
		{githubTestsPlannerPartitionBoundExhausted, plan.BoundExhausted},
		{githubTestsPlannerDropUndoneByAdmission, plan.UndoneByAdmission},
	} {
		if len(outcome.indexes) == 0 {
			continue
		}
		ids := make([]string, 0, len(outcome.indexes))
		for _, index := range outcome.indexes {
			ids = append(ids, string(parsed[index].ID))
		}
		githubTestsPlannerKeptAggregate.Add(ctx, int64(len(ids)), metric.WithAttributes(attribute.String("outcome", outcome.name)))
		slog.Warn(
			"provider run report artifacts kept beside their parts: rows are stored twice",
			"provider", provider, "dataset", dataset, "unit", unit, "repository", logging.ProviderText(repo), logging.ProviderIDAttr("run", runID),
			"outcome", outcome.name, "artifact_ids", strings.Join(ids, ","),
		)
	}
}
