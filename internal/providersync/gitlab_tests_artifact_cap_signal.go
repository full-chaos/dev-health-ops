package providersync

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const gitLabTestsArtifactJobsOverCapName = "gitlab_tests_artifact_jobs_over_cap_total"

// The one outcome of gitlab_tests_artifact_jobs_over_cap_total: a pipeline had more artifact jobs than the route reads.
const gitLabTestsArtifactJobsOverCapOutcome = "artifact_jobs_over_cap"

var gitLabTestsArtifactJobsOverCapCounter = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		gitLabTestsArtifactJobsOverCapName,
		metric.WithDescription("GitLab pipelines with more report artifact jobs than the route reads; the jobs past the cap are not read."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(gitLabTestsArtifactJobsOverCapName)
	}
	return counter
}()

// reportGitLabTestsArtifactJobsOverCap counts one pipeline whose artifact jobs passed the cap and logs one WARN for it.
func reportGitLabTestsArtifactJobsOverCap(ctx context.Context, provider, dataset, unit, repo, runID string, jobs, capJobs int) {
	gitLabTestsArtifactJobsOverCapCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", gitLabTestsArtifactJobsOverCapOutcome)))
	slog.Warn(
		"gitlab pipeline artifact jobs over the cap: the jobs past the cap are not read",
		"provider", provider, "dataset", dataset, "unit", unit, "repository", repo, logging.ProviderIDAttr("run", runID),
		"jobs", jobs, "cap", capJobs,
	)
}
