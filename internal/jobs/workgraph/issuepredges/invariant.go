package issuepredges

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

const (
	// FallbackInvariantBreachedLog is the WARN line of a build that left an
	// explicit_text or heuristic link beside a native one (CHAOS-9821 rule).
	FallbackInvariantBreachedLog = "issue-pr fallback invariant breached"
	// FallbackInvariantHeldLog is the Info line of a build that left none.
	FallbackInvariantHeldLog = "issue-pr fallback invariant held"
	// FallbackBesideNativeGauge is set on every check, 0 included, with the
	// attribute organization_id, so an alert can watch for a value above 0.
	FallbackBesideNativeGauge = "workgraph_issue_pr_fallback_beside_native_rows"
	// The same check, one gauge per side, so an alert from metrics alone names
	// which side breached. A row can be on more than one side, so the sides do
	// not add up to the total.
	FallbackBesideNativePullRequestGauge = "workgraph_issue_pr_fallback_beside_native_pull_request_rows"
	FallbackBesideNativeIssueGauge       = "workgraph_issue_pr_fallback_beside_native_issue_rows"
	FallbackPullRequestIssueSideGauge    = "workgraph_issue_pr_fallback_pull_request_issue_side_rows"
)

// InvariantOutcome is one check of the native-first rule for one organization.
type InvariantOutcome struct {
	OrganizationID string
	LinksRead      int
	Beside         issueprlinks.FallbacksBesideNative
	Duration       time.Duration
}

// CheckFallbackInvariant reads the organization's stored links after the
// build's link writers ran and reports every lower-tier link that sits beside
// a native one. It reports and never repairs: a nonzero count is a writer
// regression the retraction step must not hide.
func (service *Service) CheckFallbackInvariant(ctx context.Context, orgID string) (InvariantOutcome, error) {
	if service == nil || service.rawConn == nil {
		return InvariantOutcome{}, ErrUnavailable
	}
	if orgID == "" {
		return InvariantOutcome{}, ErrScopeRequired
	}
	started := service.now()
	stored, err := issueprlinks.ReadStoredLinks(ctx, service.rawConn, orgID)
	if err != nil {
		service.logger.Error("issue-pr fallback invariant: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return InvariantOutcome{OrganizationID: orgID}, err
	}
	outcome := InvariantOutcome{
		OrganizationID: orgID,
		LinksRead:      len(stored),
		Beside:         issueprlinks.CountFallbacksBesideNative(stored),
		Duration:       service.now().Sub(started),
	}
	reportFallbackInvariant(ctx, service.logger, outcome)
	return outcome, nil
}

func reportFallbackInvariant(ctx context.Context, logger *slog.Logger, outcome InvariantOutcome) {
	meter := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges")
	for _, gauge := range []struct {
		name, description string
		value             int
	}{
		{FallbackBesideNativeGauge, "explicit_text and heuristic work_graph_issue_pr rows the native-first rule forbids (their pull request or issue has a native link, or their issue side is a pull request), per organization, after a work-graph build. Must be 0.", outcome.Beside.Total},
		{FallbackBesideNativePullRequestGauge, "explicit_text and heuristic work_graph_issue_pr rows whose pull request has a native link, per organization, after a work-graph build. Must be 0.", outcome.Beside.ByPullRequest},
		{FallbackBesideNativeIssueGauge, "explicit_text and heuristic work_graph_issue_pr rows whose issue has a native link, per organization, after a work-graph build. Must be 0.", outcome.Beside.ByIssue},
		{FallbackPullRequestIssueSideGauge, "explicit_text and heuristic work_graph_issue_pr rows whose issue side is a pull request, per organization, after a work-graph build. Must be 0.", outcome.Beside.PullRequestIssueSide},
	} {
		instrument, err := meter.Int64Gauge(gauge.name, metric.WithUnit("{row}"), metric.WithDescription(gauge.description))
		if err != nil {
			logger.Error("issue-pr fallback invariant: gauge unavailable", slog.String("gauge", gauge.name), slog.Any("error", err))
			continue
		}
		instrument.Record(ctx, int64(gauge.value), metric.WithAttributes(attribute.String("organization_id", outcome.OrganizationID)))
	}
	attrs := []any{
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("links_read", outcome.LinksRead),
		slog.Int("beside_native_pull_request", outcome.Beside.ByPullRequest),
		slog.Int("beside_native_issue", outcome.Beside.ByIssue),
		slog.Int("pull_request_issue_side", outcome.Beside.PullRequestIssueSide),
		slog.Int("beside_native_total", outcome.Beside.Total),
		slog.Duration("duration", outcome.Duration),
	}
	if outcome.Beside.Total > 0 {
		logger.Warn(FallbackInvariantBreachedLog, attrs...)
		return
	}
	logger.Info(FallbackInvariantHeldLog, attrs...)
}
