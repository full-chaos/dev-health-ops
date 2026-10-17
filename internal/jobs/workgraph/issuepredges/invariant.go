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
	// FallbackBesideNativeGauge is set on every check, 0 included, so an alert
	// can watch for a value above 0 per organization.
	FallbackBesideNativeGauge = "workgraph_issue_pr_fallback_beside_native_rows"
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
	gauge, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges").Int64Gauge(
		FallbackBesideNativeGauge,
		metric.WithUnit("{row}"),
		metric.WithDescription("explicit_text and heuristic work_graph_issue_pr rows whose pull request or issue has a native link, per organization, after a work-graph build. Must be 0."))
	if err == nil {
		gauge.Record(ctx, int64(outcome.Beside.Total), metric.WithAttributes(attribute.String("org_id", outcome.OrganizationID)))
	} else {
		logger.Error("issue-pr fallback invariant: gauge unavailable", slog.Any("error", err))
	}
	attrs := []any{
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("links_read", outcome.LinksRead),
		slog.Int("beside_native_pull_request", outcome.Beside.ByPullRequest),
		slog.Int("beside_native_issue", outcome.Beside.ByIssue),
		slog.Int("beside_native_total", outcome.Beside.Total),
		slog.Duration("duration", outcome.Duration),
	}
	if outcome.Beside.Total > 0 {
		logger.Warn(FallbackInvariantBreachedLog, attrs...)
		return
	}
	logger.Info(FallbackInvariantHeldLog, attrs...)
}
