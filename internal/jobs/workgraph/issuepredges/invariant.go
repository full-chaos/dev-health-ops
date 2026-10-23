package issuepredges

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

const (
	// FallbackInvariantBreachedLog is the WARN line of a build that left an
	// explicit_text or heuristic link beside a native one (CHAOS-9821 rule).
	FallbackInvariantBreachedLog = "issue-pr fallback invariant breached"
	// FallbackInvariantHeldLog is the Info line of a build that left none.
	FallbackInvariantHeldLog = "issue-pr fallback invariant held"
	// FallbackBesideNativeCounter adds the rows of every check, 0 included. It
	// carries no attribute: the organization is named by the log line only, so
	// an alert watches for an increase above 0.
	FallbackBesideNativeCounter = "workgraph_issue_pr_fallback_beside_native_rows_total"
	// The same check, one counter per side, so an alert from metrics alone names
	// which side breached. A row can be on more than one side, so the sides do
	// not add up to the total.
	FallbackBesideNativePullRequestCounter = "workgraph_issue_pr_fallback_beside_native_pull_request_rows_total"
	FallbackBesideNativeIssueCounter       = "workgraph_issue_pr_fallback_beside_native_issue_rows_total"
	FallbackPullRequestIssueSideCounter    = "workgraph_issue_pr_fallback_pull_request_issue_side_rows_total"
	// FallbackInvariantChecksCounter counts every check that ran, so "no
	// violation" differs from "the check did not run".
	FallbackInvariantChecksCounter = "workgraph_issue_pr_fallback_invariant_checks_total"
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
			slog.String("organization_id", orgID), workgraph.ErrorAttr(err))
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
	for _, counter := range []struct {
		name, description, unit string
		value                   int
	}{
		{FallbackBesideNativeCounter, "explicit_text and heuristic work_graph_issue_pr rows the native-first rule forbids (their pull request or issue has a native link, or their issue side is a pull request), added after every work-graph build. Must not increase.", "{row}", outcome.Beside.Total},
		{FallbackBesideNativePullRequestCounter, "explicit_text and heuristic work_graph_issue_pr rows whose pull request has a native link, added after every work-graph build. Must not increase.", "{row}", outcome.Beside.ByPullRequest},
		{FallbackBesideNativeIssueCounter, "explicit_text and heuristic work_graph_issue_pr rows whose issue has a native link, added after every work-graph build. Must not increase.", "{row}", outcome.Beside.ByIssue},
		{FallbackPullRequestIssueSideCounter, "explicit_text and heuristic work_graph_issue_pr rows whose issue side is a pull request, added after every work-graph build. Must not increase.", "{row}", outcome.Beside.PullRequestIssueSide},
		{FallbackInvariantChecksCounter, "Native-first rule checks that ran after a work-graph build; one per check, whatever the result.", "{check}", 1},
	} {
		instrument, err := meter.Int64Counter(counter.name, metric.WithUnit(counter.unit), metric.WithDescription(counter.description))
		if err != nil {
			logger.Error("issue-pr fallback invariant: counter unavailable", slog.String("counter", counter.name), workgraph.ErrorAttr(err))
			continue
		}
		instrument.Add(ctx, int64(counter.value))
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
