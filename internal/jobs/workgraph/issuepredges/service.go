package issuepredges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// FastPathOutcome is one run's accounting for the fast-path edge builder
// (_build_issue_pr_edges_from_fast_path).
type FastPathOutcome struct {
	OrganizationID string
	RowsRead       int
	EdgesWritten   int
	Duration       time.Duration
}

// TextParseOutcome is one run's accounting for the text-parse builder
// (_build_issue_pr_edges).
type TextParseOutcome struct {
	OrganizationID   string
	PullRequestsRead int
	EdgesWritten     int
	LinksWritten     int
	JiraRefsFound    int
	GitHubRefsFound  int
	GitLabRefsFound  int
	Duration         time.Duration
}

// Service orchestrates both issue<->PR edge sub-builders against ClickHouse.
//
// Like prcommit.Service, this ports TWO sub-builders that must run in a fixed
// order (see the package doc's "Ordering" section): fast-path first, then
// text-parse, so a caller wiring both as pre-steps must register them in this
// order.
type Service struct {
	loader      *Loader
	rawConn     driver.Conn
	linksWriter *issueprlinks.Writer
	logger      *slog.Logger
	now         func() time.Time
}

// NewService wires the production path. rawConn is used directly for
// edges.WriteEdges (typed against driver.Conn, like every sibling producer in
// this family); linksWriter reuses issueprlinks.Writer for the explicit_text
// rows this package's text-parse half writes back to work_graph_issue_pr --
// there is no reason to duplicate that insert statement in a second package.
func NewService(loader *Loader, rawConn driver.Conn, linksWriter *issueprlinks.Writer, logger *slog.Logger) (*Service, error) {
	if loader == nil || rawConn == nil || linksWriter == nil {
		return nil, ErrUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{loader: loader, rawConn: rawConn, linksWriter: linksWriter, logger: logger, now: time.Now}, nil
}

// SetClock overrides the fallback clock. Test-only seam, same reasoning as
// prcommit.Service.SetClock.
func (service *Service) SetClock(now func() time.Time) {
	if service == nil || now == nil {
		return
	}
	service.now = now
}

// ProduceFastPathEdges runs _build_issue_pr_edges_from_fast_path for one org
// and window: read the work_graph_issue_pr x git_pull_requests join, write
// IMPLEMENTS edges. Must run BEFORE ProduceTextParseEdges -- see Service's
// doc comment.
func (service *Service) ProduceFastPathEdges(ctx context.Context, orgID string, window Window) (FastPathOutcome, error) {
	if service == nil || service.loader == nil || service.rawConn == nil {
		return FastPathOutcome{}, ErrUnavailable
	}
	started := service.now()

	rows, err := service.loader.LoadFastPath(ctx, orgID, window)
	if err != nil {
		service.logger.Error("issue-pr fast-path edges: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return FastPathOutcome{OrganizationID: orgID}, err
	}

	buildTime := service.now().UTC()
	edgeRows := DeriveFastPathEdges(rows, buildTime)

	written, err := edges.WriteEdges(ctx, service.rawConn, orgID, edgeRows)
	if err != nil {
		service.logger.Error("issue-pr fast-path edges: write failed",
			slog.String("organization_id", orgID),
			slog.Int("rows_read", len(rows)),
			slog.Any("error", err))
		return FastPathOutcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr fast-path edges: %w", err)
	}

	outcome := FastPathOutcome{
		OrganizationID: orgID,
		RowsRead:       len(rows),
		EdgesWritten:   written,
		Duration:       service.now().Sub(started),
	}
	service.logger.Info("issue-pr fast-path edges produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("rows_read", outcome.RowsRead),
		slog.Int("edges_written", outcome.EdgesWritten),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}

// ProduceTextParseEdges runs _build_issue_pr_edges for one org and window:
// scan PR text for Jira/GitHub/GitLab issue references, write edges AND the
// new fast-path links those references corroborate. Must run AFTER
// ProduceFastPathEdges -- see Service's doc comment.
func (service *Service) ProduceTextParseEdges(ctx context.Context, orgID string, window Window) (TextParseOutcome, error) {
	if service == nil || service.loader == nil || service.rawConn == nil || service.linksWriter == nil {
		return TextParseOutcome{}, ErrUnavailable
	}
	started := service.now()

	prs, workItems, err := service.loader.LoadTextParseInputs(ctx, orgID, window)
	if err != nil {
		service.logger.Error("issue-pr text-parse edges: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return TextParseOutcome{OrganizationID: orgID}, err
	}

	buildTime := service.now().UTC()
	result := DeriveTextParseEdges(prs, workItems, buildTime)

	for index := range result.Links {
		result.Links[index].OrgID = orgID
		result.Links[index].LastSynced = buildTime
	}

	edgesWritten, err := edges.WriteEdges(ctx, service.rawConn, orgID, result.Edges)
	if err != nil {
		service.logger.Error("issue-pr text-parse edges: write failed",
			slog.String("organization_id", orgID),
			slog.Int("prs_read", len(prs)),
			slog.Any("error", err))
		return TextParseOutcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr text-parse edges: %w", err)
	}

	if err := service.linksWriter.Write(ctx, result.Links); err != nil {
		service.logger.Error("issue-pr text-parse edges: link write failed",
			slog.String("organization_id", orgID),
			slog.Int("links_derived", len(result.Links)),
			slog.Any("error", err))
		return TextParseOutcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr text-parse links: %w", err)
	}

	outcome := TextParseOutcome{
		OrganizationID:   orgID,
		PullRequestsRead: len(prs),
		EdgesWritten:     edgesWritten,
		LinksWritten:     len(result.Links),
		JiraRefsFound:    result.JiraRefsFound,
		GitHubRefsFound:  result.GitHubRefsFound,
		GitLabRefsFound:  result.GitLabRefsFound,
		Duration:         service.now().Sub(started),
	}
	service.logger.Info("issue-pr text-parse edges produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("prs_read", outcome.PullRequestsRead),
		slog.Int("edges_written", outcome.EdgesWritten),
		slog.Int("links_written", outcome.LinksWritten),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}

// HeuristicOutcome is one run's accounting for the heuristic builder
// (_build_heuristic_issue_pr_edges).
type HeuristicOutcome struct {
	OrganizationID   string
	WorkItemsRead    int
	PullRequestsRead int
	EdgesWritten     int
	LinksWritten     int
	Duration         time.Duration
}

// ProduceHeuristicEdges runs _build_heuristic_issue_pr_edges for one org and
// window: read work items, PRs, and the CURRENT explicit-links set fresh
// from work_graph_issue_pr (see ExplicitLink's doc comment), then write
// heuristic RELATES edges plus their own fast-path links. Must run AFTER
// ProduceFastPathEdges and ProduceTextParseEdges in the same build -- their
// rows are exactly what this method's explicit-links read excludes.
func (service *Service) ProduceHeuristicEdges(ctx context.Context, orgID string, window Window) (HeuristicOutcome, error) {
	if service == nil || service.loader == nil || service.rawConn == nil || service.linksWriter == nil {
		return HeuristicOutcome{}, ErrUnavailable
	}
	started := service.now()

	workItems, pullRequests, explicitLinks, err := service.loader.LoadHeuristicInputs(ctx, orgID, window)
	if err != nil {
		service.logger.Error("issue-pr heuristic edges: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return HeuristicOutcome{OrganizationID: orgID}, err
	}

	buildTime := service.now().UTC()
	result := DeriveHeuristicEdges(HeuristicInputs{
		WorkItems:           workItems,
		PullRequests:        pullRequests,
		ExplicitLinks:       explicitLinks,
		HeuristicDaysWindow: window.HeuristicDaysWindow,
		HeuristicConfidence: window.HeuristicConfidence,
	}, buildTime)

	for index := range result.Links {
		result.Links[index].OrgID = orgID
		result.Links[index].LastSynced = buildTime
	}

	edgesWritten, err := edges.WriteEdges(ctx, service.rawConn, orgID, result.Edges)
	if err != nil {
		service.logger.Error("issue-pr heuristic edges: write failed",
			slog.String("organization_id", orgID),
			slog.Int("work_items_read", len(workItems)),
			slog.Any("error", err))
		return HeuristicOutcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr heuristic edges: %w", err)
	}

	if err := service.linksWriter.Write(ctx, result.Links); err != nil {
		service.logger.Error("issue-pr heuristic edges: link write failed",
			slog.String("organization_id", orgID),
			slog.Int("links_derived", len(result.Links)),
			slog.Any("error", err))
		return HeuristicOutcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr heuristic links: %w", err)
	}

	outcome := HeuristicOutcome{
		OrganizationID:   orgID,
		WorkItemsRead:    len(workItems),
		PullRequestsRead: len(pullRequests),
		EdgesWritten:     edgesWritten,
		LinksWritten:     len(result.Links),
		Duration:         service.now().Sub(started),
	}
	service.logger.Info("issue-pr heuristic edges produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("work_items_read", outcome.WorkItemsRead),
		slog.Int("prs_read", outcome.PullRequestsRead),
		slog.Int("edges_written", outcome.EdgesWritten),
		slog.Int("links_written", outcome.LinksWritten),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}
