package issuecommitedges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// Outcome is one run's accounting for _build_issue_commit_edges_from_text_parsing.
type Outcome struct {
	OrganizationID string
	CommitsScanned int
	EdgesWritten   int
	Duration       time.Duration
}

// Service orchestrates the issue<->commit text-parse sub-builder against
// ClickHouse. One producing method, no fast-path/link split -- structurally
// closer to issueprlinks.Service's single-Produce shape than prcommit's
// two-method one, since this sub-builder reads git_commits/work_items and
// writes edges directly, with no intermediate table of its own.
type Service struct {
	loader  *Loader
	rawConn driver.Conn
	logger  *slog.Logger
	now     func() time.Time
}

// NewService wires the production path.
func NewService(loader *Loader, rawConn driver.Conn, logger *slog.Logger) (*Service, error) {
	if loader == nil || rawConn == nil {
		return nil, ErrUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{loader: loader, rawConn: rawConn, logger: logger, now: time.Now}, nil
}

// SetClock overrides the fallback clock. Test-only seam, same reasoning as
// prcommit.Service.SetClock / issueprlinks.Service.SetClock.
func (service *Service) SetClock(now func() time.Time) {
	if service == nil || now == nil {
		return
	}
	service.now = now
}

// Produce runs _build_issue_commit_edges_from_text_parsing for one org and
// window: read commits + work items, extract jira/github/gitlab issue
// references from commit messages, write COMMIT->ISSUE edges.
func (service *Service) Produce(ctx context.Context, orgID string, window Window) (Outcome, error) {
	if service == nil || service.loader == nil || service.rawConn == nil {
		return Outcome{}, ErrUnavailable
	}
	started := service.now()

	inputs, err := service.loader.Load(ctx, orgID, window)
	if err != nil {
		service.logger.Error("issue-commit edges: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return Outcome{OrganizationID: orgID}, err
	}

	buildTime := service.now().UTC()
	result := Derive(inputs, buildTime)

	written, err := edges.WriteEdges(ctx, service.rawConn, orgID, result.Edges)
	if err != nil {
		service.logger.Error("issue-commit edges: write failed",
			slog.String("organization_id", orgID),
			slog.Int("commits_scanned", len(inputs.Commits)),
			slog.Any("error", err))
		return Outcome{OrganizationID: orgID}, fmt.Errorf("write issue-commit edges: %w", err)
	}

	outcome := Outcome{
		OrganizationID: orgID,
		CommitsScanned: len(inputs.Commits),
		EdgesWritten:   written,
		Duration:       service.now().Sub(started),
	}
	service.logger.Info("issue-commit edges produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("commits_scanned", outcome.CommitsScanned),
		slog.Int("edges_written", outcome.EdgesWritten),
		slog.Int("jira_refs_found", result.JiraRefsFound),
		slog.Int("github_refs_found", result.GitHubRefsFound),
		slog.Int("gitlab_refs_found", result.GitLabRefsFound),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}
