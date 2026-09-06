package prcommit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// LinksOutcome is one run's accounting for the commit-message derivation half
// (_derive_pr_commit_links).
type LinksOutcome struct {
	OrganizationID string
	CommitsScanned int
	LinksWritten   int
	Duration       time.Duration
}

// EdgesOutcome is one run's accounting for the fast-path edge-building half
// (_build_pr_commit_edges_from_fast_path).
type EdgesOutcome struct {
	OrganizationID string
	RowsRead       int
	EdgesWritten   int
	Duration       time.Duration
}

// Service orchestrates both PR<->commit sub-builders against ClickHouse.
//
// Unlike issueprlinks.Service (one producer, one Produce method), this package
// ports TWO sub-builders that straddle each other exactly the way
// buildPreStepOrder's own doc comment describes for a future edge port: the
// edge half READS what the link half WRITES (builder.py:483-486 runs them back
// to back in that order), so they are two methods, not one, and a caller
// wiring both as pre-steps must register them in this order.
type Service struct {
	loader *Loader
	writer *Writer
	// rawConn is the fast-path edge builder's write path. It is a plain
	// driver.Conn, not the narrow conn interface Loader/Writer use, because
	// WriteFastPathEdges delegates to the shared edges package, which is typed
	// against driver.Conn directly (edges.WriteEdges). Kept separate rather
	// than widening conn's interface, since Loader/Writer never need
	// PrepareBatch's fuller shape.
	rawConn driver.Conn
	logger  *slog.Logger
	now     func() time.Time
}

// NewService wires the production path.
func NewService(loader *Loader, writer *Writer, rawConn driver.Conn, logger *slog.Logger) (*Service, error) {
	if loader == nil || writer == nil || rawConn == nil {
		return nil, ErrUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{loader: loader, writer: writer, rawConn: rawConn, logger: logger, now: time.Now}, nil
}

// SetClock overrides the fallback clock. Test-only seam, same reasoning as
// issueprlinks.Service.SetClock.
func (service *Service) SetClock(now func() time.Time) {
	if service == nil || now == nil {
		return
	}
	service.now = now
}

// ProduceLinks runs _derive_pr_commit_links for one org and window: parse
// commit messages, corroborate against known PRs, write the fast-path table.
func (service *Service) ProduceLinks(ctx context.Context, orgID string, window Window) (LinksOutcome, error) {
	if service == nil || service.loader == nil || service.writer == nil {
		return LinksOutcome{}, ErrUnavailable
	}
	started := service.now()

	inputs, err := service.loader.Load(ctx, orgID, window)
	if err != nil {
		service.logger.Error("pr-commit links: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return LinksOutcome{OrganizationID: orgID}, err
	}
	result := Derive(inputs)

	// Derive leaves LastSynced zero (see prcommit.go) -- stamp the real
	// timestamp here, one clock read for the whole batch, mirroring
	// issueprlinks.Service.Produce's identical post-Derive fallback.
	stamped := service.now().UTC()
	for index := range result.Links {
		result.Links[index].LastSynced = stamped
	}

	if err := service.writer.Write(ctx, orgID, result.Links); err != nil {
		service.logger.Error("pr-commit links: write failed",
			slog.String("organization_id", orgID),
			slog.Int("links_derived", len(result.Links)),
			slog.Any("error", err))
		return LinksOutcome{OrganizationID: orgID}, fmt.Errorf("write pr-commit links: %w", err)
	}

	outcome := LinksOutcome{
		OrganizationID: orgID,
		CommitsScanned: result.CommitsScanned,
		LinksWritten:   len(result.Links),
		Duration:       service.now().Sub(started),
	}
	service.logger.Info("pr-commit links produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("commits_scanned", outcome.CommitsScanned),
		slog.Int("links_written", outcome.LinksWritten),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}

// ProduceEdges runs _build_pr_commit_edges_from_fast_path for one org and
// window: read the fast-path table joined against git_commits, write CONTAINS
// edges. Must run AFTER ProduceLinks in the same build -- see Service's doc
// comment on why these are ordered, not merged.
func (service *Service) ProduceEdges(ctx context.Context, orgID string, window Window) (EdgesOutcome, error) {
	if service == nil || service.loader == nil || service.rawConn == nil {
		return EdgesOutcome{}, ErrUnavailable
	}
	started := service.now()

	rows, err := service.loader.LoadFastPath(ctx, orgID, window)
	if err != nil {
		service.logger.Error("pr-commit edges: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return EdgesOutcome{OrganizationID: orgID}, err
	}

	buildTime := service.now().UTC()
	edgeRows := BuildFastPathEdges(orgID, rows, buildTime)

	written, err := WriteFastPathEdges(ctx, service.rawConn, orgID, edgeRows)
	if err != nil {
		service.logger.Error("pr-commit edges: write failed",
			slog.String("organization_id", orgID),
			slog.Int("rows_read", len(rows)),
			slog.Any("error", err))
		return EdgesOutcome{OrganizationID: orgID}, fmt.Errorf("write pr-commit edges: %w", err)
	}

	outcome := EdgesOutcome{
		OrganizationID: orgID,
		RowsRead:       len(rows),
		EdgesWritten:   written,
		Duration:       service.now().Sub(started),
	}
	service.logger.Info("pr-commit edges produced",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("rows_read", outcome.RowsRead),
		slog.Int("edges_written", outcome.EdgesWritten),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}
