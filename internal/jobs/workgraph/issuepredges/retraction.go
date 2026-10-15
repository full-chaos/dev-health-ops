package issuepredges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// RetractionOutcome is one run's accounting for the fallback retraction.
type RetractionOutcome struct {
	OrganizationID string
	LinksRead      int
	LinksRetracted int
	EdgesRetired   int
	Duration       time.Duration
}

// PlanOutrankedEdgeTombstones retires every live pull request -> issue edge of
// a fallback tier whose pull request or issue has a native link in stored.
// One shape is kept: an IMPLEMENTS edge between a natively linked pair,
// because the fast-path step rewrites that identity as native and a tombstone
// could only race it.
func PlanOutrankedEdgeTombstones(versions edges.EdgeVersions, stored []issueprlinks.Link, clock time.Time) []edges.Row {
	native := issueprlinks.NewNativeLinks(stored)
	nativePRIDs := make(map[string]struct{})
	nativePairs := make(map[[2]string]struct{})
	for _, link := range stored {
		if link.Provenance != issueprlinks.ProvenanceNative {
			continue
		}
		prID := edges.GeneratePRID(link.RepoID, int(link.PRNumber))
		nativePRIDs[prID] = struct{}{}
		nativePairs[[2]string{prID, link.WorkItemID}] = struct{}{}
	}
	return edges.PlanTombstonesWhere(versions, clock, func(row edges.Row) bool {
		if row.SourceType != edges.NodeTypePR || row.TargetType != edges.NodeTypeIssue {
			return false
		}
		if row.Provenance != edges.ProvenanceExplicitText && row.Provenance != edges.ProvenanceHeuristic {
			return false
		}
		_, prNative := nativePRIDs[row.SourceID]
		if !prNative && !native.HasIssue(row.TargetID) {
			return false
		}
		if _, pair := nativePairs[[2]string{row.SourceID, row.TargetID}]; pair && row.EdgeType == edges.EdgeTypeImplements {
			return false
		}
		return true
	})
}

// RetractOutrankedFallbacks removes the explicit_text and heuristic links a
// native link outranks, from work_graph_issue_pr and from work_graph_edges.
// It runs after the native link step and before any step that reads the link
// table, so every later reader sees only links the tier rule allows. Rows
// written before the rule, and rows a native link arrived after, are both
// cleaned by the next build of the organization.
func (service *Service) RetractOutrankedFallbacks(ctx context.Context, orgID string) (RetractionOutcome, error) {
	if service == nil || service.rawConn == nil {
		return RetractionOutcome{}, ErrUnavailable
	}
	if orgID == "" {
		return RetractionOutcome{}, ErrScopeRequired
	}
	started := service.now()

	stored, err := issueprlinks.ReadStoredLinks(ctx, service.rawConn, orgID)
	if err != nil {
		service.logger.Error("issue-pr fallback retraction: load failed",
			slog.String("organization_id", orgID), slog.Any("error", err))
		return RetractionOutcome{OrganizationID: orgID}, err
	}
	native := issueprlinks.NewNativeLinks(stored)
	outranked := issueprlinks.PlanOutrankedFallbacks(stored)
	for _, link := range outranked {
		service.logger.Debug("issue-pr fallback retraction: link outranked by native",
			slog.String("organization_id", orgID),
			slog.String("repo_id", link.RepoID.String()),
			slog.Uint64("pr_number", uint64(link.PRNumber)),
			slog.String("work_item_id", link.WorkItemID),
			slog.String("provenance", link.Provenance))
	}
	if err := issueprlinks.RetractOutranked(ctx, service.rawConn, orgID, outranked); err != nil {
		service.logger.Error("issue-pr fallback retraction: link retraction failed",
			slog.String("organization_id", orgID),
			slog.Int("links_outranked", len(outranked)),
			slog.Any("error", err))
		return RetractionOutcome{OrganizationID: orgID}, err
	}

	retired := 0
	if !native.Empty() {
		prIDs := make([]string, 0, len(native.PullRequests()))
		for _, key := range native.PullRequests() {
			prIDs = append(prIDs, edges.GeneratePRID(key.RepoID, int(key.PRNumber)))
		}
		versions, err := edges.ReadPRIssueEdgeVersions(ctx, service.rawConn, orgID, prIDs, native.Issues())
		if err != nil {
			service.logger.Error("issue-pr fallback retraction: edge read failed",
				slog.String("organization_id", orgID), slog.Any("error", err))
			return RetractionOutcome{OrganizationID: orgID}, err
		}
		tombstones := PlanOutrankedEdgeTombstones(versions, stored, service.now().UTC())
		if retired, err = edges.WriteTombstones(ctx, service.rawConn, orgID, tombstones); err != nil {
			service.logger.Error("issue-pr fallback retraction: edge tombstone write failed",
				slog.String("organization_id", orgID),
				slog.Int("edges_outranked", len(tombstones)),
				slog.Any("error", err))
			return RetractionOutcome{OrganizationID: orgID}, fmt.Errorf("retire outranked issue-pr edges: %w", err)
		}
	}

	outcome := RetractionOutcome{
		OrganizationID: orgID,
		LinksRead:      len(stored),
		LinksRetracted: len(outranked),
		EdgesRetired:   retired,
		Duration:       service.now().Sub(started),
	}
	service.logger.Info("issue-pr fallback retraction done",
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("links_read", outcome.LinksRead),
		slog.Int("links_retracted", outcome.LinksRetracted),
		slog.Int("edges_retired", outcome.EdgesRetired),
		slog.Duration("duration", outcome.Duration),
	)
	return outcome, nil
}
