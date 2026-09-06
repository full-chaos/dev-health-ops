package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuecommitedges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// issueCommitEdgesPreStep adapts the issue<->commit text-parse sub-builder
// (CHAOS-5304) to the work-graph build's pre-step seam, mirroring
// issuePRLinksPreStep / prCommitLinksPreStep -- same composition-root-only-file
// reasoning: issuecommitedges stays a pure producer with no River/request-state
// knowledge, only this file knows both.
//
// It reads git_commits and work_items and writes COMMIT->ISSUE edges directly
// -- no fast-path/link split like issue_pr_links, no table another pre-step
// writes (unlike pr_commit_edges reading work_graph_pr_commit). Its position
// in buildPreStepOrder relative to pr_commit_links/pr_commit_edges is
// therefore free by the ordering invariant; placed right after issue_pr_links,
// preserving Python's own relative stage order (builder.py's issue->commit
// stage, "3b", runs before the pr_commit stages, "4b"/"5").
//
// Window derivation is IDENTICAL in shape to issue_pr_links'/pr_commit's: the
// bridge admits the same from_date/to_date/repo_id/heuristic_* scope for every
// sub-builder of this one bridge kind (worker_workgraph.py:74-80), so this
// reuses scopeString/parseScopeInstant/withPythonYearRange/
// buildScopeAllowedFields from workgraph_issue_pr_links.go rather than
// re-deriving them -- same reasoning prCommitWindowFor gives for its own
// reuse.
type issueCommitEdgesPreStep struct {
	service *issuecommitedges.Service
	now     func() time.Time
}

func newIssueCommitEdgesPreStep(service *issuecommitedges.Service) (*issueCommitEdgesPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issueCommitEdgesPreStep{service: service, now: time.Now}, nil
}

func (step *issueCommitEdgesPreStep) Name() string { return "issue_commit_edges" }

func (step *issueCommitEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := issueCommitWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}
	outcome, err := step.service.Produce(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"commits_scanned": outcome.CommitsScanned,
		"edges_written":   outcome.EdgesWritten,
	}, nil
}

// issueCommitWindowFor reproduces the SAME window derivation as
// issuePRLinksPreStep.windowFor / prCommitWindowFor (run_work_graph_build,
// work_graph_tasks.py:121-135) -- identical scope, different Window type
// (issuecommitedges.Window), so the parsing helpers are shared but the
// assembly is not.
func issueCommitWindowFor(rawScope []byte, now func() time.Time) (issuecommitedges.Window, error) {
	scope := map[string]json.RawMessage{}
	if len(rawScope) > 0 {
		if strings.TrimSpace(string(rawScope)) == "null" {
			return issuecommitedges.Window{}, fmt.Errorf("build scope must be an object, got null")
		}
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return issuecommitedges.Window{}, fmt.Errorf("build scope must be an object: %w", err)
		}
	}
	for field := range scope {
		if _, allowed := buildScopeAllowedFields[field]; !allowed {
			return issuecommitedges.Window{}, fmt.Errorf(
				"build scope contains unsupported field %q; the bridge rejects it", field,
			)
		}
	}

	to := now().UTC()
	if text, present, err := scopeString(scope["to_date"]); err != nil {
		return issuecommitedges.Window{}, fmt.Errorf("build scope to_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issuecommitedges.Window{}, fmt.Errorf("build scope to_date: %w", parseErr)
		}
		to = parsed
	}
	var from time.Time
	if text, present, err := scopeString(scope["from_date"]); err != nil {
		return issuecommitedges.Window{}, fmt.Errorf("build scope from_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issuecommitedges.Window{}, fmt.Errorf("build scope from_date: %w", parseErr)
		}
		from = parsed
	} else {
		from = to.AddDate(0, 0, -30)
		if _, rangeErr := withPythonYearRange("derived from_date", from); rangeErr != nil {
			return issuecommitedges.Window{}, fmt.Errorf(
				"build scope to_date %s: the default 30-day lower bound falls outside the "+
					"reference's 1..9999 year range, where it raises OverflowError", to.Format(time.RFC3339))
		}
	}

	window := issuecommitedges.Window{From: &from, To: &to}
	if text, present, err := scopeString(scope["repo_id"]); err != nil {
		return issuecommitedges.Window{}, fmt.Errorf("build scope repo_id: %w", err)
	} else if present {
		repoID, parseErr := pythonparity.ParseUUID(text)
		if parseErr != nil {
			return issuecommitedges.Window{}, fmt.Errorf("build scope repo_id: %w", parseErr)
		}
		window.RepoID = &repoID
	}
	return window, nil
}

// commitFileEdgesPreStep adapts _count_commit_file_edges (CHAOS-5306,
// builder.py:1569-1584) to the pre-step seam. Unlike every sibling step, it
// WRITES nothing -- it is a pure readback over git_commit_stats -- so its
// position in buildPreStepOrder is unconstrained by the ordering invariant in
// either direction; placed after issue_commit_edges/pr_commit_edges,
// preserving Python's own relative stage order (builder.py stage 6, before
// 7/8's flag_guards_edges/operational_incident_edges).
//
// UNLIKE Python's try/except-warn-return-0, IssueCommitEdges.CountCommitFileEdges
// propagates a query error rather than swallowing it -- see that function's own
// doc comment for why. A pre-step failure fails the whole build (NativePreStep's
// documented failure semantics), which for a step that only counts rows is a
// bigger blast radius than Python's silent 0 ever had; accepted anyway, per
// this fleet's standing rule that a touched failure path surfaces rather than
// re-introduces a swallow.
type commitFileEdgesPreStep struct {
	conn driver.Conn
}

func newCommitFileEdgesPreStep(conn driver.Conn) (*commitFileEdgesPreStep, error) {
	if conn == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &commitFileEdgesPreStep{conn: conn}, nil
}

func (step *commitFileEdgesPreStep) Name() string { return "commit_file_edges" }

func (step *commitFileEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	count, err := issuecommitedges.CountCommitFileEdges(ctx, step.conn, claim.Request.OrganizationID)
	if err != nil {
		return nil, err
	}
	return map[string]any{"commit_file_edges": count}, nil
}
