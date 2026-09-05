package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// prCommitLinksPreStep and prCommitEdgesPreStep adapt the two PR<->commit
// sub-builders (CHAOS-5264) to the work-graph build's pre-step seam, mirroring
// issuePRLinksPreStep in workgraph_issue_pr_links.go -- same reasoning: these
// stay pure producers with no River/request-state knowledge, only this file
// knows both.
//
// Both are PRE-steps, unlike issue_pr_links' straddle (mapping pre-step, edge
// builder still Python). pr_commit_edges READS work_graph_pr_commit, which
// pr_commit_links WRITES -- registering both here, in that order, is what
// buildPreStepOrder's own ordering invariant requires (NativePreStep's doc:
// "ANY step that READS a table an earlier step WRITES must be registered
// after it").
//
// Window derivation is IDENTICAL to issue_pr_links': the bridge admits the
// same from_date/to_date/repo_id/heuristic_* scope shape for every sub-builder
// of this one bridge kind (worker_workgraph.py:74-80), so this reuses
// scopeString/parseScopeInstant/withPythonYearRange/buildScopeAllowedFields
// from workgraph_issue_pr_links.go rather than re-deriving them.
type prCommitLinksPreStep struct {
	service *prcommit.Service
	now     func() time.Time
}

func newPRCommitLinksPreStep(service *prcommit.Service) (*prCommitLinksPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &prCommitLinksPreStep{service: service, now: time.Now}, nil
}

func (step *prCommitLinksPreStep) Name() string { return "pr_commit_links" }

func (step *prCommitLinksPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := prCommitWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}
	outcome, err := step.service.ProduceLinks(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"links_written":   outcome.LinksWritten,
		"commits_scanned": outcome.CommitsScanned,
	}, nil
}

type prCommitEdgesPreStep struct {
	service *prcommit.Service
	now     func() time.Time
}

func newPRCommitEdgesPreStep(service *prcommit.Service) (*prCommitEdgesPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &prCommitEdgesPreStep{service: service, now: time.Now}, nil
}

func (step *prCommitEdgesPreStep) Name() string { return "pr_commit_edges" }

func (step *prCommitEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := prCommitWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}
	outcome, err := step.service.ProduceEdges(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"edges_written": outcome.EdgesWritten,
		"rows_read":     outcome.RowsRead,
	}, nil
}

// prCommitWindowFor reproduces the SAME window derivation as
// issuePRLinksPreStep.windowFor (run_work_graph_build, work_graph_tasks.py
// :121-135) -- identical scope, different Window type
// (prcommit.Window vs issueprlinks.Window), so the parsing helpers are shared
// but the assembly is not.
func prCommitWindowFor(rawScope []byte, now func() time.Time) (prcommit.Window, error) {
	scope := map[string]json.RawMessage{}
	if len(rawScope) > 0 {
		if strings.TrimSpace(string(rawScope)) == "null" {
			return prcommit.Window{}, fmt.Errorf("build scope must be an object, got null")
		}
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return prcommit.Window{}, fmt.Errorf("build scope must be an object: %w", err)
		}
	}
	for field := range scope {
		if _, allowed := buildScopeAllowedFields[field]; !allowed {
			return prcommit.Window{}, fmt.Errorf(
				"build scope contains unsupported field %q; the bridge rejects it", field,
			)
		}
	}

	to := now().UTC()
	if text, present, err := scopeString(scope["to_date"]); err != nil {
		return prcommit.Window{}, fmt.Errorf("build scope to_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return prcommit.Window{}, fmt.Errorf("build scope to_date: %w", parseErr)
		}
		to = parsed
	}
	from := to.AddDate(0, 0, -30)
	if _, rangeErr := withPythonYearRange("derived from_date", from); rangeErr != nil {
		return prcommit.Window{}, fmt.Errorf(
			"build scope to_date %s: the default 30-day lower bound falls outside the "+
				"reference's 1..9999 year range, where it raises OverflowError", to.Format(time.RFC3339))
	}
	if text, present, err := scopeString(scope["from_date"]); err != nil {
		return prcommit.Window{}, fmt.Errorf("build scope from_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return prcommit.Window{}, fmt.Errorf("build scope from_date: %w", parseErr)
		}
		from = parsed
	}

	window := prcommit.Window{From: &from, To: &to}
	if text, present, err := scopeString(scope["repo_id"]); err != nil {
		return prcommit.Window{}, fmt.Errorf("build scope repo_id: %w", err)
	} else if present {
		repoID, parseErr := pythonparity.ParseUUID(text)
		if parseErr != nil {
			return prcommit.Window{}, fmt.Errorf("build scope repo_id: %w", parseErr)
		}
		window.RepoID = &repoID
	}
	return window, nil
}
