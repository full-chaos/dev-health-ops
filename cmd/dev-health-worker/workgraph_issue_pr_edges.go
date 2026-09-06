package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// issuePREdgesFastPathPreStep, issuePREdgesTextParsePreStep and
// issuePREdgesHeuristicPreStep adapt the three issue<->PR EDGE sub-builders
// (CHAOS-4924, folding what was going to be a separate PR F into this one
// per team-lead's ruling: `_read_existing_issue_pr_links` would have been
// NEW Python in builder.py, and the only allowed Python edits in this family
// are deletions) to the work-graph build's pre-step seam, mirroring
// prCommitLinksPreStep/prCommitEdgesPreStep in workgraph_pr_commit.go --
// same reasoning, same shared-window pattern, applied here from the start
// rather than discovered by a later confirmation round (see
// sharedIssuePREdgesWindow's doc for why one window must serve all three
// steps, and issuePREdgesWindowFor's doc for the derived-bound-guard ordering
// fix that confirm2's codex round found in the sibling operationaledges/
// prcommit window helpers, applied here proactively).
//
// Ordering: fast_path, then text_parse, then heuristic -- see issuepredges'
// package doc's "Ordering" section (fast-path/text-parse order) and
// ExplicitLink's doc comment (why heuristic must run after both: it reads
// work_graph_issue_pr fresh, which is exactly what the other two commit).
type sharedIssuePREdgesWindow struct {
	mu      sync.Mutex
	byClaim map[string]issuepredges.Window
}

func newSharedIssuePREdgesWindow() *sharedIssuePREdgesWindow {
	return &sharedIssuePREdgesWindow{byClaim: make(map[string]issuepredges.Window)}
}

func (shared *sharedIssuePREdgesWindow) store(requestID string, window issuepredges.Window) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	shared.byClaim[requestID] = window
}

// peek returns the window the fast-path step stored for this claim WITHOUT
// consuming it -- used by the text-parse step, which is a MIDDLE consumer:
// the heuristic step still needs the same entry afterward, so only the last
// consumer (take, below) may delete it.
func (shared *sharedIssuePREdgesWindow) peek(requestID string) (issuepredges.Window, bool) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	window, ok := shared.byClaim[requestID]
	return window, ok
}

// take returns the window the fast-path step stored for this claim, consuming
// it (deleting the entry) so the map does not grow unbounded across the
// worker's lifetime -- identical reasoning to sharedPRCommitWindow.take.
// Only the LAST consumer in the pipeline (issuePREdgesHeuristicPreStep) may
// call this; an earlier consumer calling it would delete the entry before
// the steps after it could read it.
func (shared *sharedIssuePREdgesWindow) take(requestID string) (issuepredges.Window, bool) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	window, ok := shared.byClaim[requestID]
	if ok {
		delete(shared.byClaim, requestID)
	}
	return window, ok
}

type issuePREdgesFastPathPreStep struct {
	service *issuepredges.Service
	shared  *sharedIssuePREdgesWindow
	now     func() time.Time
}

func newIssuePREdgesFastPathPreStep(service *issuepredges.Service, shared *sharedIssuePREdgesWindow) (*issuePREdgesFastPathPreStep, error) {
	if service == nil || shared == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePREdgesFastPathPreStep{service: service, shared: shared, now: time.Now}, nil
}

func (step *issuePREdgesFastPathPreStep) Name() string { return "issue_pr_edges_fast_path" }

func (step *issuePREdgesFastPathPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := issuePREdgesWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}
	// Store ONLY after ProduceFastPathEdges succeeds -- same reasoning as
	// prCommitLinksPreStep.Run (codex round chaos-5264-pr-confirm, P1):
	// storing first would leak an entry on a claim that fails once and is
	// never retried, since runPreSteps aborts the whole claim on any error
	// and the text-parse step's `take` -- the only thing that deletes an
	// entry -- would never run.
	outcome, err := step.service.ProduceFastPathEdges(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	step.shared.store(claim.Request.ID, window)
	return map[string]any{
		"edges_written": outcome.EdgesWritten,
		"rows_read":     outcome.RowsRead,
	}, nil
}

// issuePREdgesTextParsePreStep has no `now` field, deliberately: it never
// computes its own window -- the only clock read for one claim's window is
// the fast-path step's, which sharedIssuePREdgesWindow carries over verbatim.
type issuePREdgesTextParsePreStep struct {
	service *issuepredges.Service
	shared  *sharedIssuePREdgesWindow
}

func newIssuePREdgesTextParsePreStep(service *issuepredges.Service, shared *sharedIssuePREdgesWindow) (*issuePREdgesTextParsePreStep, error) {
	if service == nil || shared == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePREdgesTextParsePreStep{service: service, shared: shared}, nil
}

func (step *issuePREdgesTextParsePreStep) Name() string { return "issue_pr_edges_text_parse" }

func (step *issuePREdgesTextParsePreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	// peek, not take: the heuristic step (registered after this one) still
	// needs the same window entry -- see sharedIssuePREdgesWindow.peek's doc.
	// A missing entry here means the fast-path step did NOT run for this
	// claim -- unreachable given buildPreStepOrder's declared order and the
	// constructed-vs-declared assertion in workgraphBuildPreSteps, but per
	// the standing fail-loudly ruling this is refused loudly rather than
	// silently recomputing an independent window, which would reintroduce
	// the window-drift defect chaos-5264-pr-r1 found for the sibling
	// pr_commit_links/pr_commit_edges pair.
	window, ok := step.shared.peek(claim.Request.ID)
	if !ok {
		return nil, fmt.Errorf(
			"issue_pr_edges_text_parse: no shared window found for request %s (org %s) -- "+
				"issue_pr_edges_fast_path must run first and store one; refusing to recompute "+
				"independently, which would reintroduce the window-drift defect "+
				"(codex round chaos-5264-pr-r1, P1)",
			claim.Request.ID, claim.Request.OrganizationID,
		)
	}
	outcome, err := step.service.ProduceTextParseEdges(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		// This step only peeked, so the entry fast_path stored is still in
		// the shared map. If this Run returns an error, runPreSteps aborts
		// the whole claim -- issue_pr_edges_heuristic (the only step that
		// calls take, the one thing that actually deletes an entry) never
		// runs, and the entry would leak forever for this request id
		// (the exact leak class chaos-5264-pr-confirm's P1 closed for the
		// pr_commit pair, reoccurring here because a THIRD step sits between
		// the peek and the eventual take). Clean it up here instead.
		step.shared.take(claim.Request.ID)
		return nil, err
	}
	return map[string]any{
		"edges_written":      outcome.EdgesWritten,
		"links_written":      outcome.LinksWritten,
		"pull_requests_read": outcome.PullRequestsRead,
	}, nil
}

// issuePREdgesHeuristicPreStep is the LAST consumer of the shared window --
// see sharedIssuePREdgesWindow.take's doc. It has no `now` field, same
// reasoning as issuePREdgesTextParsePreStep.
type issuePREdgesHeuristicPreStep struct {
	service *issuepredges.Service
	shared  *sharedIssuePREdgesWindow
}

func newIssuePREdgesHeuristicPreStep(service *issuepredges.Service, shared *sharedIssuePREdgesWindow) (*issuePREdgesHeuristicPreStep, error) {
	if service == nil || shared == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePREdgesHeuristicPreStep{service: service, shared: shared}, nil
}

func (step *issuePREdgesHeuristicPreStep) Name() string { return "issue_pr_edges_heuristic" }

func (step *issuePREdgesHeuristicPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	// take, not peek: this is the last step in the pipeline that needs this
	// claim's window, so this is what actually deletes the entry -- see
	// sharedIssuePREdgesWindow.take's doc on why an earlier step must not.
	window, ok := step.shared.take(claim.Request.ID)
	if !ok {
		return nil, fmt.Errorf(
			"issue_pr_edges_heuristic: no shared window found for request %s (org %s) -- "+
				"issue_pr_edges_fast_path must run first and store one; refusing to recompute "+
				"independently, which would reintroduce the window-drift defect "+
				"(codex round chaos-5264-pr-r1, P1)",
			claim.Request.ID, claim.Request.OrganizationID,
		)
	}
	outcome, err := step.service.ProduceHeuristicEdges(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"edges_written":   outcome.EdgesWritten,
		"links_written":   outcome.LinksWritten,
		"work_items_read": outcome.WorkItemsRead,
	}, nil
}

// issuePREdgesWindowFor reproduces the SAME window derivation as
// prCommitWindowFor/issuePRLinksPreStep.windowFor (run_work_graph_build,
// work_graph_tasks.py:121-135) -- identical scope, different Window type.
//
// # Derived-bound guard ordering (applied proactively, not discovered here)
//
// prCommitWindowFor and the pre-fix operationaledges window helper both
// compute the derived `to - 30d` lower bound and range-check it for overflow
// BEFORE checking whether scope even supplies an explicit from_date --
// confirmed as a real bug for operationaledges by codex round
// chaos-4924-pr-d-r1-confirm (P1, EXECUTED repro: explicit from_date/to_date
// both "0001-01-01", each individually valid, wrongly rejected over a
// derived value Python never computes when from_date is supplied). That
// sibling bug is tracked separately (ticketed, parent CHAOS-5264) rather than
// fixed in the two already-merged copies from this PR, but THIS function is
// new code, so the fix is applied directly here: the from_date presence
// check runs FIRST, and the derive+guard only happens in the else branch.
func issuePREdgesWindowFor(rawScope []byte, now func() time.Time) (issuepredges.Window, error) {
	window := issuepredges.Window{HeuristicDaysWindow: 7, HeuristicConfidence: 0.3}
	scope := map[string]json.RawMessage{}
	if len(rawScope) > 0 {
		if strings.TrimSpace(string(rawScope)) == "null" {
			return issuepredges.Window{}, fmt.Errorf("build scope must be an object, got null")
		}
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope must be an object: %w", err)
		}
	}
	for field := range scope {
		if _, allowed := buildScopeAllowedFields[field]; !allowed {
			return issuepredges.Window{}, fmt.Errorf(
				"build scope contains unsupported field %q; the bridge rejects it", field,
			)
		}
	}

	to := now().UTC()
	if text, present, err := scopeString(scope["to_date"]); err != nil {
		return issuepredges.Window{}, fmt.Errorf("build scope to_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope to_date: %w", parseErr)
		}
		to = parsed
	}

	var from time.Time
	if text, present, err := scopeString(scope["from_date"]); err != nil {
		return issuepredges.Window{}, fmt.Errorf("build scope from_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope from_date: %w", parseErr)
		}
		from = parsed
	} else {
		from = to.AddDate(0, 0, -30)
		if _, rangeErr := withPythonYearRange("derived from_date", from); rangeErr != nil {
			return issuepredges.Window{}, fmt.Errorf(
				"build scope to_date %s: the default 30-day lower bound falls outside the "+
					"reference's 1..9999 year range, where it raises OverflowError", to.Format(time.RFC3339))
		}
	}

	window.From = &from
	window.To = &to
	if text, present, err := scopeString(scope["repo_id"]); err != nil {
		return issuepredges.Window{}, fmt.Errorf("build scope repo_id: %w", err)
	} else if present {
		repoID, parseErr := pythonparity.ParseUUID(text)
		if parseErr != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope repo_id: %w", parseErr)
		}
		window.RepoID = &repoID
	}
	if raw, ok := scope["heuristic_window"]; ok {
		parsed, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope heuristic_window: %w", err)
		}
		window.HeuristicDaysWindow = parsed
	}
	if raw, ok := scope["heuristic_confidence"]; ok {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 32)
		if err != nil {
			return issuepredges.Window{}, fmt.Errorf("build scope heuristic_confidence: %w", err)
		}
		window.HeuristicConfidence = float32(parsed)
	}
	return window, nil
}
