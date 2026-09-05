package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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
//
// # Why the two steps share ONE computed window (codex round chaos-5264-pr-r1,
// # P1, EXECUTED repro)
//
// Python computes its default 30-day window exactly ONCE per build, in
// BuildConfig, and BOTH sub-builders read that same fixed config -- there is
// no scenario where Python's two functions see different bounds. An earlier
// version of this file had EACH step call prCommitWindowFor with its OWN
// `time.Now`, independently, at whatever wall-clock instant its Run() happened
// to execute. Since the steps run sequentially (pr_commit_edges strictly after
// pr_commit_links per buildPreStepOrder), real time elapses between the two
// calls -- however small -- and the DERIVED default `from` bound (`to - 30d`)
// shifts by that same delta. A commit landing in that sliver is INSIDE
// pr_commit_links' window (so a link gets written for it) but OUTSIDE
// pr_commit_edges' slightly-later, slightly-more-restrictive window (so no
// edge is ever built for that link) -- and because the window is a rolling
// 30-day lookback, the commit only drifts FURTHER outside it on every later
// run. Reproduced with two `time.Now` values 1 second apart: link_from
// 12:00:00, edge_from 12:00:01, a commit at 12:00:00.5 -- linked, never edged.
//
// sharedPRCommitWindow closes this by having pr_commit_links compute the
// window once and pr_commit_edges consume that EXACT value, keyed by request
// id because both step INSTANCES are long-lived and shared across whatever
// claims a River worker processes concurrently -- a plain field on the step
// would race across two builds in flight at once.
type sharedPRCommitWindow struct {
	mu      sync.Mutex
	byClaim map[string]prcommit.Window
}

func newSharedPRCommitWindow() *sharedPRCommitWindow {
	return &sharedPRCommitWindow{byClaim: make(map[string]prcommit.Window)}
}

func (shared *sharedPRCommitWindow) store(requestID string, window prcommit.Window) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	shared.byClaim[requestID] = window
}

// take returns the window pr_commit_links stored for this claim, consuming it
// (deleting the entry) so the map does not grow unbounded across the worker's
// lifetime. ok is false only if pr_commit_edges somehow ran for a claim
// pr_commit_links did not -- unreachable given buildPreStepOrder's declared
// order and the constructed-vs-declared assertion in workgraphBuildPreSteps,
// but a step must not panic on a missing key, so the caller falls back to
// computing its own window rather than trusting an ok=true zero value.
func (shared *sharedPRCommitWindow) take(requestID string) (prcommit.Window, bool) {
	shared.mu.Lock()
	defer shared.mu.Unlock()
	window, ok := shared.byClaim[requestID]
	if ok {
		delete(shared.byClaim, requestID)
	}
	return window, ok
}

type prCommitLinksPreStep struct {
	service *prcommit.Service
	shared  *sharedPRCommitWindow
	now     func() time.Time
}

func newPRCommitLinksPreStep(service *prcommit.Service, shared *sharedPRCommitWindow) (*prCommitLinksPreStep, error) {
	if service == nil || shared == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &prCommitLinksPreStep{service: service, shared: shared, now: time.Now}, nil
}

func (step *prCommitLinksPreStep) Name() string { return "pr_commit_links" }

func (step *prCommitLinksPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := prCommitWindowFor(claim.Request.Scope, step.now)
	if err != nil {
		return nil, err
	}
	step.shared.store(claim.Request.ID, window)
	outcome, err := step.service.ProduceLinks(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"links_written":   outcome.LinksWritten,
		"commits_scanned": outcome.CommitsScanned,
	}, nil
}

// prCommitEdgesPreStep has no `now` field, deliberately: it never computes its
// own window (see Run below) -- the only clock read for one claim's window is
// pr_commit_links', which sharedPRCommitWindow carries over verbatim.
type prCommitEdgesPreStep struct {
	service *prcommit.Service
	shared  *sharedPRCommitWindow
}

func newPRCommitEdgesPreStep(service *prcommit.Service, shared *sharedPRCommitWindow) (*prCommitEdgesPreStep, error) {
	if service == nil || shared == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &prCommitEdgesPreStep{service: service, shared: shared}, nil
}

func (step *prCommitEdgesPreStep) Name() string { return "pr_commit_edges" }

func (step *prCommitEdgesPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	// A missing entry here means pr_commit_links did NOT run for this claim --
	// unreachable given buildPreStepOrder's declared order and the
	// constructed-vs-declared assertion in workgraphBuildPreSteps, but per
	// chris's 15:30 fail-loudly ruling this is refused loudly rather than
	// silently recomputing an independent window, which would reintroduce the
	// EXACT drift this type exists to close (a second, later `time.Now` read
	// producing a different bound than the one pr_commit_links actually used).
	window, ok := step.shared.take(claim.Request.ID)
	if !ok {
		return nil, fmt.Errorf(
			"pr_commit_edges: no shared window found for request %s (org %s) -- "+
				"pr_commit_links must run first and store one; refusing to recompute "+
				"independently, which would reintroduce the window-drift defect "+
				"(codex round chaos-5264-pr-r1, P1)",
			claim.Request.ID, claim.Request.OrganizationID,
		)
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
