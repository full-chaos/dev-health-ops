package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/google/uuid"
)

// issuePRLinksPreStep adapts the provider-attached PR-issue mapping producer
// (CHAOS-4757) to the work-graph build's pre-step seam.
//
// It lives at the composition root on purpose: `issueprlinks` stays a pure
// producer with no knowledge of River or the request state machine, and
// `workgraph` stays unaware of any particular producer. Only this file knows
// both.
type issuePRLinksPreStep struct {
	service *issueprlinks.Service
	now     func() time.Time
}

func newIssuePRLinksPreStep(service *issueprlinks.Service) (*issuePRLinksPreStep, error) {
	if service == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &issuePRLinksPreStep{service: service, now: time.Now}, nil
}

func (step *issuePRLinksPreStep) Name() string { return "issue_pr_links" }

// buildScope is the subset of `workgraph.build`'s request scope this step
// reads. The full allowed set is fixed by the Python bridge at
// `api/internal/worker_workgraph.py:74-80`: from_date, to_date, repo_id,
// heuristic_window, heuristic_confidence. The two heuristic keys belong to the
// heuristic edge builder, which this step does not implement.
type buildScope struct {
	FromDate *string `json:"from_date"`
	ToDate   *string `json:"to_date"`
	RepoID   *string `json:"repo_id"`
}

func (step *issuePRLinksPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	window, err := step.windowFor(claim.Request.Scope)
	if err != nil {
		return nil, err
	}
	outcome, err := step.service.Produce(ctx, claim.Request.OrganizationID, window)
	if err != nil {
		return nil, err
	}
	fragment := map[string]any{
		"rows_written":      outcome.Written,
		"dependencies_read": outcome.DependenciesRead,
		"balanced":          outcome.Balanced,
	}
	for reason, count := range outcome.Rejected {
		if count > 0 {
			fragment["rejected_"+string(reason)] = count
		}
	}
	for kind, count := range outcome.ReservedSeenByRawKind {
		if count > 0 {
			fragment["reserved_seen_"+kind] = count
		}
	}
	return fragment, nil
}

// windowFor reproduces `run_work_graph_build`'s window derivation
// (`workers/work_graph_tasks.py:121-135`) exactly:
//
//	to   = fromisoformat(to_date)   if given, else now
//	from = fromisoformat(from_date) if given, else to - 30 days
//	repo = UUID(repo_id)            if given, else unset
//
// The 30-day default is load-bearing and easy to miss: Python's build window is
// NEVER unbounded. A pre-step that read with no window would consider pull
// requests the Python producer never looks at, and write mapping rows for them.
func (step *issuePRLinksPreStep) windowFor(rawScope []byte) (issueprlinks.Window, error) {
	scope := buildScope{}
	if len(rawScope) > 0 {
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return issueprlinks.Window{}, fmt.Errorf("decode build scope: %w", err)
		}
	}

	to := step.now().UTC()
	if scope.ToDate != nil {
		parsed, err := parseScopeInstant(*scope.ToDate)
		if err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope to_date: %w", err)
		}
		to = parsed
	}
	from := to.AddDate(0, 0, -30)
	if scope.FromDate != nil {
		parsed, err := parseScopeInstant(*scope.FromDate)
		if err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope from_date: %w", err)
		}
		from = parsed
	}

	window := issueprlinks.Window{From: &from, To: &to}
	if scope.RepoID != nil && strings.TrimSpace(*scope.RepoID) != "" {
		repoID, err := uuid.Parse(strings.TrimSpace(*scope.RepoID))
		if err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope repo_id: %w", err)
		}
		window.RepoID = &repoID
	}
	return window, nil
}

// parseScopeInstant accepts the forms Python's `datetime.fromisoformat` accepts
// for this scope in practice -- a bare date, or a date-time without an offset --
// and anchors both to UTC.
//
// That anchoring is what Python effectively does: `fromisoformat("2026-09-01")`
// yields a NAIVE datetime, the builder renders it with `strftime`
// (builder.py:57-60), and ClickHouse reads the resulting literal in the
// column's own UTC zone. So naive-means-UTC is the reference behaviour, not an
// assumption.
//
// An offset-bearing value is REFUSED rather than converted, matching
// `issueprlinks.truncateBoundToSecond`: Python's strftime keeps the wall-clock
// fields and DISCARDS the offset, so `2026-01-01T00:00:00+05:00` means
// `2026-01-01 00:00:00 UTC` there and `2025-12-31T19:00:00Z` under any
// instant-preserving reading. Those are different queries, nothing produces
// such a scope today, and guessing between them is exactly what a measurement
// that cannot honour its contract must not do.
func parseScopeInstant(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty value")
	}
	for _, layout := range []string{"2006-01-02", "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if parsed, err := time.ParseInLocation(layout, trimmed, time.UTC); err == nil {
			return parsed, nil
		}
	}
	if parsed, err := time.Parse(time.RFC3339, trimmed); err == nil {
		// A ZERO offset ("Z" or "+00:00") is unambiguous: Python's
		// fromisoformat yields an aware UTC datetime and strftime renders the
		// same wall clock either reading would give. Accepting it here matches
		// issueprlinks.truncateBoundToSecond, which also accepts a zero-offset
		// bound and refuses only a shifted one. Refusing "Z" would have been
		// over-strict AND inconsistent between the two layers.
		if _, offset := parsed.Zone(); offset == 0 {
			return parsed.UTC(), nil
		}
		return time.Time{}, fmt.Errorf(
			"%q carries a non-zero UTC offset; Python's strftime would discard it and mean a "+
				"different instant (see issueprlinks.ErrNonUTCWindowBound)", trimmed,
		)
	}
	return time.Time{}, fmt.Errorf("%q is not a supported ISO date or date-time", trimmed)
}
