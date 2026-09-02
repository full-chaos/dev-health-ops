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
//
// # The two-`now` boundary, analysed and accepted (codex round 1, F1)
//
// On a default scope (the fixed producer persists `{}`) this step takes `now`
// when it runs and Python takes its own `now` moments later, so Python's `to`
// bound is strictly LATER. A pull request created in that sliver is inside
// Python's window and outside this one. It cannot be removed: both planes
// derive `now` independently and nothing is shared to anchor them, so the only
// "fix" would be changing the reference producer.
//
// It is accepted rather than merely noted, because the consequence is bounded
// at both ends of this stack's life:
//
//   - WHILE the Python producer is still live (PR 2), its window is the wider
//     one at the `to` end, so it writes the row this step skipped. The mapping
//     TABLE is unaffected; only this step's ledger fragment counts differ.
//   - AFTER the Python producer is retired (PR 3), this window is the only one,
//     and "the window ends when the step runs" is the correct semantics for a
//     producer with a `now` bound -- not a missing link.
//
// What would be a real defect is the reverse ordering (this step's `to` being
// LATER than Python's), because then this step would write links Python never
// derives. It cannot happen: the step runs strictly before the bridge dispatch
// that leads to Python's own derivation.
func (step *issuePRLinksPreStep) windowFor(rawScope []byte) (issueprlinks.Window, error) {
	scope := buildScope{}
	if len(rawScope) > 0 {
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return issueprlinks.Window{}, fmt.Errorf("decode build scope: %w", err)
		}
	}

	// Python gates these on TRUTHINESS (`if to_date:`, work_graph_tasks.py:124
	// and :129), so an empty or whitespace-only value means ABSENT and the
	// default applies. Testing the pointer for nil instead would turn an
	// ordinary "no value" serialisation into a build failure (codex round 1,
	// F2 -- and the first version of this adapter did exactly that).
	to := step.now().UTC()
	if present(scope.ToDate) {
		parsed, err := parseScopeInstant(*scope.ToDate)
		if err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope to_date: %w", err)
		}
		to = parsed
	}
	from := to.AddDate(0, 0, -30)
	if present(scope.FromDate) {
		parsed, err := parseScopeInstant(*scope.FromDate)
		if err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope from_date: %w", err)
		}
		from = parsed
	}

	window := issueprlinks.Window{From: &from, To: &to}
	if present(scope.RepoID) {
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
// present reports whether a scope string carries a value, matching Python's
// truthiness gate. A nil pointer, an empty string and a whitespace-only string
// are all "absent".
func present(value *string) bool {
	return value != nil && strings.TrimSpace(*value) != ""
}

func parseScopeInstant(value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty value")
	}
	// The layouts `datetime.fromisoformat` accepts that a caller could plausibly
	// send, verified against the deployed interpreter: extended and BASIC dates,
	// and date-times at minute or second precision. ISO week dates
	// ("2026-W33-6") and ordinal dates are accepted by Python and refused here;
	// nothing in the tree writes a build scope with dates at all (the fixed
	// producer persists `{}`), so those forms are unreachable, and a LOUD
	// refusal is the right failure for an unreachable input -- far better than
	// silently computing a different window than the reference would.
	for _, layout := range []string{
		"2006-01-02", "20060102",
		"2006-01-02T15:04:05", "2006-01-02 15:04:05",
		"2006-01-02T15:04", "2006-01-02 15:04",
	} {
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
