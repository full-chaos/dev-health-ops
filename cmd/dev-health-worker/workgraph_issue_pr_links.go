package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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
// buildScopeAllowedFields mirrors the bridge's allowlist for this kind
// (`worker_workgraph.py:74-80`). `_scope_arguments` raises
// `request scope contains unsupported fields` on ANY key outside it, so a scope
// this step accepts but the bridge rejects would leave mapping rows behind for
// a build that never ran.
var buildScopeAllowedFields = map[string]struct{}{
	"from_date": {}, "to_date": {}, "repo_id": {},
	// Belong to the heuristic edge builder, which this step does not implement,
	// but they are ADMISSIBLE to the bridge and so must not be refused here.
	"heuristic_window": {}, "heuristic_confidence": {},
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
// On a scope with no `to_date` -- 66 of 612 rows on the proof org, all of them
// the fixed-schedule membership backfill -- this step takes `now` when it runs
// and Python takes its own `now` moments later, so Python's `to` bound is
// strictly LATER. A pull request created in that sliver is inside
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
	// The bridge admits an OBJECT and nothing else: `_scope_arguments` raises
	// `request scope must be an object` for null, arrays, strings, numbers and
	// booleans (worker_workgraph.py:72-73). Decoding into a struct would accept
	// `null` as an empty object and take the default window -- writing mapping
	// rows for a request the bridge is about to reject.
	scope := map[string]json.RawMessage{}
	if len(rawScope) > 0 {
		// `json.Unmarshal` decodes a literal `null` into a map WITHOUT error,
		// leaving it nil -- which would read as "an empty scope" and take the
		// default window. The bridge raises on it. Check the shape explicitly.
		if strings.TrimSpace(string(rawScope)) == "null" {
			return issueprlinks.Window{}, fmt.Errorf("build scope must be an object, got null")
		}
		if err := json.Unmarshal(rawScope, &scope); err != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope must be an object: %w", err)
		}
	}
	for field := range scope {
		if _, allowed := buildScopeAllowedFields[field]; !allowed {
			return issueprlinks.Window{}, fmt.Errorf(
				"build scope contains unsupported field %q; the bridge rejects it", field,
			)
		}
	}

	// Python gates these on TRUTHINESS (`if to_date:`, work_graph_tasks.py:124
	// and :129), so an empty or whitespace-only value means ABSENT and the
	// default applies. Testing the pointer for nil instead would turn an
	// ordinary "no value" serialisation into a build failure (codex round 1,
	// F2 -- and the first version of this adapter did exactly that).
	to := step.now().UTC()
	if text, present, err := scopeString(scope["to_date"]); err != nil {
		return issueprlinks.Window{}, fmt.Errorf("build scope to_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope to_date: %w", parseErr)
		}
		to = parsed
	}
	// The DEFAULT lower bound is arithmetic, and the arithmetic can leave the
	// reference's range even when both endpoints of the parse were inside it.
	//
	// `to_date: "0001-01-01"` parses cleanly on both planes -- year 1 is valid --
	// and then `to - 30 days` underflows below year 1, where CPython raises
	// OverflowError("date value out of range"). Go's time.Time has no such bound
	// and rolls silently into year zero, so Go RAN a build the bridge rejects.
	//
	// Checking only the parse was a check that looked complete: the parse is
	// where a value enters, but the DERIVED bound is a second value the
	// reference also range-checks, and nothing had tested it because every date
	// in the corpus was 2026.
	from := to.AddDate(0, 0, -30)
	if _, rangeErr := withPythonYearRange("derived from_date", from); rangeErr != nil {
		return issueprlinks.Window{}, fmt.Errorf(
			"build scope to_date %s: the default 30-day lower bound falls outside the "+
				"reference's 1..9999 year range, where it raises OverflowError", to.Format(time.RFC3339))
	}
	if text, present, err := scopeString(scope["from_date"]); err != nil {
		return issueprlinks.Window{}, fmt.Errorf("build scope from_date: %w", err)
	} else if present {
		parsed, parseErr := parseScopeInstant(text)
		if parseErr != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope from_date: %w", parseErr)
		}
		from = parsed
	}

	window := issueprlinks.Window{From: &from, To: &to}
	if text, present, err := scopeString(scope["repo_id"]); err != nil {
		return issueprlinks.Window{}, fmt.Errorf("build scope repo_id: %w", err)
	} else if present {
		repoID, parseErr := pythonparity.ParseUUID(text)
		if parseErr != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope repo_id: %w", parseErr)
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
// scopeString applies Python's truthiness gate to one raw scope value and
// returns the string Python would then parse.
//
// present=false means Python takes the DEFAULT window. present=true with a
// non-nil error means Python reaches its parser and RAISES, so this step must
// fail too rather than quietly defaulting.
//
// Measured against the deployed interpreter rather than assumed, because an
// earlier version of this adapter guessed and was wrong TWICE:
//
//	''        FALSY  -> default window
//	'\t'      TRUTHY -> ValueError: Invalid isoformat string
//	'   '     TRUTHY -> ValueError: Invalid isoformat string
//	false     FALSY  -> default window
//	0         FALSY  -> default window
//	123       TRUTHY -> TypeError: argument must be str
//
// The whitespace rows are the ones that matter: a whitespace-only string is
// NON-EMPTY and therefore TRUTHY, so Python parses and fails. Trimming before
// the emptiness test -- which the previous version did -- turns a request
// Python REJECTS into a default 30-day window here, and this step writes
// mapping rows before the bridge ever gets to reject it.
func scopeString(raw json.RawMessage) (string, bool, error) {
	trimmed := strings.TrimSpace(string(raw))
	switch trimmed {
	case "", "null", "false", "[]", "{}", `""`:
		// Falsy in Python: absent, and the default applies.
		return "", false, nil
	}
	// Any JSON number equal to zero is falsy in Python, not just the literal
	// `0`: `0.0`, `-0.0`, `0e100` and `1e-400` all decode to 0.0 and are falsy.
	// Matching only the literal treated them as truthy non-strings and failed
	// the build where the reference takes the default window.
	if number, err := strconv.ParseFloat(trimmed, 64); err == nil {
		if number == 0 {
			return "", false, nil
		}
		return "", true, fmt.Errorf("%s is a number; the reference raises TypeError on it", trimmed)
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		// Truthy but not a string: Python raises TypeError from fromisoformat.
		return "", true, fmt.Errorf("%s is not a string; the reference raises on it", trimmed)
	}
	if text == "" {
		return "", false, nil
	}
	// NOT trimmed: a whitespace-only string is truthy in Python and reaches its
	// parser, which rejects it.
	return text, true, nil
}

// canonicalScopeShape is the ENTIRE accept set for a build-scope date.
//
// # Why this is a regexp and not a grammar
//
// Three consecutive review rounds found a dangerous-direction defect in this
// parser, each one introduced by the previous round's fix. The parser was
// trying to MIRROR `datetime.fromisoformat`, and that grammar is far larger and
// stranger than anyone's model of it: it takes any single character as the
// date/time separator, reads "+" as that separator when no time follows, folds
// an hour of 24 into the next day, and accepts five offset spellings. Every
// round closed a cell and left the surface intact.
//
// So the surface is gone. Go can only be guaranteed to accept nothing the
// reference rejects if Go accepts almost nothing, and the accept set is now
// exactly what the PRODUCERS emit, measured rather than imagined:
//
//	YYYY-MM-DDTHH:MM:SSZ        cmd/dev-health-worker/sync_dispatch.go:334,337
//	                            `plan.To.UTC().Format(time.RFC3339)` -- the LIVE
//	                            Go post-sync emitter, and all 744 dated values
//	                            in the proof org's work_graph_execution_requests
//	YYYY-MM-DDTHH:MM:SS+00:00   src/dev_health_ops/workers/post_sync_dispatch.py
//	                            :111-118, the Python plane's midnight form
//	YYYY-MM-DD                  the same file's `to_date_str` fallback, and the
//	                            Go emitter's investment.materialize branch
//
// Everything else the reference accepts is REFUSED, fail-closed, under one
// enumerated class. That direction costs a failed build for a scope no producer
// writes; the other direction writes mapping rows for a build the bridge then
// rejects.
//
// A "+00:00" is spelled literally here rather than parsed as an offset, so a
// non-zero offset cannot match at all -- there is no arithmetic left to get
// wrong. That deleted `splitTrailingOffset`, `parseOffsetSeconds`,
// `offsetSpelling` and two layout tables, and with them every defect class the
// three rounds found.
var canonicalScopeShape = regexp.MustCompile(
	`^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(Z|\+00:00))?$`)

// parseScopeInstant accepts exactly the shapes above, then applies the
// reference's own value checks.
//
// The shape gate is necessary but NOT sufficient: it constrains the digits'
// POSITIONS, not their values. "2026-02-30" and "0000-01-01" both match it, and
// the reference rejects both -- so calendar validity (from time.Parse) and the
// year range still run behind it.
func parseScopeInstant(value string) (time.Time, error) {
	// NOT trimmed: the caller has already applied Python's truthiness, and a
	// whitespace-only value must reach here and be REJECTED, exactly as it is by
	// fromisoformat.
	if !canonicalScopeShape.MatchString(value) {
		return time.Time{}, fmt.Errorf(
			"%q is not a canonical build-scope date; the producers emit only "+
				"YYYY-MM-DD, YYYY-MM-DDTHH:MM:SSZ and YYYY-MM-DDTHH:MM:SS+00:00", value)
	}
	layout := "2006-01-02"
	if len(value) > len("2006-01-02") {
		// Both zero-offset spellings; the shape gate has already excluded every
		// other offset, so a successful parse is necessarily UTC.
		layout = "2006-01-02T15:04:05Z07:00"
	}
	// time.Parse supplies the calendar validity the reference also enforces: it
	// rejects month 13, day 32, and 29 February in a common year.
	parsed, err := time.ParseInLocation(layout, value, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is not a valid date: %w", value, err)
	}
	return withPythonYearRange(value, parsed.UTC())
}

// withPythonYearRange applies `datetime`'s own year bound to a parsed value.
//
// CPython raises "year must be in 1..9999, not 0" and Go's time.Parse does not:
// the "2006" layout element happily reads "0000". A year-zero bound is not
// merely wrong but MEANINGLESS downstream -- ClickHouse reads the Go driver's
// `0000-01-01 00:00:00` as `2026-01-01 00:00:00` and returns rows for a window
// nobody asked for.
//
// Applied at the parse AND at the derived lower bound (see windowFor): the
// parse is where a value enters, but `to - 30 days` is a SECOND value the
// reference range-checks, and it can leave the range when the parse did not.
func withPythonYearRange(original string, parsed time.Time) (time.Time, error) {
	if year := parsed.Year(); year < 1 || year > 9999 {
		return time.Time{}, fmt.Errorf(
			"%q has year %d; the reference requires 1..9999", original, year)
	}
	return parsed, nil
}
