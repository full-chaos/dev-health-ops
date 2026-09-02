package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
	from := to.AddDate(0, 0, -30)
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
		repoID, parseErr := parsePythonUUID(text)
		if parseErr != nil {
			return issueprlinks.Window{}, fmt.Errorf("build scope repo_id: %w", parseErr)
		}
		window.RepoID = &repoID
	}
	return window, nil
}

// parsePythonUUID mirrors `uuid.UUID(hex=...)`'s own normalisation rather than
// using a general-purpose parser, because the two accept DIFFERENT SETS.
//
// CPython does, in order: strip a `urn:` prefix, strip a `uuid:` prefix, strip
// surrounding braces, remove hyphens, then require exactly 32 hex digits. Every
// step is CASE-SENSITIVE, which is the whole point: `urn:uuid:X` is accepted and
// `URN:UUID:X` is NOT, because the uppercase prefix is never stripped and the
// leftover colons fail the length check.
//
// google/uuid.Parse is case-INSENSITIVE about that prefix, so it accepted
// `URN:UUID:X` — a value the reference rejects. This step runs before the
// bridge and writes rows, so that direction persists mapping rows for a build
// the bridge is about to fail. It also REJECTED three spellings the reference
// accepts (`{unhyphenated}`, `urn:uuid:unhyphenated`, `urn:uuid:{...}`).
//
// Measured, not inferred: the accepted set is a row of the parity corpus under
// the repo_id field, so a change to either plane fails the differential.
func parsePythonUUID(value string) (uuid.UUID, error) {
	hex := strings.Replace(value, "urn:", "", 1)
	hex = strings.Replace(hex, "uuid:", "", 1)
	hex = strings.TrimPrefix(hex, "{")
	hex = strings.TrimSuffix(hex, "}")
	hex = strings.ReplaceAll(hex, "-", "")
	if len(hex) != 32 {
		return uuid.Nil, fmt.Errorf("%q is not a UUID the reference accepts", value)
	}
	parsed, err := uuid.Parse(hex)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%q is not a UUID the reference accepts", value)
	}
	return parsed, nil
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

func parseScopeInstant(value string) (time.Time, error) {
	// NOT trimmed: the caller has already applied Python's truthiness, and a
	// whitespace-only value must reach the parser and be REJECTED here, exactly
	// as it is by fromisoformat.
	trimmed := value
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
		// Basic (hyphen-less) date-times, which fromisoformat also accepts.
		"20060102T150405", "20060102T1504",
	} {
		if parsed, err := time.ParseInLocation(layout, trimmed, time.UTC); err == nil {
			return parsed, nil
		}
	}
	// Zero-offset forms, including the colon-less "+0000" that RFC3339 rejects
	// but fromisoformat accepts.
	for _, layout := range []string{
		time.RFC3339,
		"2006-01-02T15:04:05-0700", "2006-01-02T15:04-0700",
		// fromisoformat also accepts hour-only and second-precision offsets.
		"2006-01-02T15:04:05-07", "2006-01-02T15:04-07",
		"2006-01-02T15:04:05-07:00:00",
	} {
		parsed, err := time.Parse(layout, trimmed)
		if err != nil {
			continue
		}
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
