// Package scopeparity compares a Go scope adapter against the reference's own
// admission, over a corpus generated from a grammar.
//
// # Why a library rather than a test
//
// The adapter under test (`windowFor` in cmd/dev-health-worker) and the
// generator that measures the reference live in different places, and a second
// adapter — the investment materializer's, CHAOS-4441 — will need the same
// comparison. Putting the comparison in a package means one asymmetric rule and
// one disagreement report, rather than each caller re-deciding what counts as a
// divergence. The callers supply an Adapter and their own divergence list.
//
// # The asymmetric rule, which is the whole point
//
// These adapters run BEFORE the reference validates, and they WRITE. So the two
// directions are not equally bad:
//
//   - Reference RAISES, Go ACCEPTS: never acceptable. The adapter does its
//     writes and the bridge then rejects the request, leaving rows behind for a
//     build that never legitimately ran. This is not enumerable as a
//     "divergence" — it is a defect, always.
//   - Reference RUNS, Go REFUSES: fail-closed. Nothing is written and the build
//     fails where the reference would have run. Still a divergence, but it may
//     be enumerated with a reason.
//
// Compare enforces exactly that: the first direction cannot be silenced by an
// entry in the divergence list.
package scopeparity

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Resolved is what an adapter derives from a scope: the reference's three
// outputs, in a form neither plane's own types leak into.
type Resolved struct {
	From   time.Time
	To     time.Time
	RepoID string // canonical UUID text, or "" when unset
}

// Adapter is the Go side under test. It returns the window it would use, or an
// error if it refuses the scope.
type Adapter func(scope json.RawMessage) (Resolved, error)

// Corpus is the generator's output: documents plus the reference's verdict.
type Corpus struct {
	Schema     string `json:"schema"`
	Seed       int    `json:"seed"`
	MeasuredOn string `json:"measured_on"`
	FrozenNow  string `json:"frozen_now"`
	Cases      []Case `json:"cases"`
}

// Case is one measured document.
type Case struct {
	Scope   json.RawMessage `json:"scope"`
	Verdict string          `json:"verdict"` // RAISES | RUNS
	Stage   string          `json:"stage"`
	Error   string          `json:"error"`
	Window  *Window         `json:"window"`
}

// Window is the reference's derived window, as ISO text.
type Window struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	RepoID *string `json:"repo_id"`
}

// Kind classifies a disagreement.
type Kind string

const (
	// KindAcceptedRejected is the direction that writes rows for a doomed
	// build. Never enumerable.
	KindAcceptedRejected Kind = "go_accepted_what_the_reference_rejects"
	// KindRefusedAccepted is fail-closed: nothing written, build fails.
	KindRefusedAccepted Kind = "go_refused_what_the_reference_accepts"
	// KindWindowDiffers is both planes running with different windows, which
	// selects a different row set.
	KindWindowDiffers Kind = "windows_differ"
)

// Disagreement is one case where the adapter and the reference part company.
type Disagreement struct {
	Scope       string
	Kind        Kind
	Detail      string
	Enumerable  bool
	Enumerated  bool
	ReferenceOK bool
}

func (d Disagreement) String() string {
	return fmt.Sprintf("%s: %s — %s", d.Scope, d.Kind, d.Detail)
}

// LoadCorpus reads a generated corpus.
func LoadCorpus(path string) (Corpus, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Corpus{}, fmt.Errorf("read corpus: %w", err)
	}
	var corpus Corpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		return Corpus{}, fmt.Errorf("decode corpus: %w", err)
	}
	if len(corpus.Cases) == 0 {
		return Corpus{}, fmt.Errorf("corpus %s has no cases", path)
	}
	return corpus, nil
}

// Compare runs every case through the adapter and reports disagreements.
//
// enumerated is keyed by the compacted scope document. An entry suppresses ONLY
// a fail-closed divergence; it can never suppress the adapter accepting a scope
// the reference rejects, and Compare reports a stale entry (one whose case now
// agrees) as its own disagreement, because a stale entry hides a real one.
func Compare(corpus Corpus, adapter Adapter, enumerated map[string]string) []Disagreement {
	var out []Disagreement
	for _, testCase := range corpus.Cases {
		key := compact(testCase.Scope)
		reason, isEnumerated := enumerated[key]
		resolved, err := adapter(testCase.Scope)

		switch {
		case testCase.Verdict == "RAISES" && err == nil:
			out = append(out, Disagreement{
				Scope: key, Kind: KindAcceptedRejected, Enumerable: false, Enumerated: isEnumerated,
				Detail: fmt.Sprintf(
					"reference raises %s at %s; the adapter accepted it and would write before the bridge rejects",
					testCase.Error, testCase.Stage),
			})
		case testCase.Verdict == "RAISES":
			// Agreement. A divergence entry for it is stale.
			if isEnumerated {
				out = append(out, staleEntry(key, reason))
			}
		case err != nil:
			if !isEnumerated {
				out = append(out, Disagreement{
					Scope: key, Kind: KindRefusedAccepted, Enumerable: true,
					Detail: fmt.Sprintf("reference runs with %s..%s; the adapter refused: %v",
						testCase.Window.From, testCase.Window.To, err),
				})
			}
		default:
			// The window comparison runs FIRST, and an enumerated entry does not
			// suppress it.
			//
			// This branch previously short-circuited to staleEntry and `continue`d
			// whenever an entry existed, so an adapter that SUCCEEDED with the
			// wrong window was reported as "the adapter now agrees with the
			// reference". That message is not merely a missed finding: it is
			// FALSE, and it points the reader at a bookkeeping chore while a live
			// divergence goes unreported.
			//
			// Enumeration cannot excuse it either. The contract above is that an
			// entry "suppresses ONLY a fail-closed divergence" -- the adapter
			// REFUSING a scope the reference runs, which is the `err != nil` arm.
			// A differing window is not fail-closed: a window skewed wider writes
			// rows the reference never derives, which is the dangerous direction
			// wearing different clothes. Suppressing it here would let an entry
			// reach that direction through a second door.
			//
			// Enumerated is still recorded so a reader can see an entry exists.
			detail, differs := windowDiffers(testCase.Window, resolved)
			if differs {
				// Enumerable is FALSE: enumeration no longer suppresses this
				// kind, and a field claiming suppressibility that nothing
				// honours is a claim without enforcement.
				//
				// KNOWN LIMITATION, deliberately not fixed here. Suppressibility
				// for a window difference really depends on its DIRECTION, which
				// this Kind cannot express: a NARROWER window writes fewer rows
				// than the reference (fail-closed, legitimately excusable), a
				// WIDER one writes rows the reference never derives (the
				// dangerous direction). windowDiffers returns on the first
				// unequal bound and carries no notion of which way, so both
				// arrive as the same Kind. Treating all of them as
				// non-suppressible fails toward REPORTING, which is the safe
				// default; distinguishing them needs the corpus to say whether
				// narrower windows occur at all, and is its own change.
				out = append(out, Disagreement{
					Scope: key, Kind: KindWindowDiffers, Enumerable: false,
					Enumerated: isEnumerated, Detail: detail,
				})
				continue
			}
			// Only now is agreement established, so only now can an entry be stale.
			if isEnumerated {
				out = append(out, staleEntry(key, reason))
			}
		}
	}
	return out
}

func staleEntry(scope, reason string) Disagreement {
	return Disagreement{
		Scope: scope, Kind: "stale_enumerated_divergence", Enumerable: false,
		Detail: fmt.Sprintf(
			"listed as a divergence (%s) but the adapter now agrees with the reference; "+
				"remove the entry — a stale divergence hides a real one", reason),
	}
}

func windowDiffers(reference *Window, resolved Resolved) (string, bool) {
	if reference == nil {
		return "reference reported RUNS with no window", true
	}
	from, err := parseInstant(reference.From)
	if err != nil {
		return fmt.Sprintf("cannot parse the reference's own from bound %q: %v", reference.From, err), true
	}
	to, err := parseInstant(reference.To)
	if err != nil {
		return fmt.Sprintf("cannot parse the reference's own to bound %q: %v", reference.To, err), true
	}
	if !resolved.From.Equal(from) {
		return fmt.Sprintf("from: adapter %s, reference %s", resolved.From, reference.From), true
	}
	if !resolved.To.Equal(to) {
		return fmt.Sprintf("to: adapter %s, reference %s", resolved.To, reference.To), true
	}
	referenceRepo := ""
	if reference.RepoID != nil {
		referenceRepo = *reference.RepoID
	}
	if resolved.RepoID != referenceRepo {
		return fmt.Sprintf("repo_id: adapter %q, reference %q", resolved.RepoID, referenceRepo), true
	}
	return "", false
}

// parseInstant decodes `datetime.isoformat()` at whatever precision it used,
// without discarding any of it. Comparing a truncated expectation against an
// untruncated value reports divergences that do not exist — a mistake made once
// already in this family of tests.
func parseInstant(value string) (time.Time, error) {
	for _, layout := range []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("no layout matched %q", value)
}

func compact(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return string(raw)
	}
	return string(encoded)
}
