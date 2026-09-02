package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The build-scope gate has been got wrong TWICE by reasoning about it: once
// treating an empty string as a parse error, once treating a whitespace-only
// string as absent. On BOTH occasions a hand-written Go test asserted the wrong
// behaviour and passed, because the test and the code shared one author's wrong
// model of the reference.
//
// So the accept/reject set is no longer argued. It is MEASURED from the
// reference interpreter into tests/fixtures/build_scope_parity_table.json, and
// this test diffs the adapter against every row. A shape that neither matches
// nor appears in the enumerated divergence list below is a FAILURE — the
// divergences have to be listed one by one, with a reason, rather than covered
// by a rule someone can widen later without noticing.
//
// This is the structural replacement for prose justification, and the direct
// consequence of being wrong twice in the same place.

type scopeParityTable struct {
	Schema    string            `json:"schema"`
	FrozenNow string            `json:"frozen_now"`
	Measured  string            `json:"measured_on"`
	Cases     []scopeParityCase `json:"cases"`
}

type scopeParityCase struct {
	Case    string          `json:"case"`
	Scope   json.RawMessage `json:"scope"`
	Verdict string          `json:"verdict"` // RAISES | RUNS
	Stage   string          `json:"stage"`
	Error   string          `json:"error"`
	Window  *struct {
		From   string  `json:"from"`
		To     string  `json:"to"`
		RepoID *string `json:"repo_id"`
	} `json:"window"`
}

// divergence records a shape where Go deliberately differs from the reference,
// with the reason. Each is a decision on the record, not a gap.
type divergence struct {
	want   string // the Go verdict this case must produce instead
	reason string
}

// enumeratedDivergences is keyed by the VALUE, because these divergences are
// properties of the value's FORM, not of which field carries it. Keying on the
// whole document made the same form diverge silently under from_date while
// being enumerated under to_date -- which the field axis exposed immediately.
//
// Every entry REFUSES something the reference accepts. None accepts something
// the reference refuses — that direction would let this step write mapping rows
// for a request the build is about to reject, which is the defect class that
// produced the second failure of this gate.
var enumeratedDivergences = map[string]divergence{
	`"2026-08-15T06:07:08+05:00"`: {
		want: "RAISES",
		reason: "non-zero UTC offset: the reference's strftime keeps the wall-clock fields and " +
			"DISCARDS the offset, so rendering it would silently pick one of two possible instants. " +
			"Refused rather than guessed; nothing produces such a scope today.",
	},
	`"2026-08-15T06:07:08-08:00"`: {
		want:   "RAISES",
		reason: "same as the +05:00 case.",
	},
	`"2026-W33-6"`: {
		want: "RAISES",
		reason: "ISO week date. Accepted by fromisoformat, refused here: nothing in the tree writes " +
			"a build scope with dates at all (the fixed producer persists `{}`), so it is unreachable, " +
			"and a loud refusal beats silently computing a different window.",
	},
	`"2026-08-15t06:07:08"`: {
		want:   "RAISES",
		reason: "lowercase separator; same unreachability reasoning as the ISO week date.",
	},
	`"2026-08-15_06:07:08"`: {
		want:   "RAISES",
		reason: "arbitrary single-character separator; same reasoning.",
	},
	// NOTE: "2026-08-15T06:07:08.123" was listed here on the assumption that Go
	// would refuse a fractional second its layout does not name. It does not —
	// time.Parse accepts a fractional second immediately after seconds even when
	// the layout omits it, so both planes parse it and the truncation to whole
	// seconds then applies equally. The entry was removed when this very test
	// reported it as a STALE divergence, roughly a minute after being written.
	// Recorded because it is the third time this gate was got wrong by
	// reasoning, and the first time the mechanism caught it instead of a
	// reviewer.
}

// canonicalScope compacts a scope document so the divergence list can be keyed
// on it regardless of how the generator indented the fixture.
func canonicalScope(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, raw); err != nil {
		t.Fatalf("compact scope %s: %v", raw, err)
	}
	return buffer.String()
}

// parsedScopeFields are the fields this step actually reads. `heuristic_window`
// and `heuristic_confidence` are ADMISSIBLE to the bridge but belong to the
// heuristic edge builder, so their values never reach this step's parser.
var parsedScopeFields = map[string]struct{}{
	"to_date": {}, "from_date": {}, "repo_id": {},
}

// scopeValueKey returns the divergence key for a one-field scope: the VALUE,
// but ONLY when the field is one this step parses.
//
// A divergence here is a property of the value's FORM crossed with WHETHER THIS
// STEP PARSES THAT FIELD — not of the form alone. The same ISO week date is a
// deliberate refusal under `to_date` (this step parses it) and simply passes
// through under `heuristic_window` (it does not), so Go correctly agrees with
// the reference in the second case. Keying on the form alone marked those rows
// as stale divergences; the field axis is what made that visible.
func scopeValueKey(t *testing.T, raw json.RawMessage) (string, bool) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 1 {
		return "", false
	}
	for field, value := range fields {
		if _, parsed := parsedScopeFields[field]; !parsed {
			return "", false
		}
		return canonicalScope(t, value), true
	}
	return "", false
}

func loadScopeParityTable(t *testing.T) scopeParityTable {
	t.Helper()
	path := filepath.Join(repositoryRoot(t), "tests", "fixtures", "build_scope_parity_table.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the measured scope table: %v", err)
	}
	var table scopeParityTable
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatalf("decode the measured scope table: %v", err)
	}
	if table.Schema != "build_scope_parity_table.v2" {
		t.Fatalf("unexpected table schema %q", table.Schema)
	}
	if len(table.Cases) == 0 {
		t.Fatal("the measured scope table is empty")
	}
	return table
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find the repository root")
		}
		directory = parent
	}
}

// TestBuildScopeMatchesTheBridgeAdmission is the differential, and it asserts
// the property that actually matters rather than parser agreement:
//
//	the reference RAISES => Go must REFUSE, before writing anything
//	the reference RUNS   => Go's window must EQUAL the reference's
//
// The first direction is the one that bites. This step runs before the bridge
// and writes to work_graph_issue_pr, so Go accepting a scope the bridge will
// reject persists mapping rows for a build that never legitimately ran. Three
// separate defects across three review rounds were exactly that shape.
func TestBuildScopeMatchesTheBridgeAdmission(t *testing.T) {
	table := loadScopeParityTable(t)
	step := frozenPreStep()

	for index, testCase := range table.Cases {
		name := fmt.Sprintf("%d_%s_%s", index, testCase.Case, string(testCase.Scope))
		t.Run(name, func(t *testing.T) {
			window, err := step.windowFor(testCase.Scope)

			var diverge divergence
			enumerated := false
			if key, parsed := scopeValueKey(t, testCase.Scope); parsed {
				diverge, enumerated = enumeratedDivergences[key]
			}

			if testCase.Verdict == "RAISES" {
				if err == nil {
					t.Fatalf(
						"scope %s: the reference RAISES (%s at %s) but Go accepted it and would "+
							"WRITE mapping rows for a build the bridge rejects. This direction is "+
							"never an acceptable divergence.",
						testCase.Scope, testCase.Error, testCase.Stage,
					)
				}
				return
			}

			// The reference RUNS.
			if enumerated {
				if err == nil {
					t.Fatalf(
						"scope %s is listed as a divergence (%s) but Go now agrees with the "+
							"reference. Remove the entry — a stale divergence hides a real one.",
						testCase.Scope, diverge.reason,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf(
					"scope %s: the reference RUNS with window %s..%s but Go refused: %v.\n"+
						"Either fix the adapter, or add an entry to enumeratedDivergences with a "+
						"REASON. Do not widen a parsing rule to make this pass.",
					testCase.Scope, testCase.Window.From, testCase.Window.To, err,
				)
			}

			wantFrom, parseErr := parseReferenceISO(testCase.Window.From)
			if parseErr != nil {
				t.Fatalf("cannot parse the reference's own from bound %q: %v", testCase.Window.From, parseErr)
			}
			wantTo, parseErr := parseReferenceISO(testCase.Window.To)
			if parseErr != nil {
				t.Fatalf("cannot parse the reference's own to bound %q: %v", testCase.Window.To, parseErr)
			}
			if !window.From.Equal(wantFrom) {
				t.Errorf("scope %s: from = %s, the reference derived %s", testCase.Scope, window.From, wantFrom)
			}
			if !window.To.Equal(wantTo) {
				t.Errorf("scope %s: to = %s, the reference derived %s", testCase.Scope, window.To, wantTo)
			}
		})
	}
}

// parseReferenceISO decodes what `datetime.isoformat()` produced, at whatever
// precision it used, without discarding any of it.
func parseReferenceISO(value string) (time.Time, error) {
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

// TestEveryEnumeratedDivergenceIsInTheTable stops the divergence list from
// accumulating entries for shapes nobody measures any more.
func TestEveryEnumeratedDivergenceIsInTheTable(t *testing.T) {
	table := loadScopeParityTable(t)
	measured := make(map[string]struct{}, len(table.Cases))
	for _, testCase := range table.Cases {
		if key, parsed := scopeValueKey(t, testCase.Scope); parsed {
			measured[key] = struct{}{}
		}
	}
	for raw, diverge := range enumeratedDivergences {
		if _, present := measured[raw]; !present {
			t.Errorf(
				"enumeratedDivergences lists %s (%s) but the measured table does not contain it; "+
					"add it to the generator's CASES or drop the entry",
				raw, diverge.reason,
			)
		}
	}
}
