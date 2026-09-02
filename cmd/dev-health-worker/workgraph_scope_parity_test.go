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

	// BASIC DATE crossed with an OFFSET. The value axis carried basic dates and
	// it carried offsets, but never the two together -- and the Go layout table
	// pairs offsets only with EXTENDED dates, so the crossing is exactly where a
	// divergence could hide. It did. Eight values, sixteen failing rows (each
	// value under both to_date and from_date), every one of them fail-closed.
	//
	// Enumerated rather than fixed, deliberately, and this is a CHOICE rather
	// than an oversight: adding the eight layouts would be a behaviour change to
	// the parser, and nothing in the tree writes a build scope with dates at all
	// (the fixed producer persists `{}`), so the forms are unreachable. A loud
	// refusal is the right failure for an unreachable input. If a producer ever
	// starts emitting basic-format dates, these entries turn into the work item.
	`"20260815T060708Z"`: {
		want: "RAISES",
		reason: "basic date with a zero offset: the reference accepts it and Go's offset layouts " +
			"are all extended-date. Fail-closed and unreachable; see the block comment above.",
	},
	`"20260815T060708+00:00"`: {
		want:   "RAISES",
		reason: "basic date with a zero offset; same as the Z form.",
	},
	`"20260815T060708+0000"`: {
		want:   "RAISES",
		reason: "basic date, colon-less zero offset; same reasoning.",
	},
	`"20260815T060708+00"`: {
		want:   "RAISES",
		reason: "basic date, hour-only zero offset; same reasoning.",
	},
	`"20260815T0607+00:00"`: {
		want:   "RAISES",
		reason: "basic date at minute precision with a zero offset; same reasoning.",
	},
	`"20260815T060708+05:00"`: {
		want: "RAISES",
		reason: "basic date with a NON-ZERO offset. Refused for two independent reasons: the " +
			"basic-date layout gap above, and the same non-zero-offset hazard already enumerated " +
			"for the extended form -- strftime keeps the wall clock and discards the offset.",
	},
	`"20260815T060708-08:00"`: {
		want:   "RAISES",
		reason: "same as the +05:00 basic-date case.",
	},
	// This one is not a layout gap. It is the reference doing something a reader
	// would never predict, and it is the strongest argument in this table for
	// refusing rather than matching.
	//
	// `fromisoformat` accepts ANY single character as the date/time separator,
	// so in "20260815+05:00" the `+` is read as the SEPARATOR and "05:00" as a
	// wall-clock TIME -- not as an offset at all:
	//
	//	fromisoformat("20260815+05:00")  -> 2026-08-15T05:00:00   tzinfo=None
	//	fromisoformat("20260815+00:00")  -> 2026-08-15T00:00:00   tzinfo=None
	//	fromisoformat("20260815Z")       -> raises
	//
	// So a value that every reader parses as "midnight at UTC+5" becomes "05:00,
	// timezone unknown", silently, and the window moves five hours. Matching the
	// reference here would mean reproducing that reinterpretation on purpose.
	// Refusing is fail-closed AND correct-by-intent, which is a rarer alignment
	// than it sounds -- the other entries trade one for the other.
	// The OTHER half of the same cross-product, named by the same review: an
	// EXTENDED date at MINUTE precision, or with a space separator, crossed with
	// an offset. Go's offset layouts carry minute precision only in the
	// colon-less ("+0000") and hour-only ("+00") spellings, and carry no
	// space-separated form with an offset at all.
	//
	// Recorded together with the basic-date entries above so that neither half
	// can be closed while the other stands -- fixing only the half that was
	// easiest to see is how this gap survived to a seventh round.
	//
	// "2026-08-15T06:07+0000" is deliberately ABSENT: Go already accepts it via
	// the colon-less layout, so an entry for it would be stale on arrival.
	`"2026-08-15T06:07+00:00"`: {
		want: "RAISES",
		reason: "extended date, minute precision, colon-form zero offset. Go has the colon-less " +
			"and hour-only spellings at this precision but not this one. Fail-closed; unreachable " +
			"for the same reason as the basic-date block above.",
	},
	`"2026-08-15T06:07Z"`: {
		want:   "RAISES",
		reason: "minute precision with Z; same layout gap.",
	},
	`"2026-08-15T06:07+05:00"`: {
		want: "RAISES",
		reason: "minute precision with a NON-ZERO offset: refused both by the layout gap and by " +
			"the non-zero-offset rule already enumerated for the second-precision form.",
	},
	`"2026-08-15 06:07:08+00:00"`: {
		want: "RAISES",
		reason: "space separator with a zero offset. Go accepts the space separator WITHOUT an " +
			"offset and every offset layout uses \"T\", so the combination has no layout.",
	},
	`"2026-08-15 06:07:08Z"`: {
		want:   "RAISES",
		reason: "space separator with Z; same gap.",
	},

	`"20260815+00:00"`: {
		want: "RAISES",
		reason: "the reference reads `+` as the date/time separator and \"00:00\" as a wall-clock " +
			"time, yielding a NAIVE 2026-08-15T00:00:00 rather than an offset. Refused rather " +
			"than reproduced; see the comment above.",
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
