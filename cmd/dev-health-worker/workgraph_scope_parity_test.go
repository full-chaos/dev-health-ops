package main

import (
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
	Schema string            `json:"schema"`
	Cases  []scopeParityCase `json:"cases"`
}

type scopeParityCase struct {
	Value   json.RawMessage `json:"value"`
	Verdict string          `json:"verdict"` // DEFAULT | PARSED | RAISES
	ISO     string          `json:"iso"`
	Error   string          `json:"error"`
}

// divergence records a shape where Go deliberately differs from the reference,
// with the reason. Each is a decision on the record, not a gap.
type divergence struct {
	want   string // the Go verdict this case must produce instead
	reason string
}

// enumeratedDivergences is keyed by the case's raw JSON value.
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
	if table.Schema != "build_scope_parity_table.v1" {
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

// TestBuildScopeMatchesTheMeasuredReference is the differential.
func TestBuildScopeMatchesTheMeasuredReference(t *testing.T) {
	table := loadScopeParityTable(t)
	step := frozenPreStep()
	defaultTo := time.Date(2026, 9, 1, 12, 30, 45, 500_000_000, time.UTC)

	for _, testCase := range table.Cases {
		raw := string(testCase.Value)
		t.Run(raw, func(t *testing.T) {
			scope := []byte(fmt.Sprintf(`{"to_date":%s}`, raw))
			window, err := step.windowFor(scope)

			got := "PARSED"
			switch {
			case err != nil:
				got = "RAISES"
			case window.To.Equal(defaultTo):
				got = "DEFAULT"
			}

			want := testCase.Verdict
			if diverge, enumerated := enumeratedDivergences[raw]; enumerated {
				if got == want {
					t.Fatalf(
						"%s is listed as a divergence (%s) but Go now AGREES with the reference "+
							"(%s). Remove the entry — a stale divergence hides a real one.",
						raw, diverge.reason, got,
					)
				}
				want = diverge.want
			}

			if got != want {
				t.Fatalf(
					"scope value %s: Go says %s, the reference says %s.\n"+
						"Either fix the adapter, or add an entry to enumeratedDivergences with a "+
						"REASON. Do not widen a parsing rule to make this pass — that is how this "+
						"gate was got wrong twice.",
					raw, got, testCase.Verdict,
				)
			}

			// Where both parse, the INSTANT must agree too, not just the verdict.
			//
			// Compared at full precision on purpose. An earlier version of this
			// assertion sliced the reference's ISO string to 19 characters and
			// so compared a truncated expectation against an untruncated value,
			// which reported a divergence that did not exist: both planes carry
			// the fractional second at THIS layer, and the truncation to whole
			// seconds happens further down (issueprlinks.truncateBoundToSecond
			// on one side, strftime on the other). An assertion that quietly
			// drops precision is the same class of defect as a parser that
			// quietly widens.
			if got == "PARSED" && testCase.ISO != "" {
				wantInstant, parseErr := parseReferenceISO(testCase.ISO)
				if parseErr != nil {
					t.Fatalf("could not parse the reference's own output %q: %v", testCase.ISO, parseErr)
				}
				if !window.To.Equal(wantInstant) {
					t.Errorf("scope %s: Go parsed %s, the reference parsed %s", raw, window.To, testCase.ISO)
				}
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
		measured[string(testCase.Value)] = struct{}{}
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
