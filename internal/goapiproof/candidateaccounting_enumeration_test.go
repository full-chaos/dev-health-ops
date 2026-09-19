package goapiproof

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// This file proves the pull-request drilldown family's accounting
// invariant by enumeration, through the real bound Options of both routes:
// whenever a comparison is admitted with nothing outside, every candidate
// row equals one whole baseline copy of its id, the candidate is ordered
// newest first, nothing the baseline returned is missing, and the only
// rows without a baseline copy sit after every shared row of a page-cut
// baseline, within the request's own limit.

// enumRow is one generated row: id and title variant ("1", "2" are the two
// physical titles the baseline can carry, "9" a title no copy carries);
// rank orders created_at newest first.
type enumRow struct {
	id    string
	title string
	rank  int
}

var enumRank = map[string]int{"A": 1, "B": 2, "C": 3, "X": 4}

// json renders the row as a plane serves it: the reference plane's naive
// timestamps, or the candidate plane's RFC 3339 form with an offset.
func (r enumRow) json(candidate bool) map[string]any {
	createdAt := fmt.Sprintf("2024-01-0%dT00:00:00", 9-r.rank)
	if candidate {
		createdAt += "Z"
	}
	return map[string]any{
		"repo_id":    "ABC",
		"number":     json.Number(fmt.Sprint(r.rank)),
		"title":      "t" + r.title,
		"author":     "a",
		"created_at": createdAt,
	}
}

// enumBaselines lists every baseline over the prefixes {A}, {A,B},
// {A,B,C} with one to three physical copies per id; A's copies carry title
// "1" or "2".
func enumBaselines() [][]enumRow {
	aCopies := [][]string{{"1"}, {"1", "1"}, {"1", "2"}, {"1", "1", "1"}, {"1", "1", "2"}, {"1", "2", "2"}}
	counts := []int{1, 2, 3}
	var out [][]enumRow
	for _, a := range aCopies {
		var rows []enumRow
		for _, title := range a {
			rows = append(rows, enumRow{"A", title, enumRank["A"]})
		}
		out = append(out, rows)
		for _, b := range counts {
			withB := append(append([]enumRow{}, rows...), repeatRow("B", b)...)
			out = append(out, withB)
			for _, c := range counts {
				out = append(out, append(append([]enumRow{}, withB...), repeatRow("C", c)...))
			}
		}
	}
	return out
}

func repeatRow(id string, n int) []enumRow {
	rows := make([]enumRow, n)
	for i := range rows {
		rows[i] = enumRow{id, "1", enumRank[id]}
	}
	return rows
}

// enumSymbols is the candidate alphabet: both physical titles of A, a
// title no copy carries, B, C, and an id no baseline carries.
var enumSymbols = []enumRow{{"A", "1", 1}, {"A", "2", 1}, {"A", "9", 1}, {"B", "1", 2}, {"C", "1", 3}, {"X", "1", 4}}

// enumCandidates lists every sequence over enumSymbols up to maxLen:
// every permutation, substitution, insertion and deletion relative to any
// baseline of that length range.
func enumCandidates(maxLen int) [][]enumRow {
	out := [][]enumRow{{}}
	frontier := [][]enumRow{{}}
	for n := 1; n <= maxLen; n++ {
		var next [][]enumRow
		for _, prefix := range frontier {
			for _, s := range enumSymbols {
				next = append(next, append(append([]enumRow{}, prefix...), s))
			}
		}
		out = append(out, next...)
		frontier = next
	}
	return out
}

// enumInvariant is the oracle, written from the invariant alone.
// allowDropped permits a baseline id absent from the candidate, the one
// violation a team scope's narrower population may carry.
func enumInvariant(base, cand []enumRow, limit int, allowDropped bool) bool {
	titles := map[string]map[string]bool{}
	for _, r := range base {
		if titles[r.id] == nil {
			titles[r.id] = map[string]bool{}
		}
		titles[r.id][r.title] = true
	}
	if len(cand) > limit {
		return false
	}
	seen := map[string]bool{}
	lastShared, firstOnly := -1, -1
	for i, r := range cand {
		if seen[r.id] {
			return false
		}
		seen[r.id] = true
		if i > 0 && r.rank < cand[i-1].rank {
			return false
		}
		copies, shared := titles[r.id]
		if !shared {
			if firstOnly < 0 {
				firstOnly = i
			}
			continue
		}
		if !copies[r.title] {
			return false
		}
		lastShared = i
	}
	for id := range titles {
		if !seen[id] && !allowDropped {
			return false
		}
	}
	if firstOnly < 0 {
		return true
	}
	return len(base) >= limit && firstOnly > lastShared
}

func enumSnapshot(t *testing.T, rows []enumRow, candidate bool) Snapshot {
	items := make([]any, len(rows))
	for i, r := range rows {
		items[i] = r.json(candidate)
	}
	raw, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	snap.Data = InjectRESTDedupKeys(snap.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
	return snap
}

// enumOnlyShape keeps the datetime entry plus every entry binding the
// named duplicate shape field; enumWholeFamily keeps every entry.
func enumOnlyShape(opts Options, field string) Options {
	if field == enumWholeFamily {
		return opts
	}
	kept := opts
	kept.BaselineDefects = nil
	for _, d := range opts.BaselineDefects {
		value := reflect.ValueOf(d).FieldByName(field)
		if d.TimestampRenderingShape != nil || !value.IsNil() {
			kept.BaselineDefects = append(kept.BaselineDefects, d)
		}
	}
	return kept
}

const enumWholeFamily = "whole family"

func rowsString(rows []enumRow) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = r.id + r.title
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariant runs every
// generated baseline against every candidate of up to three rows, with and
// without a page cut, through each duplicate shape of each route's bound
// Options. The integration build runs the same enumeration up to four rows.
func TestCandidateAccounting_EnumeratedAdmissionImpliesTheInvariant(t *testing.T) {
	runAccountingEnumeration(t, 3)
}

// runAccountingEnumeration runs the enumeration with candidates up to
// maxLen rows.
func runAccountingEnumeration(t *testing.T, maxLen int) {
	routes := map[string]func(limit int) Options{
		"scope route":       drilldownPRsParityWithLimit,
		"person route":      func(limit int) Options { return parityWithPageCutLimit(personDrilldownPRsParity, limit) },
		"team-scoped route": drilldownPRsTeamScopedParityWithLimit,
	}
	shapes := []string{"WorkGraphEdgeDedupShape", "DuplicateCollapseLengthShape", "DuplicateCollapsePageCutShape", "TimestampRenderingShape", "TeamRepoSubsetShape", enumWholeFamily}
	baselines := enumBaselines()
	candidates := enumCandidates(maxLen)
	// Compare annotates findings and never writes to a snapshot's data, so
	// each generated body is decoded once and shared by every comparison.
	baseSnaps := make([]Snapshot, len(baselines))
	for i, base := range baselines {
		baseSnaps[i] = enumSnapshot(t, base, false)
	}
	candSnaps := make([]Snapshot, len(candidates))
	for i, cand := range candidates {
		candSnaps[i] = enumSnapshot(t, cand, true)
	}
	admitted := map[string]int{}
	compared := 0
	for routeName, bind := range routes {
		for _, cut := range []bool{false, true} {
			for bi, base := range baselines {
				limit := drilldownPRsDefaultLimit
				if cut {
					limit = len(base)
				}
				for _, shape := range shapes {
					// The team-subset declaration exists on the team-scoped
					// route alone; there, and in that route's whole
					// Options, a dropped row is the one permitted
					// violation.
					if shape == "TeamRepoSubsetShape" && routeName != "team-scoped route" {
						continue
					}
					allowDropped := routeName == "team-scoped route" && (shape == "TeamRepoSubsetShape" || shape == enumWholeFamily)
					opts := enumOnlyShape(bind(limit), shape)
					for ci, cand := range candidates {
						compared++
						result := Compare(baseSnaps[bi], candSnaps[ci], opts)
						// Admitted: at least one finding, every one of them
						// covered. A pair with no finding at all is equal
						// bodies, which no declaration admits.
						if result.DifferencesOutsideBaselineDefect != 0 || result.StructuralRefusal != "" || !hasMismatch(result) {
							continue
						}
						relation := "equal"
						switch {
						case len(cand) < len(base):
							relation = "shorter"
						case len(cand) > len(base):
							relation = "longer"
						}
						admitted[fmt.Sprintf("%s/%s/cut=%v", shape, relation, cut)]++
						if !enumInvariant(base, cand, limit, allowDropped) {
							t.Fatalf("%s %s cut=%v: baseline %s candidate %s admitted, invariant broken", routeName, shape, cut, rowsString(base), rowsString(cand))
						}
					}
				}
			}
		}
	}
	t.Logf("compared %d, admitted by class %v", compared, admitted)
	for _, shape := range shapes {
		any := 0
		for key, n := range admitted {
			if strings.HasPrefix(key, shape+"/") && !strings.Contains(key, "/equal/") && n > 0 {
				any += n
			}
			if strings.HasPrefix(key, shape+"/equal/") {
				any += n
			}
		}
		if any == 0 {
			t.Errorf("%s admitted nothing: the enumeration never exercised it", shape)
		}
	}
}

func hasMismatch(result Result) bool {
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch {
			return true
		}
	}
	return false
}
