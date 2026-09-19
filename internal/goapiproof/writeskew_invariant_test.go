package goapiproof

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// This file proves the write-skew invariant by exhaustive enumeration,
// not by chosen cells. For every generated case -- per leaf, the first
// baseline read B1, the candidate C1 and the second baseline read B2 each
// over {1, 2, 3, null, missing} (B2's numbers spelled differently: 1.0,
// 2e0, 3.00); per leaf, declaration coverage over {uncovered, covered by
// a plain declaration (admits any value difference), covered by a
// direction shape (admits only B1 > C1, refuses the rest), covered by a
// CONTESTED direction shape (admits only when this leaf is the case's
// only mismatch -- coverage that changes with the other leaves, so a
// declaration can admit a leaf in the second comparison it left outside
// in the first)}; one and two leaves per case, all pairs -- ClassifyWriteSkew
// must return exactly the verdict this oracle derives from the symbols:
//
//	not_applicable  when the first comparison leaves no outside finding,
//	                or any outside finding that is not a value leaf;
//	refused         when any outside leaf has B2 equal to neither B1 nor
//	                C1 (null and missing included), naming that leaf;
//	stands          else when any outside leaf has B2 == B1 (no write
//	                witnessed there), whatever the declarations would do
//	                with the second read;
//	skew_admitted   else (every outside leaf carries the witness
//	                B2 != B1 and B2 == C1) exactly when the second
//	                baseline read agrees with the candidate on the whole
//	                case; stands otherwise.
//
// and the recorded leaves must be exactly the first comparison's outside
// findings -- a leaf a declaration admits is never recorded, never counted
// as a witness, and never changed by the skew path.

var skewSymbols = []string{"1", "2", "3", "null", "missing"}

// skewLeaf is one generated leaf: where it lives in a body, how to set it,
// and how to recognise its findings.
type skewLeaf struct {
	name    string
	set     func(body any, symbol string)
	matches func(Finding) bool
	path    string // the declaration path covering it
}

func skewValue(symbol string) any {
	if spelled, ok := strings.CutPrefix(symbol, "spelled:"); ok {
		return json.Number(spelled)
	}
	if symbol == "null" {
		return nil
	}
	return json.Number(symbol)
}

// secondSpelling spells B2's numbers differently from B1 and C1, so an
// equality that compares spellings instead of numbers cannot pass.
var secondSpelling = map[string]string{"1": "1.0", "2": "2e0", "3": "3.00"}

type skewCase struct {
	symbols [][3]string // per leaf: B1, C1, B2
}

// skewOracle derives the verdict from the symbols and the first and
// second comparisons, independently of ClassifyWriteSkew.
func skewOracle(first Result, leaves []skewLeaf, c skewCase, second func() Result) (WriteSkewVerdict, string) {
	if len(first.outsideFindings) == 0 {
		return WriteSkewNotApplicable, ""
	}
	for _, i := range first.outsideFindings {
		if first.Findings[i].Shape != ShapeValue {
			return WriteSkewNotApplicable, ""
		}
	}
	outsideLeaf := map[int]bool{}
	for _, i := range first.outsideFindings {
		matched := -1
		for l, leaf := range leaves {
			if leaf.matches(first.Findings[i]) {
				matched = l
			}
		}
		if matched < 0 {
			return "", fmt.Sprintf("outside finding %s %s matches no generated leaf", first.Findings[i].Path, first.Findings[i].Detail)
		}
		outsideLeaf[matched] = true
	}
	r := second()
	if r.StructuralRefusal != "" {
		return WriteSkewRefused, RESTRefusalRereadStructural
	}
	stands := false
	for l := range leaves {
		if !outsideLeaf[l] {
			continue
		}
		b1, c1, b2 := c.symbols[l][0], c.symbols[l][1], c.symbols[l][2]
		switch {
		case b2 == b1:
			stands = true
		case b2 == c1:
		default:
			return WriteSkewRefused, leaves[l].name
		}
	}
	if stands {
		return WriteSkewStands, ""
	}
	if r.DifferencesOutsideBaselineDefect != 0 {
		return WriteSkewStands, ""
	}
	if len(r.Acceptance()) > 0 {
		return WriteSkewRefused, RESTRefusalRereadDeclarationsInvalid
	}
	return WriteSkewAdmitted, ""
}

// runSkewGenerator enumerates every case for leaves over base under the
// given declaration choices and checks the invariant. It returns the
// number of cases per verdict.
func runSkewGenerator(t *testing.T, base func() any, leaves []skewLeaf, declarations []Options) map[WriteSkewVerdict]int {
	t.Helper()
	counts := map[WriteSkewVerdict]int{}
	combos := 1
	for range leaves {
		combos *= 125
	}
	// One body per read position is built once and its generated leaves are
	// overwritten for every case; every case sets every leaf in every body.
	bodies := [3]any{base(), base(), base()}
	for _, opts := range declarations {
		for n := 0; n < combos; n++ {
			c := skewCase{symbols: make([][3]string, len(leaves))}
			rest := n
			for l := range leaves {
				k := rest % 125
				rest /= 125
				c.symbols[l] = [3]string{skewSymbols[k%5], skewSymbols[(k/5)%5], skewSymbols[k/25]}
			}
			build := func(pos int) Snapshot {
				body := bodies[pos]
				for l, leaf := range leaves {
					symbol := c.symbols[l][pos]
					if spelled, ok := secondSpelling[symbol]; ok && pos == 2 {
						leaf.set(body, "spelled:"+spelled)
						continue
					}
					leaf.set(body, symbol)
				}
				return Snapshot{Data: body, DataPresent: true}
			}
			b1, c1, b2 := build(0), build(1), build(2)
			first := Compare(b1, c1, opts)
			if len(first.outsideFindings) != first.DifferencesOutsideBaselineDefect {
				t.Fatalf("case %v: %d findings recorded outside, the comparison counts %d", c.symbols, len(first.outsideFindings), first.DifferencesOutsideBaselineDefect)
			}
			want, oracleErr := skewOracle(first, leaves, c, func() Result { return Compare(b2, c1, opts) })
			if oracleErr != "" && want == "" {
				t.Fatalf("case %v: %s", c.symbols, oracleErr)
			}
			got, recorded, detail := classifyForTest(first, b1, c1, b2, opts)
			counts[got]++
			if got != want {
				t.Fatalf("case %v: verdict %s, want %s (%s)", c.symbols, got, want, detail)
			}
			if want == WriteSkewRefused && oracleErr != RESTRefusalRereadStructural && oracleErr != RESTRefusalRereadDeclarationsInvalid && !strings.Contains(detail, recordedPathFor(first, leaves, oracleErr)) {
				t.Fatalf("case %v: refusal detail %q does not name leaf %s", c.symbols, detail, oracleErr)
			}
			if got == WriteSkewNotApplicable {
				continue
			}
			// Recorded leaves are exactly the outside findings -- never a
			// declared (admitted) leaf -- up to a refusal's early stop.
			outsidePaths := map[string]bool{}
			for _, i := range first.outsideFindings {
				outsidePaths[first.Findings[i].Path] = true
			}
			for _, leaf := range recorded {
				if !outsidePaths[leaf.Path] {
					t.Fatalf("case %v: recorded leaf %s is not an outside finding", c.symbols, leaf.Path)
				}
			}
			if got != WriteSkewRefused && len(recorded) != len(first.outsideFindings) {
				t.Fatalf("case %v: recorded %d leaves, first comparison has %d outside", c.symbols, len(recorded), len(first.outsideFindings))
			}
			if got == WriteSkewAdmitted {
				for _, leaf := range recorded {
					if !leaf.SecondBaselinePresent || leafValuesEqual(leaf.SecondBaseline, leaf.FirstBaseline) || !leafValuesEqual(leaf.SecondBaseline, leaf.Candidate) {
						t.Fatalf("case %v: admitted with an unwitnessed leaf %+v", c.symbols, leaf)
					}
				}
			}
		}
	}
	return counts
}

// recordedPathFor returns the path of the outside finding for the named
// generated leaf, for checking a refusal names it.
func recordedPathFor(first Result, leaves []skewLeaf, name string) string {
	for _, leaf := range leaves {
		if leaf.name != name {
			continue
		}
		for _, i := range first.outsideFindings {
			if leaf.matches(first.Findings[i]) {
				return first.Findings[i].Path
			}
		}
	}
	return "\x00no such leaf"
}

func formatSkewCounts(counts map[WriteSkewVerdict]int) string {
	var parts []string
	total := 0
	for v, n := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", v, n))
		total += n
	}
	sort.Strings(parts)
	return fmt.Sprintf("%d cases: %s", total, strings.Join(parts, " "))
}

// syntheticLeaf is a top-level field of a flat object body.
func syntheticLeaf(name string) skewLeaf {
	return skewLeaf{
		name: name,
		path: "data." + name,
		set: func(body any, symbol string) {
			object := body.(map[string]any)
			if symbol == "missing" {
				delete(object, name)
				return
			}
			object[name] = skewValue(symbol)
		},
		matches: func(f Finding) bool { return f.Path == "$.data."+name },
	}
}

// syntheticDeclarations returns, for the given leaves, every combination
// of per-leaf coverage: uncovered, a plain declaration, a direction shape,
// a contested direction shape.
func syntheticDeclarations(leaves []skewLeaf) []Options {
	var out []Options
	combos := 1
	for range leaves {
		combos *= 4
	}
	for n := 0; n < combos; n++ {
		var defects []BaselineDefect
		rest := n
		for _, leaf := range leaves {
			switch rest % 4 {
			case 1:
				defects = append(defects, BaselineDefect{Ticket: "ABC-123", Reason: "test fixture", Paths: []string{leaf.path}})
			case 2:
				defects = append(defects, BaselineDefect{Ticket: "ABC-124", Reason: "test fixture", Paths: []string{leaf.path},
					ScalarDirectionShape: &ScalarDirectionShape{Path: leaf.path, BaselineMustBeGreater: true}})
			case 3:
				defects = append(defects, BaselineDefect{Ticket: "ABC-125", Reason: "test fixture", Paths: []string{leaf.path},
					ScalarDirectionShape: &ScalarDirectionShape{Path: leaf.path, BaselineMustBeGreater: true, ContestedPaths: []string{leaf.path}}})
			}
			rest /= 4
		}
		out = append(out, Options{BaselineDefects: defects})
	}
	return out
}

// TestWriteSkewInvariant_Synthetic enumerates one and two leaves of a flat
// body under every per-leaf declaration coverage.
func TestWriteSkewInvariant_Synthetic(t *testing.T) {
	t.Parallel()
	base := func() any { return map[string]any{"other": json.Number("7")} }
	for _, leaves := range [][]skewLeaf{
		{syntheticLeaf("l0")},
		{syntheticLeaf("l0"), syntheticLeaf("l1")},
	} {
		t.Run(fmt.Sprintf("%d leaves", len(leaves)), func(t *testing.T) {
			counts := runSkewGenerator(t, base, leaves, syntheticDeclarations(leaves))
			t.Logf("synthetic, %d leaves, every declaration coverage: %s", len(leaves), formatSkewCounts(counts))
			if counts[WriteSkewAdmitted] == 0 || counts[WriteSkewStands] == 0 || counts[WriteSkewRefused] == 0 || counts[WriteSkewNotApplicable] == 0 {
				t.Fatalf("the domain must reach every verdict: %v", counts)
			}
		})
	}
}

// TestWriteSkewInvariant_RealSunburstDeclarations runs the generator on the
// captured team-scoped sunburst body under that case's real declarations,
// varying the value of one and two keyed elements.
func TestWriteSkewInvariant_RealSunburstDeclarations(t *testing.T) {
	t.Parallel()
	opts := investmentSunburstTeamScopedParityWithLimit(investmentSunburstDefaultLimit)
	raw, err := os.ReadFile("testdata/investmentsunburst_teamscoped_skew_baseline_3cf72260.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	decl, _ := findOrderInsensitiveList(opts, "data")
	var rows []any
	decodeNumbers(t, raw, &rows)
	// A small slice of the real body keeps the enumeration fast; every
	// element keeps its real shape and keys.
	rows = rows[:8]
	keyOf := func(element any) string {
		k, _ := orderInsensitiveKey(element, decl.KeyFields)
		return k
	}
	leafFor := func(index int) skewLeaf {
		key := keyOf(rows[index])
		return skewLeaf{
			name: key,
			path: "data.value",
			set: func(body any, symbol string) {
				for _, element := range body.([]any) {
					if keyOf(element) == key {
						object := element.(map[string]any)
						if symbol == "missing" {
							delete(object, "value")
							return
						}
						object["value"] = skewValue(symbol)
					}
				}
			},
			matches: func(f Finding) bool {
				keys := findingKeys(f.Detail)
				return len(keys) == 1 && keys[0] == key
			},
		}
	}
	base := func() any {
		var copyRows []any
		decodeNumbers(t, mustMarshal(t, rows), &copyRows)
		return copyRows
	}
	for _, leaves := range [][]skewLeaf{{leafFor(0)}, {leafFor(0), leafFor(1)}} {
		t.Run(fmt.Sprintf("%d leaves", len(leaves)), func(t *testing.T) {
			counts := runSkewGenerator(t, base, leaves, []Options{opts})
			t.Logf("real sunburst team-scoped declarations, %d leaves: %s", len(leaves), formatSkewCounts(counts))
		})
	}
}

// TestWriteSkewInvariant_RealInvestmentDeclarations runs the generator on
// the captured GET /api/v1/investment body under that route's real
// declarations, varying one and two theme_distribution entries.
func TestWriteSkewInvariant_RealInvestmentDeclarations(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/investment_default_window_baseline_852da907.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var body map[string]any
	decodeNumbers(t, raw, &body)
	leafFor := func(theme string) skewLeaf {
		return skewLeaf{
			name: theme,
			path: "data.theme_distribution",
			set: func(b any, symbol string) {
				dist := b.(map[string]any)["theme_distribution"].(map[string]any)
				if symbol == "missing" {
					delete(dist, theme)
					return
				}
				dist[theme] = skewValue(symbol)
			},
			matches: func(f Finding) bool { return f.Path == "$.data.theme_distribution."+theme },
		}
	}
	base := func() any {
		var copyBody map[string]any
		decodeNumbers(t, mustMarshal(t, body), &copyBody)
		return copyBody
	}
	for _, leaves := range [][]skewLeaf{{leafFor("quality")}, {leafFor("quality"), leafFor("risk")}} {
		t.Run(fmt.Sprintf("%d leaves", len(leaves)), func(t *testing.T) {
			counts := runSkewGenerator(t, base, leaves, []Options{investmentParity})
			t.Logf("real investment declarations, %d leaves: %s", len(leaves), formatSkewCounts(counts))
		})
	}
}

func decodeNumbers(t *testing.T, raw []byte, into any) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(into); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
