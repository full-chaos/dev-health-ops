package goapiproof

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Invariant these tests enforce: every scope branch of POST
// /api/v1/investment/flow/repo-team has a comparing case, and the
// comparison pairs each candidate link with exactly one baseline link
// identity -- (source, target), after every baseline copy of that pair is
// consumed once into one element -- and a body whose links are not one
// element per identity under that rule is refused by name, never paired
// by position.

const (
	repoTeamDefaultOrgBaselinePath    = "testdata/investmentflow_repoteam_default_org_baseline_d7461edc.json"
	repoTeamDefaultOrgCandidatePath   = "testdata/investmentflow_repoteam_default_org_candidate_249a5bf0.json"
	repoTeamDefaultOrgPerRowPath      = "testdata/investmentflow_repoteam_default_org_perrowcandidate_7231b988.json"
	repoTeamRepoScopedBaselinePath    = "testdata/investmentflow_repoteam_repo_scoped_baseline_55041245.json"
	repoTeamRepoScopedCandidatePath   = "testdata/investmentflow_repoteam_repo_scoped_candidate_b96cb073.json"
	repoTeamRouteOperation            = "REST:POST:/api/v1/investment/flow/repo-team"
	repoTeamCandidateDuplicateRefusal = "the candidate side has two elements"
)

// repoTeamLinkCopiesTicket is read from the declaration, never retyped.
var repoTeamLinkCopiesTicket = investmentFlowRepoTeamLinkCopiesDefect.Ticket

// repoTeamScopeBranchNames are POST /api/v1/investment/flow/repo-team's
// four scope branches. POST /api/v1/investment/flow/repo-team is a
// deleted-Python-body route (CHAOS-6241, restdeletedbody.go): every
// Request's own WantBaselineStatus/BodyMode no longer distinguishes them
// (all four are overridden to the same fixed-sentinel shape), so the tests
// below select branches by NAME -- their own Parity field, what these
// tests actually exercise against captured pre-deletion fixture pairs, is
// left untouched by that override.
var repoTeamScopeBranchNames = map[string]bool{"default_org": true, "theme_scoped_org": true, "team_scoped": true, "repo_scoped": true}

// The *_baseline_* fixtures are reference-plane bodies captured from a
// production deployed-vs-deployed prove run (each file name carries the
// first 8 hex digits of its own sha256). *_perrowcandidate_* is the
// candidate body captured in the same run, when the candidate still
// appended one link per row. *_candidate_* files are that captured
// candidate body with its links summed per (source, target) in list order
// (first appearance kept) and nodes unchanged -- what buildRepoTeamSankey
// writes for the same rows (investmentflow's
// TestRepoTeamSankeyLinkIdentityByEnumeration proves that equivalence).

func restSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

func restSnapshotFromValue(t *testing.T, value any) Snapshot {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return snapshot
}

// repeatedLinkKeys counts the (source, target) pairs that appear more than
// once in a decoded body's links.
func repeatedLinkKeys(t *testing.T, snapshot Snapshot) int {
	t.Helper()
	links, ok := listAtDottedPath(snapshot.Data, "data.links")
	if !ok {
		t.Fatal("body carries no data.links list")
	}
	counts := map[string]int{}
	for _, link := range links {
		key, ok := orderInsensitiveKey(link, []string{"source", "target"})
		if !ok {
			t.Fatalf("link %v carries no source/target", link)
		}
		counts[key] = counts[key] + 1
	}
	if len(counts) == 0 {
		return 0
	}
	repeated := 0
	for _, n := range counts {
		if n > 1 {
			repeated++
		}
	}
	return repeated
}

// TestRepoTeamEveryScopeBranchComparesCapturedBodies drives every 200
// request of the route's own corpus entry, through its own declared
// Parity, over the captured bodies: none is refused, and the only
// differences are the baseline's repeated links, each covered by the
// link-copies defect.
func TestRepoTeamEveryScopeBranchComparesCapturedBodies(t *testing.T) {
	spec, ok := restEndpointSpecs[repoTeamRouteOperation]
	if !ok {
		t.Fatalf("corpus carries no %s entry", repoTeamRouteOperation)
	}
	fixtures := map[string][2]string{
		"default_org":      {repoTeamDefaultOrgBaselinePath, repoTeamDefaultOrgCandidatePath},
		"theme_scoped_org": {repoTeamDefaultOrgBaselinePath, repoTeamDefaultOrgCandidatePath},
		"team_scoped":      {repoTeamDefaultOrgBaselinePath, repoTeamDefaultOrgCandidatePath},
		"repo_scoped":      {repoTeamRepoScopedBaselinePath, repoTeamRepoScopedCandidatePath},
	}
	var branches []string
	for _, request := range spec.Requests {
		if !repoTeamScopeBranchNames[request.Name] {
			continue
		}
		branches = append(branches, request.Name)
		pair, ok := fixtures[request.Name]
		if !ok {
			t.Fatalf("scope branch %q has no captured fixture pair in this test -- add one so every branch keeps a comparing case", request.Name)
		}
		baseline := restSnapshotFromFile(t, pair[0])
		candidate := restSnapshotFromFile(t, pair[1])
		wantCopies := repeatedLinkKeys(t, baseline)
		if wantCopies == 0 {
			t.Fatalf("%s: fixture %s repeats no link; it must exercise the copies", request.Name, pair[0])
		}
		if n := repeatedLinkKeys(t, candidate); n != 0 {
			t.Fatalf("%s: candidate fixture repeats %d links", request.Name, n)
		}
		result := Compare(baseline, candidate, request.Parity)
		if result.StructuralRefusal != "" || len(result.OrderInsensitiveListRefusals) > 0 {
			t.Fatalf("%s: refused: structural=%q lists=%v", request.Name, result.StructuralRefusal, result.OrderInsensitiveListRefusals)
		}
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("%s: terminal state %q, want mismatch (the baseline copies are a declared divergence, never a match)", request.Name, result.TerminalState)
		}
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("%s: outside=%d, findings %+v", request.Name, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
		if got := result.CoveredByShape[ShapeBaselineCopies]; got != wantCopies {
			t.Fatalf("%s: covered baseline_copies = %d, want %d (one per repeated baseline pair)", request.Name, got, wantCopies)
		}
		if !slices.Contains(result.BaselineDefectsMatched, repoTeamLinkCopiesTicket) {
			t.Fatalf("%s: matched %v, want %s", request.Name, result.BaselineDefectsMatched, repoTeamLinkCopiesTicket)
		}
		if len(result.StaleBaselineDefects) > 0 || len(result.LiveBaselineDefectsUnexplained) > 0 || len(result.UnusedOrderInsensitiveLists) > 0 {
			t.Fatalf("%s: stale=%v live-unexplained=%v unused-lists=%v", request.Name, result.StaleBaselineDefects, result.LiveBaselineDefectsUnexplained, result.UnusedOrderInsensitiveLists)
		}
	}
	sort.Strings(branches)
	if want := []string{"default_org", "repo_scoped", "team_scoped", "theme_scoped_org"}; !reflect.DeepEqual(branches, want) {
		t.Fatalf("scope branches = %v, want %v", branches, want)
	}
}

// TestRepoTeamCandidateRepeatedLinksStayRefused: the collapse is
// baseline-only, so a candidate that repeats a link (the per-row shape)
// is refused by name.
func TestRepoTeamCandidateRepeatedLinksStayRefused(t *testing.T) {
	result := Compare(restSnapshotFromFile(t, repoTeamDefaultOrgBaselinePath), restSnapshotFromFile(t, repoTeamDefaultOrgPerRowPath), investmentFlowRepoTeamParity)
	if len(result.OrderInsensitiveListRefusals) == 0 || !strings.Contains(strings.Join(result.OrderInsensitiveListRefusals, "\n"), repoTeamCandidateDuplicateRefusal) {
		t.Fatalf("refusals = %v, want the candidate-side duplicate refusal", result.OrderInsensitiveListRefusals)
	}
}

// TestRepoTeamOneSummedLinkOffRefusesOnlyThatKey: moving one summed
// candidate link beyond the float tier leaves that key's copies outside
// the citation, and every other repeated pair still covered.
func TestRepoTeamOneSummedLinkOffRefusesOnlyThatKey(t *testing.T) {
	baseline := restSnapshotFromFile(t, repoTeamRepoScopedBaselinePath)
	candidate := restSnapshotFromFile(t, repoTeamRepoScopedCandidatePath)
	links, _ := listAtDottedPath(candidate.Data, "data.links")
	moved := false
	for _, link := range links {
		object := link.(map[string]any)
		if object["target"] == "Fullchaos" {
			value, _ := asFloat(object["value"])
			object["value"] = json.Number(fmt.Sprint(value + 1))
			moved = true
		}
	}
	if !moved {
		t.Fatal("fixture carries no repo->team link to move")
	}
	result := Compare(baseline, candidate, investmentFlowRepoTeamParity)
	if result.OutsideByShape[ShapeBaselineCopies] != 1 || result.OutsideByShape[ShapeValue] != 1 {
		t.Fatalf("outside by shape = %v, want baseline_copies=1 value=1", result.OutsideByShape)
	}
}

// copyState is one key's baseline side in the enumeration below.
type copyState struct {
	name   string
	copies int
	// variant: "eq" copies agree outside value, "diff" one copy carries an
	// extra field, "nonnum" one copy carries a non-numeric value.
	variant string
}

var enumBaselineStates = []copyState{
	{"absent", 0, "eq"}, {"one", 1, "eq"}, {"two_eq", 2, "eq"}, {"two_diff", 2, "diff"},
	{"two_nonnum", 2, "nonnum"}, {"three_eq", 3, "eq"}, {"three_diff", 3, "diff"},
}

var enumCandidateStates = []string{"absent", "sum", "sum_plus_one", "sum_last_bits", "repeated"}

// TestBaselineCopySumByEnumeration enumerates two link keys, each over
// every baseline state x every candidate state (35 x 35 = 1225 bodies),
// through the route's production declaration of data.links and the
// link-copies defect alone, against an independent oracle.
func TestBaselineCopySumByEnumeration(t *testing.T) {
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{investmentFlowRepoTeamParity.OrderInsensitiveLists[1]},
		BaselineDefects:       []BaselineDefect{investmentFlowRepoTeamLinkCopiesDefect},
		FloatTierB:            map[string]string{"data.links.value": investmentFlowRepoTeamParity.FloatTierB["data.links.value"]},
	}
	if opts.OrderInsensitiveLists[0].Path != "data.links" {
		t.Fatalf("production declaration [1] = %+v, want data.links", opts.OrderInsensitiveLists[0])
	}
	keys := [][2]string{{"Sub A", "repo-a"}, {"repo-a", "TeamA"}}
	cells := 0
	for _, b0 := range enumBaselineStates {
		for _, c0 := range enumCandidateStates {
			for _, b1 := range enumBaselineStates {
				for _, c1 := range enumCandidateStates {
					bs := []copyState{b0, b1}
					cs := []string{c0, c1}
					baseLinks, candLinks := enumBodies(keys, bs, cs)
					node := []any{map[string]any{"name": "repo-a", "group": "repo", "value": 1}}
					result := Compare(
						restSnapshotFromValue(t, map[string]any{"nodes": node, "links": baseLinks}),
						restSnapshotFromValue(t, map[string]any{"nodes": node, "links": candLinks}),
						opts)
					cell := fmt.Sprintf("baseline=%s/%s candidate=%s/%s", b0.name, b1.name, c0, c1)
					checkEnumOracle(t, cell, bs, cs, result)
					cells++
				}
			}
		}
	}
	if want := len(enumBaselineStates) * len(enumCandidateStates) * len(enumBaselineStates) * len(enumCandidateStates); cells != want {
		t.Fatalf("cells = %d, want %d", cells, want)
	}
}

func enumBodies(keys [][2]string, bs []copyState, cs []string) (baseLinks, candLinks []any) {
	sums := make([]float64, len(keys))
	// Copies of the two keys interleave, so grouping never relies on
	// adjacency.
	for copy := 0; copy < 3; copy++ {
		for k, key := range keys {
			if copy >= bs[k].copies {
				continue
			}
			value := float64(copy+1)*1.1 + float64(k)*0.37
			link := map[string]any{"source": key[0], "target": key[1], "value": value}
			if copy == 1 && bs[k].variant == "diff" {
				link["extra"] = 1
			}
			if copy == 1 && bs[k].variant == "nonnum" {
				link["value"] = "n/a"
			}
			sums[k] = sums[k] + value
			baseLinks = append(baseLinks, link)
		}
	}
	for k, key := range keys {
		value := sums[k]
		if bs[k].copies == 0 {
			value = 1
		}
		switch cs[k] {
		case "absent":
			continue
		case "sum_plus_one":
			value = value + 1
		case "sum_last_bits":
			value = value * (1 + 1e-13)
		case "repeated":
			candLinks = append(candLinks, map[string]any{"source": key[0], "target": key[1], "value": value / 2})
			value = value / 2
		}
		candLinks = append(candLinks, map[string]any{"source": key[0], "target": key[1], "value": value})
	}
	if baseLinks == nil {
		baseLinks = []any{}
	}
	if candLinks == nil {
		candLinks = []any{}
	}
	return baseLinks, candLinks
}

func checkEnumOracle(t *testing.T, cell string, bs []copyState, cs []string, result Result) {
	t.Helper()
	refused := false
	for k := range bs {
		if cs[k] == "repeated" || (bs[k].copies > 1 && bs[k].variant != "eq") {
			refused = true
		}
	}
	gotRefused := len(result.OrderInsensitiveListRefusals) > 0
	if gotRefused != refused {
		t.Fatalf("%s: refused=%v (%v), want %v", cell, gotRefused, result.OrderInsensitiveListRefusals, refused)
	}
	if refused {
		if len(result.Findings) != 0 {
			t.Fatalf("%s: a refused list still produced findings %+v", cell, result.Findings)
		}
		return
	}
	wantCovered, wantOutside := 0, 0
	for k := range bs {
		copies, cand := bs[k].copies, cs[k]
		clean := cand == "sum" || cand == "sum_last_bits"
		if copies > 1 {
			if clean {
				wantCovered++
			} else {
				wantOutside++
			}
		}
		if (copies == 0) != (cand == "absent") {
			wantOutside++ // a key on one side only
		}
		if copies > 0 && cand == "sum_plus_one" {
			wantOutside++ // the summed value outside the tier
		}
	}
	if got := result.CoveredByShape[ShapeBaselineCopies]; got != wantCovered {
		t.Fatalf("%s: covered baseline_copies = %d, want %d; findings %+v", cell, got, wantCovered, result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != wantOutside {
		t.Fatalf("%s: outside = %d, want %d; findings %+v", cell, result.DifferencesOutsideBaselineDefect, wantOutside, result.Findings)
	}
	wantMatched := wantCovered > 0
	if got := slices.Contains(result.BaselineDefectsMatched, repoTeamLinkCopiesTicket); got != wantMatched {
		t.Fatalf("%s: matched %v, want copies defect matched=%v", cell, result.BaselineDefectsMatched, wantMatched)
	}
	if wantCovered+wantOutside == 0 && result.TerminalState != TerminalStateMatch {
		t.Fatalf("%s: terminal %q with no difference", cell, result.TerminalState)
	}
	if wantCovered+wantOutside > 0 && result.TerminalState != TerminalStateMismatch {
		t.Fatalf("%s: terminal %q, want mismatch", cell, result.TerminalState)
	}
}

// TestBaselineCopySumAppliesOnlyToItsOwnDeclaration: a shape whose key
// set differs from the list's declaration never collapses anything, so
// repeated baseline keys stay refused by name.
func TestBaselineCopySumAppliesOnlyToItsOwnDeclaration(t *testing.T) {
	defect := investmentFlowRepoTeamLinkCopiesDefect
	shape := *defect.BaselineCopySumShape
	shape.KeyFields = []string{"target", "source"}
	defect.BaselineCopySumShape = &shape
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{investmentFlowRepoTeamParity.OrderInsensitiveLists[1]},
		BaselineDefects:       []BaselineDefect{defect},
	}
	link := map[string]any{"source": "a", "target": "b", "value": 1}
	result := Compare(
		restSnapshotFromValue(t, map[string]any{"links": []any{link, link}}),
		restSnapshotFromValue(t, map[string]any{"links": []any{map[string]any{"source": "a", "target": "b", "value": 2}}}),
		opts)
	if !strings.Contains(strings.Join(result.OrderInsensitiveListRefusals, "\n"), "the baseline side has two elements") {
		t.Fatalf("refusals = %v, want the baseline-side duplicate refusal", result.OrderInsensitiveListRefusals)
	}
}

// orderInsensitiveKeyIdentity is the sweep of every order-insensitive list
// declaration the REST corpus carries, one verdict each on whether its
// KeyFields are the full identity of one element on BOTH planes. Every
// entry except the repo-team links is unique on both planes by the
// planes' own construction; the captured production bodies of one full
// prove run repeat a key in no list other than repo-team's data.links.
var orderInsensitiveKeyIdentity = map[string]string{
	"data|theme,subcategory,scope":                     "full identity: both planes GROUP BY theme, subcategory, scope",
	"data|work_unit_id":                                "full identity: one work unit per work_unit_id on both planes",
	"data.cells|x,y":                                   "full identity: (x, y) is every heatmap reader's own GROUP BY bucket key",
	"data.contributors|id":                             "full identity: one contributor row per id",
	"data.drivers|id":                                  "full identity: one driver row per id",
	"data.items|work_item_id":                          "full identity: one issue row per work_item_id",
	"data.links|source,target":                         "full identity on the candidate for every route (sankey edgeAccumulator, investment-flow link_totals/orderedEdges, dynamic-mode SQL GROUP BY source, target); the reference plane repeats it on investment/flow/repo-team only, collapsed whole-key by that route's own BaselineCopySumShape",
	"data.nodes|name":                                  "full identity: every sankey/flow builder touches a node name once",
	"data.points|entity_id":                            "full identity: one quadrant point per entity",
	"data.sections.collaboration.handoff_points|label": "full identity: one UNION ALL branch per label",
	"data.sections.collaboration.review_load|label":    "full identity: one UNION ALL branch per label",
}

// TestOrderInsensitiveKeyIdentitySweepIsComplete pins the sweep to the
// corpus: a new order-insensitive declaration fails here until it carries
// a verdict, and only the repo-team route declares a baseline copy sum.
func TestOrderInsensitiveKeyIdentitySweepIsComplete(t *testing.T) {
	declared := map[string]bool{}
	for operation, spec := range restEndpointSpecs {
		for _, request := range spec.Requests {
			for _, list := range request.Parity.OrderInsensitiveLists {
				declared[list.Path+"|"+strings.Join(list.KeyFields, ",")] = true
			}
			for _, defect := range request.Parity.BaselineDefects {
				if defect.BaselineCopySumShape != nil && operation != repoTeamRouteOperation {
					t.Fatalf("%s/%s declares a baseline copy sum; only %s repeats a link identity", operation, request.Name, repoTeamRouteOperation)
				}
			}
		}
	}
	var missing, stale []string
	if len(declared) == 0 {
		t.Fatal("the REST corpus declares no order-insensitive list")
	}
	for key := range declared {
		if _, ok := orderInsensitiveKeyIdentity[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(orderInsensitiveKeyIdentity) == 0 {
		t.Fatal("the sweep table is empty")
	}
	for key := range orderInsensitiveKeyIdentity {
		if !declared[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 || len(stale) > 0 {
		t.Fatalf("order-insensitive key sweep out of date: missing verdicts %v, verdicts for no declaration %v", missing, stale)
	}
	spec := restEndpointSpecs[repoTeamRouteOperation]
	for _, request := range spec.Requests {
		if request.WantBaselineStatus != 200 {
			continue
		}
		found := false
		for _, defect := range request.Parity.BaselineDefects {
			if defect.BaselineCopySumShape != nil && defect.Ticket == repoTeamLinkCopiesTicket {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s/%s carries no link-copies defect", repoTeamRouteOperation, request.Name)
		}
	}
}

// TestCollapseBaselineCopiesDomain runs every input class of one key's
// copies through collapseBaselineCopies in one pass.
func TestCollapseBaselineCopiesDomain(t *testing.T) {
	link := func(value any, extra ...any) map[string]any {
		m := map[string]any{"source": "a", "target": "b", "value": value}
		for i := 0; i+1 < len(extra); i += 2 {
			m[extra[i].(string)] = extra[i+1]
		}
		return m
	}
	tenth, fifth, threeTenths := 0.1, 0.2, 0.3
	cases := []struct {
		name   string
		copies []map[string]any
		ok     bool
		want   any
	}{
		{"json numbers sum to a json number", []map[string]any{link(json.Number("1.5")), link(json.Number("2"))}, true, json.Number("3.5")},
		{"go floats sum to a go float", []map[string]any{link(1.5), link(2.0)}, true, 3.5},
		{"mixed number forms sum to a json number", []map[string]any{link(1.5), link(json.Number("2"))}, true, json.Number("3.5")},
		{"three copies, list order", []map[string]any{link(tenth), link(fifth), link(threeTenths)}, true, tenth + fifth + threeTenths},
		{"zero and negative shares still sum", []map[string]any{link(0.0), link(-1.0)}, true, -1.0},
		{"value absent", []map[string]any{link(1.0), {"source": "a", "target": "b"}}, false, nil},
		{"value null", []map[string]any{link(1.0), link(nil)}, false, nil},
		{"value a string", []map[string]any{link(1.0), link("1")}, false, nil},
		{"value malformed number", []map[string]any{link(1.0), link(json.Number("1x"))}, false, nil},
		{"value NaN", []map[string]any{link(1.0), link(math.NaN())}, false, nil},
		{"value infinite", []map[string]any{link(math.Inf(1)), link(1.0)}, false, nil},
		{"opposite infinite shares", []map[string]any{link(math.Inf(1)), link(math.Inf(-1))}, false, nil},
		{"value overflows", []map[string]any{link(json.Number("1e400")), link(1.0)}, false, nil},
		{"sum overflows", []map[string]any{link(1.7e308), link(1.7e308)}, false, nil},
		{"extra field on one copy", []map[string]any{link(1.0), link(1.0, "extra", 1)}, false, nil},
		{"field differs outside value", []map[string]any{link(1.0, "kind", "x"), link(1.0, "kind", "y")}, false, nil},
		{"field equal by value outside value", []map[string]any{link(1.0, "rank", json.Number("1")), link(1.0, "rank", json.Number("1.0"))}, true, 2.0},
		{"field a container, equal", []map[string]any{link(1.0, "tags", []any{"x"}), link(1.0, "tags", []any{"x"})}, true, 2.0},
	}
	for _, tc := range cases {
		got, ok, reason := collapseBaselineCopies(tc.copies, "value")
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v (%s), want %v", tc.name, ok, reason, tc.ok)
		}
		if !ok {
			if reason == "" || got != nil {
				t.Fatalf("%s: a refused collapse must name its reason and return nothing", tc.name)
			}
			continue
		}
		if !reflect.DeepEqual(got["value"], tc.want) {
			t.Fatalf("%s: value %#v, want %#v", tc.name, got["value"], tc.want)
		}
		if got["source"] != "a" || got["target"] != "b" {
			t.Fatalf("%s: shared fields lost: %v", tc.name, got)
		}
		if reflect.ValueOf(got).Pointer() == reflect.ValueOf(tc.copies[0]).Pointer() || !reflect.DeepEqual(tc.copies[0]["value"], tc.copies[0]["value"]) {
			t.Fatalf("%s: the collapsed element must be a new map", tc.name)
		}
	}
}

// TestCollapseDeclaredBaselineCopiesScope covers the collapse's own scope
// guards and that the caller's decoded body is never mutated.
func TestCollapseDeclaredBaselineCopiesScope(t *testing.T) {
	decl := OrderInsensitiveList{Path: "data.links", KeyFields: []string{"source", "target"}, Ticket: "ABC-123"}
	shape := &BaselineCopySumShape{ListPath: "data.links", KeyFields: []string{"source", "target"}, SumField: "value"}
	opts := Options{OrderInsensitiveLists: []OrderInsensitiveList{decl}, BaselineDefects: []BaselineDefect{{Ticket: "ABC-124", BaselineCopySumShape: shape}}}
	body := func() map[string]any {
		return map[string]any{"links": []any{
			map[string]any{"source": "a", "target": "b", "value": 1.0},
			map[string]any{"source": "c", "target": "d", "value": 5.0},
			map[string]any{"source": "a", "target": "b", "value": 2.0},
		}}
	}

	original := body()
	got, lists, refusals := collapseDeclaredBaselineCopies(original, opts)
	if len(refusals) != 0 {
		t.Fatalf("refusals %v", refusals)
	}
	if !reflect.DeepEqual(original, body()) {
		t.Fatalf("the caller's body was mutated: %v", original)
	}
	want := []any{map[string]any{"source": "a", "target": "b", "value": 3.0}, map[string]any{"source": "c", "target": "d", "value": 5.0}}
	if !reflect.DeepEqual(got.(map[string]any)["links"], want) {
		t.Fatalf("collapsed %v, want %v (first appearance order)", got.(map[string]any)["links"], want)
	}
	if lists["data.links"].counts["a\x1fb"] != 2 || len(lists["data.links"].counts) != 1 || lists["data.links"].ticket != "ABC-124" {
		t.Fatalf("recorded %+v", lists)
	}

	unchanged := func(name string, opts Options, data any) {
		t.Helper()
		got, lists, refusals := collapseDeclaredBaselineCopies(data, opts)
		if !reflect.DeepEqual(got, data) || len(lists) != 0 || len(refusals) != 0 {
			t.Fatalf("%s: got %v lists %v refusals %v, want the body unchanged", name, got, lists, refusals)
		}
	}
	unchanged("no shape declared", Options{OrderInsensitiveLists: []OrderInsensitiveList{decl}}, body())
	unchanged("no order-insensitive declaration", Options{BaselineDefects: opts.BaselineDefects}, body())
	unchanged("list absent", opts, map[string]any{"nodes": []any{}})
	unchanged("list not a list", opts, map[string]any{"links": map[string]any{}})
	unchanged("no repeated key", opts, map[string]any{"links": []any{map[string]any{"source": "a", "target": "b", "value": 1.0}}})
	unchanged("element missing a key field", opts, map[string]any{"links": []any{
		map[string]any{"source": "a", "target": "b", "value": 1.0},
		map[string]any{"source": "a", "target": "b", "value": 1.0},
		map[string]any{"source": "a", "value": 1.0},
	}})
	rootShape := *shape
	rootShape.ListPath = "links"
	rootDecl := decl
	rootDecl.Path = "links"
	unchanged("list path not under data", Options{OrderInsensitiveLists: []OrderInsensitiveList{rootDecl}, BaselineDefects: []BaselineDefect{{Ticket: "ABC-124", BaselineCopySumShape: &rootShape}}},
		[]any{map[string]any{"source": "a", "target": "b", "value": 1.0}, map[string]any{"source": "a", "target": "b", "value": 1.0}})

	noKeys := *shape
	noKeys.KeyFields = nil
	unchanged("shape with no key fields and no declaration", Options{BaselineDefects: []BaselineDefect{{Ticket: "ABC-124", BaselineCopySumShape: &noKeys}}}, body())
	emptyDecl := decl
	emptyDecl.KeyFields = nil
	unchanged("shape and declaration with no key fields", Options{OrderInsensitiveLists: []OrderInsensitiveList{emptyDecl}, BaselineDefects: []BaselineDefect{{Ticket: "ABC-124", BaselineCopySumShape: &noKeys}}}, body())

	disagreeing := map[string]any{"links": []any{
		map[string]any{"source": "a", "target": "b", "value": 1.0},
		map[string]any{"source": "a", "target": "b", "value": 1.0, "extra": true},
	}}
	got, lists, refusals = collapseDeclaredBaselineCopies(disagreeing, opts)
	if len(refusals) != 1 || !strings.Contains(refusals[0], "ABC-124") || len(lists) != 0 || !reflect.DeepEqual(got, disagreeing) {
		t.Fatalf("disagreeing copies: refusals %v lists %v, want one named refusal and the list left as it is", refusals, lists)
	}
}

// TestBaselineCopiesOnlyCoveredWhenTheElementIsOtherwiseAccountedFor
// drives the admission's final pass: the copies of a key missing from the
// candidate stay outside, and so do copies whose summed value differs;
// a clean key's copies are covered beside them.
func TestBaselineCopiesOnlyCoveredWhenTheElementIsOtherwiseAccountedFor(t *testing.T) {
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{investmentFlowRepoTeamParity.OrderInsensitiveLists[1]},
		BaselineDefects:       []BaselineDefect{investmentFlowRepoTeamLinkCopiesDefect},
		FloatTierB:            map[string]string{"data.links.value": "test"},
	}
	l := func(s, tg string, v float64) map[string]any {
		return map[string]any{"source": s, "target": tg, "value": v}
	}
	baseline := map[string]any{"links": []any{l("a", "b", 1), l("a", "b", 2), l("c", "d", 1), l("c", "d", 1), l("e", "f", 4), l("e", "f", 4)}}
	candidate := map[string]any{"links": []any{l("a", "b", 3), l("e", "f", 9)}}
	result := Compare(restSnapshotFromValue(t, baseline), restSnapshotFromValue(t, candidate), opts)
	if result.CoveredByShape[ShapeBaselineCopies] != 1 || result.OutsideByShape[ShapeBaselineCopies] != 2 || result.OutsideByShape[ShapePresence] != 1 || result.OutsideByShape[ShapeValue] != 1 {
		t.Fatalf("covered %v outside %v, findings %+v", result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
}

// TestBaselineCopySumIdleWithoutCopies: through the route's production
// declaration, a body with no repeated link leaves the link-copies defect
// idle even when another declaration (the conserved-total
// redistribution) covers a link value difference beside it.
func TestBaselineCopySumIdleWithoutCopies(t *testing.T) {
	l := func(s, tg string, v float64) map[string]any {
		return map[string]any{"source": s, "target": tg, "value": v}
	}
	nodes := []any{map[string]any{"name": "Sub A", "group": "subcategory", "value": 4}, map[string]any{"name": "repo-a", "group": "repo", "value": 4}, map[string]any{"name": "repo-b", "group": "repo", "value": 4}}
	baseline := map[string]any{"nodes": nodes, "links": []any{l("Sub A", "repo-a", 1), l("Sub A", "repo-b", 3)}}
	candidate := map[string]any{"nodes": nodes, "links": []any{l("Sub A", "repo-a", 2), l("Sub A", "repo-b", 2)}}
	result := Compare(restSnapshotFromValue(t, baseline), restSnapshotFromValue(t, candidate), investmentFlowRepoTeamParity)
	if result.DifferencesOutsideBaselineDefect != 0 || result.CoveredByShape[ShapeValue] != 2 {
		t.Fatalf("precondition: the conserved redistribution must cover both values; covered %v outside %v", result.CoveredByShape, result.OutsideByShape)
	}
	if slices.Contains(result.BaselineDefectsMatched, repoTeamLinkCopiesTicket) || !slices.Contains(result.IdleIntermittentBaselineDefects, repoTeamLinkCopiesTicket) {
		t.Fatalf("matched %v idle %v, want %s idle", result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects, repoTeamLinkCopiesTicket)
	}
}

// TestRepoTeamScopeListDuplicateAxes executes every scope branch's own
// Parity x every order-insensitive list the body carries x {unique keys,
// repeated key with equal rows, repeated key with rows that differ outside
// the summed field}: only data.links collapses repeated keys, only when
// the copies agree outside value, and everything else repeated is refused
// by name.
func TestRepoTeamScopeListDuplicateAxes(t *testing.T) {
	spec := restEndpointSpecs[repoTeamRouteOperation]
	node := func(name string, extra bool) map[string]any {
		m := map[string]any{"name": name, "group": "repo", "value": 2}
		if extra {
			m["extra"] = true
		}
		return m
	}
	link := func(extra bool) map[string]any {
		m := map[string]any{"source": "Sub A", "target": "repo-a", "value": 1}
		if extra {
			m["extra"] = true
		}
		return m
	}
	type axis struct{ list, kind string }
	cells := 0
	for _, request := range spec.Requests {
		if !repoTeamScopeBranchNames[request.Name] {
			continue
		}
		for _, a := range []axis{{"data.nodes", "unique"}, {"data.nodes", "repeated_equal"}, {"data.nodes", "repeated_different"}, {"data.links", "unique"}, {"data.links", "repeated_equal"}, {"data.links", "repeated_different"}} {
			baseNodes := []any{node("Sub A", false), node("repo-a", false)}
			baseLinks := []any{link(false)}
			candLinks := []any{link(false)}
			switch {
			case a.list == "data.nodes" && a.kind == "repeated_equal":
				baseNodes = append(baseNodes, node("repo-a", false))
			case a.list == "data.nodes" && a.kind == "repeated_different":
				baseNodes = append(baseNodes, node("repo-a", true))
			case a.list == "data.links" && a.kind == "repeated_equal":
				baseLinks = append(baseLinks, link(false))
				candLinks = []any{map[string]any{"source": "Sub A", "target": "repo-a", "value": 2}}
			case a.list == "data.links" && a.kind == "repeated_different":
				baseLinks = append(baseLinks, link(true))
			}
			result := Compare(
				restSnapshotFromValue(t, map[string]any{"nodes": baseNodes, "links": baseLinks}),
				restSnapshotFromValue(t, map[string]any{"nodes": []any{node("Sub A", false), node("repo-a", false)}, "links": candLinks}),
				request.Parity)
			wantRefused := a.kind != "unique" && !(a.list == "data.links" && a.kind == "repeated_equal")
			if got := len(result.OrderInsensitiveListRefusals) > 0; got != wantRefused {
				t.Fatalf("%s %s %s: refused=%v (%v), want %v", request.Name, a.list, a.kind, got, result.OrderInsensitiveListRefusals, wantRefused)
			}
			if a.list == "data.links" && a.kind == "repeated_equal" && (result.CoveredByShape[ShapeBaselineCopies] != 1 || result.DifferencesOutsideBaselineDefect != 0) {
				t.Fatalf("%s: equal copies summed to the candidate: covered %v outside %d", request.Name, result.CoveredByShape, result.DifferencesOutsideBaselineDefect)
			}
			cells++
		}
	}
	if cells != 4*6 {
		t.Fatalf("cells = %d, want 24 (four scope branches x six axes)", cells)
	}
}
