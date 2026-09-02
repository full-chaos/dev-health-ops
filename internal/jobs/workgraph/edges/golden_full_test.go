package edges

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode"
)

// dependencyEdgeType is the ONE mapping from a dependency row's relationship_type
// to the edge kind the producer emits, shared by every test that needs it.
//
// It exists as a single function because adversarial review round 5 found a
// second, simplified copy in the falsification test that handled only the blocker
// family and `duplicates`: it bucketed `is_related_to` under `relates`, so it
// would have counted a VALID edge as rejected. Today's fixture has no such rows,
// so it passed while not validating the mapping it claimed to. A duplicated
// mapping that drifts from the real one is precisely what would mislead PR2.
//
// Mirrors DEPENDENCY_TYPE_MAP plus the blocker forcing in _canonical_dependency
// (src/dev_health_ops/work_graph/builder.py): the whole blocker family collapses
// to BLOCKS, every other known type maps directly, and anything unrecognised
// falls back to RELATES exactly as DEPENDENCY_TYPE_MAP.get(relationship, RELATES)
// does. Only the TYPE is derived here, never the direction, so the endpoint swap
// _canonical_dependency performs for blocker rows cannot affect a caller keying
// on an unordered pair.
//
// # WHY THIS MATCHES EXACTLY INSTEAD OF CASE-FOLDING
//
// The producer lowercases (`relationship_type.lower()`, builder.py:97) but Go and
// Python do not agree on what lowercasing means. Executed both ways on the
// Turkish dotted capital İ:
//
//	go     strings.ToLower("İS_BLOCKED_BY") = "is_blocked_by"   -> BLOCKS
//	python "İS_BLOCKED_BY".lower()          = "i̇s_blocked_by"   -> RELATES (falls through)
//
// Go drops the combining dot; Python keeps it. Folding here would therefore
// disagree with the producer on such a row. Folding with ASCII-only rules would
// disagree on a different one (U+212A KELVIN SIGN lowercases to "k" in Python).
// There is no folding that is right in general.
//
// Matching exactly is right whenever the stored values are already the plain
// lowercase tokens the producer's map is keyed on — and that is not assumed, it
// is asserted: TestFrozenRelationshipValuesNeedNoCaseFolding fails if the fixture
// ever carries a value for which folding would not be a no-op, so a regeneration
// that introduced one could not slip past.
func dependencyEdgeType(relationship string) string {
	switch relationship {
	case "blocks", "blocked_by", "is_blocked_by":
		return EdgeTypeBlocks
	case "relates", "relates_to":
		return EdgeTypeRelates
	case "is_related_to":
		return EdgeTypeIsRelatedTo
	case "duplicates":
		return EdgeTypeDuplicates
	case "is_duplicate_of":
		return EdgeTypeIsDuplicateOf
	case "parent", "is_parent_of":
		return EdgeTypeParentOf
	case "child", "is_child_of":
		return EdgeTypeChildOf
	default:
		return EdgeTypeRelates
	}
}

const goldenFixture = "workgraph_issue_edges_python_golden.json"

func repositoryRootPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
}

func loadGolden(t *testing.T) *GoldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", goldenFixture)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	document := &GoldenDocument{}
	if err := json.Unmarshal(raw, document); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if document.Schema != "workgraph_issue_edges_python_golden.v1" {
		t.Fatalf("unexpected golden schema %q", document.Schema)
	}
	if len(document.Edges) == 0 {
		t.Fatal("golden carries no edges")
	}
	return document
}

// TestGoldenCountsMatchItsOwnPayload catches a truncated or hand-edited fixture
// before any other test reasons about it.
func TestGoldenCountsMatchItsOwnPayload(t *testing.T) {
	document := loadGolden(t)
	existing := 0
	for _, page := range document.ExistingEdgeIDs {
		existing += len(page)
	}
	for name, got := range map[string]int{
		"dependencies":        len(document.Dependencies),
		"edges":               len(document.Edges),
		"projection_runs":     len(document.ProjectionRuns),
		"mutations":           len(document.Mutations),
		"existing_edge_pages": len(document.ExistingEdgeIDs),
		"existing_edge_ids":   existing,
	} {
		if want, ok := document.Counts[name]; !ok {
			t.Errorf("golden counts is missing %q", name)
		} else if got != want {
			t.Errorf("golden counts[%q] = %d, payload has %d", name, want, got)
		}
	}
}

// TestEdgeIDMatchesPythonForEveryFrozenEdge is the strongest assertion available
// before the derivation itself exists: it recomputes the id of all 3,548 real
// edges from their own endpoints and requires the Go hash to reproduce Python's
// exactly.
//
// The id is not cosmetic. It is the twin of the ReplacingMergeTree dedup identity
// AND the key the cleanup step deletes by, so a divergence does not fail loudly —
// it orphans the old row and mints a duplicate beside it.
func TestEdgeIDMatchesPythonForEveryFrozenEdge(t *testing.T) {
	document := loadGolden(t)
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		computed := EdgeID(row.SourceType, row.SourceID, row.EdgeType, row.TargetType, row.TargetID)
		if computed != row.EdgeID {
			t.Fatalf(
				"edge %d (%s:%s -%s-> %s:%s): Go edge_id %s, Python %s",
				index, row.SourceType, row.SourceID, row.EdgeType,
				row.TargetType, row.TargetID, computed, row.EdgeID,
			)
		}
	}
}

// TestFrozenConfidencesSurviveTheFloat32RoundTrip pins the quantisation contract
// against real values rather than a constructed one.
func TestFrozenConfidencesSurviveTheFloat32RoundTrip(t *testing.T) {
	document := loadGolden(t)
	for index, edge := range document.Edges {
		if !math.IsInf(edge.Confidence, 0) && !math.IsNaN(edge.Confidence) {
			narrowed := Quantize(edge.Confidence)
			if float64(narrowed) != float64(Quantize(float64(narrowed))) {
				t.Fatalf("edge %d: narrowing %v is not idempotent", index, edge.Confidence)
			}
			continue
		}
		t.Fatalf("edge %d carries a non-finite confidence %v", index, edge.Confidence)
	}
	// The value the variant-C policy writes must be a Float32 fixed point, or the
	// split's (confidence, edge_id) sort puts a freshly written edge in a
	// different tier from the identical edge read back.
	if got := Quantize(float64(AssociativeConfidence)); got != AssociativeConfidence {
		t.Fatalf("AssociativeConfidence %v is not a Float32 fixed point (got %v)", AssociativeConfidence, got)
	}
	if float64(AssociativeConfidence) == 0.9 {
		t.Fatal(
			"AssociativeConfidence compares equal to the float64 literal 0.9, which means " +
				"this test is not actually exercising the narrowing that changed the measured " +
				"component count from 1833 to 1832",
		)
	}
}

// TestBuildClockAndEventTimeAreDistinct pins the timestamp contract: the producer
// stamps discovered_at/last_synced from the BUILD's construction clock and
// event_ts from the ROW. Reversing them changes every ReplacingMergeTree version.
func TestBuildClockAndEventTimeAreDistinct(t *testing.T) {
	document := loadGolden(t)
	frozenNow, err := time.Parse(time.RFC3339Nano, document.FrozenNow)
	if err != nil {
		t.Fatalf("parse frozen_now: %v", err)
	}

	// Permitted instants, keyed by the edge's OWN endpoints AND its own kind.
	//
	// Two earlier versions of this were too loose and were caught by review.
	// Permitting any dependency's last_synced accepted a rotation across edges;
	// keying on endpoints alone still let a `blocks` edge take the timestamp of a
	// `relates` dependency between the same two issues.
	//
	// Only the TYPE is needed here, not the direction, which is why this does not
	// need _canonical_dependency (PR2's port, which may swap source and target for
	// blocker rows): the key is the unordered pair, so a swap cannot change it.
	type binding struct {
		low, high string
		edgeType  string
	}
	key := func(a, b, edgeType string) binding {
		if a > b {
			a, b = b, a
		}
		return binding{a, b, edgeType}
	}
	permitted := map[binding]map[int64]struct{}{}
	for index, dependency := range document.Dependencies {
		source, err := document.String(dependency[0])
		if err != nil {
			t.Fatalf("dependency %d source: %v", index, err)
		}
		target, err := document.String(dependency[1])
		if err != nil {
			t.Fatalf("dependency %d target: %v", index, err)
		}
		relationship, err := document.String(dependency[2])
		if err != nil {
			t.Fatalf("dependency %d relationship_type: %v", index, err)
		}
		lastSynced, err := document.Instant(dependency[5])
		if err != nil {
			t.Fatalf("dependency %d last_synced: %v", index, err)
		}
		bindingKey := key(source, target, dependencyEdgeType(relationship))
		if permitted[bindingKey] == nil {
			permitted[bindingKey] = map[int64]struct{}{}
		}
		permitted[bindingKey][lastSynced.UnixNano()] = struct{}{}
	}

	distinct := map[int64]struct{}{}
	boundExactly := 0
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		if !row.DiscoveredAt.Equal(frozenNow) {
			t.Fatalf("edge %d discovered_at %s is not the build clock %s", index, row.DiscoveredAt, frozenNow)
		}
		if !row.LastSynced.Equal(frozenNow) {
			t.Fatalf("edge %d last_synced %s is not the build clock %s", index, row.LastSynced, frozenNow)
		}
		// No blanket exemption for the build clock. The producer falls back to it
		// only for a row whose last_synced will not parse, and this fixture has
		// none -- so an edge carrying it is a defect, not a special case. The
		// previous version exempted them and a collapse of 3,547 of 3,548
		// timestamps to the build clock passed every predicate.
		if row.EventTs.Equal(frozenNow) {
			t.Fatalf(
				"edge %d carries the build clock as its event_ts; every dependency row in this "+
					"fixture has a parseable last_synced, so nothing should reach that fallback",
				index,
			)
		}
		allowed, known := permitted[key(row.SourceID, row.TargetID, row.EdgeType)]
		if !known {
			t.Fatalf(
				"edge %d (%s <-%s-> %s) has no dependency row with those endpoints and that kind; "+
					"it was not derived from this fixture's input",
				index, row.SourceID, row.EdgeType, row.TargetID,
			)
		}
		if _, ok := allowed[row.EventTs.UnixNano()]; !ok {
			t.Fatalf(
				"edge %d (%s <-%s-> %s) carries event_ts %s, which belongs to a DIFFERENT "+
					"dependency row — the timestamp is not bound to this edge's own source",
				index, row.SourceID, row.EdgeType, row.TargetID, row.EventTs,
			)
		}
		if len(allowed) == 1 {
			boundExactly++
		}
		distinct[row.EventTs.UnixNano()] = struct{}{}
		if want := DayFor(row.EventTs); !row.Day.Equal(want) {
			t.Fatalf("edge %d day %s != toDate(event_ts) %s", index, row.Day, want)
		}
	}

	if len(distinct) < 2 {
		t.Fatalf(
			"all %d edges carry the same event_ts; a producer that collapsed per-row "+
				"freshness to a constant would pass every other assertion here",
			len(document.Edges),
		)
	}
	if boundExactly*4 < len(document.Edges)*3 {
		t.Fatalf(
			"only %d of %d timestamps are pinned to a single permitted value; the binding "+
				"is too loose to detect a misassigned event_ts",
			boundExactly, len(document.Edges),
		)
	}
	t.Logf(
		"event_ts: %d distinct across %d edges; %d pinned to exactly one permitted instant",
		len(distinct), len(document.Edges), boundExactly,
	)
}

// TestVariantCExceptionListIsClosedAndNecessary asserts the shape of the ONE
// permitted divergence from Python: every listed kind is genuinely present in the
// frozen output at the delivery tier (so the exception is load-bearing, not
// decorative), and every associative kind the policy would change is listed (so
// the list cannot silently under-declare).
func TestVariantCExceptionListIsClosedAndNecessary(t *testing.T) {
	document := loadGolden(t)

	listed := map[string]GoldenException{}
	for _, exception := range AssociativeConfidenceExceptions {
		if _, duplicate := listed[exception.EdgeType]; duplicate {
			t.Fatalf("exception list names %q twice", exception.EdgeType)
		}
		if _, associative := AssociativeEdgeTypes[exception.EdgeType]; !associative {
			t.Fatalf("exception list names %q, which the policy does not change", exception.EdgeType)
		}
		if exception.ToGo != DependencyConfidence(exception.EdgeType) {
			t.Fatalf("exception for %q claims %v, policy returns %v",
				exception.EdgeType, exception.ToGo, DependencyConfidence(exception.EdgeType))
		}
		if !(exception.ToGo < exception.FromPy) {
			t.Fatalf("exception for %q does not lower the tier (%v -> %v)",
				exception.EdgeType, exception.FromPy, exception.ToGo)
		}
		listed[exception.EdgeType] = exception
	}
	for edgeType := range AssociativeEdgeTypes {
		if _, ok := listed[edgeType]; !ok {
			t.Fatalf("policy changes %q but the exception list does not declare it", edgeType)
		}
	}

	// Now the necessity half, against the frozen output.
	observed := map[string]int{}
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		if _, associative := AssociativeEdgeTypes[row.EdgeType]; !associative {
			continue
		}
		if row.Confidence != DeliveryConfidence {
			t.Fatalf(
				"edge %d (%s) sits at %v in the golden, not the delivery tier — the golden "+
					"was regenerated against a Python that already implements variant C, so the "+
					"exception list is stale",
				index, row.EdgeType, row.Confidence,
			)
		}
		observed[row.EdgeType]++
	}
	if len(observed) == 0 {
		t.Fatal("the golden contains no associative edges, so it cannot exercise the exception at all")
	}
	for edgeType, count := range observed {
		if _, ok := listed[edgeType]; !ok {
			t.Fatalf("golden carries %d %q edges at the delivery tier with no declared exception", count, edgeType)
		}
	}
	t.Logf("exception exercised by %s", formatCounts(observed))
}

// TestCleanupMutationsDeleteExactlyTheRecomputedCandidateSet derives the delete
// set from the frozen INPUTS and requires the mutations to match it exactly.
//
// Checking only "at least two pages, each at most 1000" was not enough: dropping
// the final 792-id page and regenerating the fixture's self-counts left every
// assertion green while 792 legacy orientations survived in the table. Counts the
// golden derives from itself cannot catch a golden that is wrong; the candidate
// set has to be recomputed from the dependency rows and the existing ids.
//
// The candidate set is, per _delete_dependency_edge_candidates: every existing
// native blocker edge id read from the cursor pages, plus — for each blocker
// dependency row — BOTH endpoint directions crossed with BLOCKS, IS_BLOCKED_BY
// and RELATES. That superset exists to catch every historical orientation a
// legacy row may have written, which is exactly why a missing page is not
// cosmetic.
func TestCleanupMutationsDeleteExactlyTheRecomputedCandidateSet(t *testing.T) {
	document := loadGolden(t)

	candidates := map[string]struct{}{}
	for pageIndex, page := range document.ExistingEdgeIDs {
		for _, index := range page {
			existing, err := document.String(index)
			if err != nil {
				t.Fatalf("existing edge id page %d: %v", pageIndex, err)
			}
			candidates[existing] = struct{}{}
		}
	}
	blockerRows := 0
	for index, dependency := range document.Dependencies {
		relationship, err := document.String(dependency[2])
		if err != nil {
			t.Fatalf("dependency %d relationship_type: %v", index, err)
		}
		switch strings.ToLower(relationship) {
		case "blocks", "blocked_by", "is_blocked_by":
		default:
			continue
		}
		source, err := document.String(dependency[0])
		if err != nil {
			t.Fatalf("dependency %d source: %v", index, err)
		}
		target, err := document.String(dependency[1])
		if err != nil {
			t.Fatalf("dependency %d target: %v", index, err)
		}
		if source == "" || target == "" {
			continue
		}
		blockerRows++
		for _, pair := range [][2]string{{source, target}, {target, source}} {
			for _, edgeType := range []string{EdgeTypeBlocks, EdgeTypeIsBlockedBy, EdgeTypeRelates} {
				candidates[EdgeID(NodeTypeIssue, pair[0], edgeType, NodeTypeIssue, pair[1])] = struct{}{}
			}
		}
	}
	if blockerRows == 0 {
		t.Fatal("the fixture has no blocker dependency rows, so it cannot exercise the cleanup at all")
	}
	expected := make([]string, 0, len(candidates))
	for id := range candidates {
		expected = append(expected, id)
	}
	sort.Strings(expected)

	// First mutation is the projection-run delete; the rest are the edge pages.
	if len(document.Mutations) < 2 {
		t.Fatalf("golden froze %d mutations; expected a projection delete plus edge pages", len(document.Mutations))
	}
	if !contains(document.Mutations[0].Statement, "ALTER TABLE work_graph_projection_runs DELETE") {
		t.Fatalf("first mutation is %q, expected the projection-run delete", document.Mutations[0].Statement)
	}
	deleted := make([]string, 0, len(expected))
	for offset, mutation := range document.Mutations[1:] {
		index := offset + 1
		if !contains(mutation.Statement, "ALTER TABLE work_graph_edges DELETE") {
			t.Fatalf("mutation %d is %q, expected an edge delete", index, mutation.Statement)
		}
		ids, ok := mutation.Parameters["edge_ids"].([]any)
		if !ok {
			t.Fatalf("mutation %d has no edge_ids array", index)
		}
		if len(ids) > 1000 {
			t.Fatalf("mutation %d deletes %d ids in one statement; the page size is 1000", index, len(ids))
		}
		// Every page but the last must be full, or the producer stopped paging early.
		if index < len(document.Mutations)-1 && len(ids) != 1000 {
			t.Fatalf(
				"mutation %d carries %d ids but is not the final page; a short page mid-run "+
					"means the candidate set was truncated",
				index, len(ids),
			)
		}
		for _, raw := range ids {
			id, ok := raw.(string)
			if !ok {
				t.Fatalf("mutation %d has a non-string edge id %#v", index, raw)
			}
			deleted = append(deleted, id)
		}
	}

	if len(deleted) != len(expected) {
		t.Fatalf(
			"the mutations delete %d ids but the candidate set recomputed from the frozen "+
				"dependencies and existing ids has %d — a page is missing or extra",
			len(deleted), len(expected),
		)
	}
	for index := range expected {
		if deleted[index] != expected[index] {
			t.Fatalf(
				"delete id %d is %s, recomputed candidate set has %s (the producer sorts the "+
					"set before paging, so order is part of the contract)",
				index, deleted[index], expected[index],
			)
		}
	}

	for index, mutation := range document.Mutations {
		if !contains(mutation.Statement, "org_id = {org_id:String}") {
			t.Fatalf("mutation %d is not org-scoped: %q", index, mutation.Statement)
		}
		if got, _ := mutation.Parameters["org_id"].(string); got != document.OrgID {
			t.Fatalf("mutation %d is scoped to %q, golden org is %q", index, got, document.OrgID)
		}
		if !contains(mutation.Statement, "mutations_sync=2") {
			t.Fatalf("mutation %d does not wait for the mutation: %q", index, mutation.Statement)
		}
	}
	t.Logf(
		"cleanup: %d blocker rows + %d existing ids -> %d candidates across %d pages",
		blockerRows, document.Counts["existing_edge_ids"], len(expected), len(document.Mutations)-1,
	)
}

// TestValidateConfidenceRefusesUngroupableValues covers what the golden cannot:
// every confidence in it is finite, which is exactly how lane-4441's NaN defect
// survived its own golden (CHAOS-4441, ca0b40349).
func TestValidateConfidenceRefusesUngroupableValues(t *testing.T) {
	for name, value := range map[string]float32{
		"NaN":        float32(math.NaN()),
		"+Inf":       float32(math.Inf(1)),
		"-Inf":       float32(math.Inf(-1)),
		"above one":  1.5,
		"below zero": -0.5,
	} {
		if err := ValidateConfidence(value); err == nil {
			t.Errorf("ValidateConfidence(%s) accepted an ungroupable value", name)
		}
	}
	for name, value := range map[string]float32{
		"delivery tier":    DeliveryConfidence,
		"associative tier": AssociativeConfidence,
		"zero":             0,
	} {
		if err := ValidateConfidence(value); err != nil {
			t.Errorf("ValidateConfidence(%s) rejected a valid value: %v", name, err)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func formatCounts(counts map[string]int) string {
	rendered := ""
	for edgeType, count := range counts {
		if rendered != "" {
			rendered += ", "
		}
		rendered += fmt.Sprintf("%s=%d", edgeType, count)
	}
	return rendered
}

// TestWindowBoundsAreTruncatedToTheSecondExactlyAsPythonRendersThem pins the
// bound-formatting contract against Python's OWN rendering, frozen in the
// fixture, rather than against a Go constant restating it.
//
// Python renders bounds with strftime("%Y-%m-%d %H:%M:%S") while the columns are
// DateTime64(3), so the sub-second component is dropped. A Go writer binding the
// raw instant would move every window boundary by up to a second in both
// directions. The frozen window carries non-zero milliseconds precisely so this
// test can fail.
func TestWindowBoundsAreTruncatedToTheSecondExactlyAsPythonRendersThem(t *testing.T) {
	document := loadGolden(t)

	from, err := time.Parse(time.RFC3339Nano, document.Config.FromTs)
	if err != nil {
		t.Fatalf("parse config.from_ts: %v", err)
	}
	to, err := time.Parse(time.RFC3339Nano, document.Config.ToTs)
	if err != nil {
		t.Fatalf("parse config.to_ts: %v", err)
	}

	// The fixture must actually exercise the truncation, or this test is theatre.
	if from.Nanosecond() == 0 && to.Nanosecond() == 0 {
		t.Fatal(
			"the frozen window has no sub-second component, so it cannot detect a Go writer " +
				"that binds the raw instant — regenerate with a window whose milliseconds are non-zero",
		)
	}
	for name, pair := range map[string]struct {
		instant  time.Time
		rendered string
	}{
		"from": {from, document.Config.ClickHouseBounds.From},
		"to":   {to, document.Config.ClickHouseBounds.To},
	} {
		if got := FormatDateTimeForClickHouse(pair.instant); got != pair.rendered {
			t.Errorf("%s bound: Go renders %q, Python froze %q", name, got, pair.rendered)
		}
		if pair.rendered == pair.instant.UTC().Format(time.RFC3339Nano) {
			t.Errorf("%s bound: the frozen rendering still carries sub-second precision", name)
		}
	}
}

// TestDependencyReadIsWindowIndependent asserts, from the frozen QUERY TEXT
// rather than from reading builder.py, that this sub-builder consults no time
// bound — so the window is not a hidden input to the edges the golden freezes.
//
// This is the structural half of the answer. The empirical half: two captures of
// the deployed producer, one with no window and one with the 14-day window frozen
// here, produced an IDENTICAL edge_id set (0 ids on either side only). What did
// differ between those runs was event_ts on 155 edges — not a window effect but
// CHAOS-4788 (the dependency read skips FINAL/argMax on a ReplacingMergeTree, so
// which unmerged version wins is decided by merge state).
func TestDependencyReadIsWindowIndependent(t *testing.T) {
	document := loadGolden(t)
	if len(document.Queries) == 0 {
		t.Fatal("the golden froze no query text, so window-independence cannot be asserted")
	}
	dependencyRead := document.Queries[0]
	if !contains(dependencyRead.Statement, "FROM work_item_dependencies") {
		t.Fatalf("first frozen query is not the dependency read: %q", dependencyRead.Statement)
	}
	for _, bound := range []string{
		"event_ts", "created_at", "author_when", "last_synced >", "last_synced <",
		"from_date", "to_date", "%(start", "%(end",
	} {
		if contains(dependencyRead.Statement, bound) {
			t.Errorf(
				"the dependency read now references %q — this sub-builder has gained a time bound, "+
					"so the window IS an input and the golden must freeze its effect: %q",
				bound, dependencyRead.Statement,
			)
		}
	}
	// It must still be org-scoped: the one input dimension it does consult.
	if !contains(dependencyRead.Statement, "org_id = '"+document.Config.OrgID+"'") {
		t.Errorf("the dependency read is not scoped to the golden's org: %q", dependencyRead.Statement)
	}
}

// TestGoldenFreezesItsFullProducerInput is the guard against the blind spot
// itself: every dimension of BuildConfig the producer could consult is present in
// the fixture, so a future sub-builder cannot quietly depend on one that was
// never captured.
func TestGoldenFreezesItsFullProducerInput(t *testing.T) {
	document := loadGolden(t)
	if document.Config.OrgID == "" {
		t.Error("config.org_id is not frozen")
	}
	if document.Config.OrgID != document.OrgID {
		t.Errorf("config.org_id %q disagrees with the document org %q", document.Config.OrgID, document.OrgID)
	}
	if document.Config.FromTs == "" || document.Config.ToTs == "" {
		t.Error("the build window is not frozen; an entire input dimension is outside the oracle")
	}
	if document.Config.ClickHouseBounds.From == "" || document.Config.ClickHouseBounds.To == "" {
		t.Error("the rendered ClickHouse bounds are not frozen")
	}
	if document.Config.HeuristicDaysWindow == 0 {
		t.Error("heuristic_days_window is not frozen (0 disables the heuristic builder entirely)")
	}
	if document.Config.HeuristicConfidence == 0 {
		t.Error("heuristic_confidence is not frozen")
	}
	if want := len(document.Queries); want != document.Counts["queries"] {
		t.Errorf("counts[queries] = %d, payload has %d", document.Counts["queries"], want)
	}
}

// TestProvenanceAndEvidenceAreDerivedFromTheFrozenInputs gives the two
// verbatim-serialised fields an oracle of their own, recomputed from the frozen
// dependency rows.
//
// Adversarial review round 4 found the gap: the generator serialises
// `provenance` and `evidence` exactly as the producer emitted them, and the rot
// guard only compares a regenerated golden against replay through that SAME
// current Python. So a Python change that corrupted either field, followed by a
// regeneration, went green — and taught the later Go port the corrupted output.
// Every assertion in this file was on some other column, so nothing caught it.
//
// Both fields are derivable, so the fix is an independent oracle rather than a
// narrower promise:
//
//   - provenance is unconditional: _build_issue_issue_edges writes
//     Provenance.NATIVE for every dependency-derived edge.
//   - evidence is `relationship_type_raw or relationship_type or "dependency"`
//     — Python truthiness, so an EMPTY raw falls through to the type.
//
// This is the same shape as the cleanup-set recomputation: derive the expected
// value from the inputs, never from the golden's own copy of the answer.
//
// The general class is CHAOS-4803 — nine generators in this repo serialise fields
// verbatim with no independent oracle, and this pattern is the remedy.
func TestProvenanceAndEvidenceAreDerivedFromTheFrozenInputs(t *testing.T) {
	document := loadGolden(t)

	type binding struct {
		low, high string
		edgeType  string
	}
	key := func(a, b, edgeType string) binding {
		if a > b {
			a, b = b, a
		}
		return binding{a, b, edgeType}
	}

	expectedEvidence := map[binding]map[string]struct{}{}
	for index, dependency := range document.Dependencies {
		source, err := document.String(dependency[0])
		if err != nil {
			t.Fatalf("dependency %d source: %v", index, err)
		}
		target, err := document.String(dependency[1])
		if err != nil {
			t.Fatalf("dependency %d target: %v", index, err)
		}
		relationship, err := document.String(dependency[2])
		if err != nil {
			t.Fatalf("dependency %d relationship_type: %v", index, err)
		}
		raw, err := document.String(dependency[3])
		if err != nil {
			t.Fatalf("dependency %d relationship_type_raw: %v", index, err)
		}
		// Python truthiness: `raw or relationship or "dependency"`.
		evidence := raw
		if evidence == "" {
			evidence = relationship
		}
		if evidence == "" {
			evidence = "dependency"
		}
		bindingKey := key(source, target, dependencyEdgeType(relationship))
		if expectedEvidence[bindingKey] == nil {
			expectedEvidence[bindingKey] = map[string]struct{}{}
		}
		expectedEvidence[bindingKey][evidence] = struct{}{}
	}

	pinned := 0
	for index, edge := range document.Edges {
		row, err := document.EdgeRow(edge)
		if err != nil {
			t.Fatalf("edge %d: %v", index, err)
		}
		if row.Provenance != ProvenanceNative {
			t.Fatalf(
				"edge %d carries provenance %q; this producer writes %q unconditionally, so "+
					"anything else means the producer changed and the golden froze the change",
				index, row.Provenance, ProvenanceNative,
			)
		}
		allowed, known := expectedEvidence[key(row.SourceID, row.TargetID, row.EdgeType)]
		if !known {
			t.Fatalf(
				"edge %d (%s <-%s-> %s) has no dependency row with those endpoints and that kind",
				index, row.SourceID, row.EdgeType, row.TargetID,
			)
		}
		if _, ok := allowed[row.Evidence]; !ok {
			t.Fatalf(
				"edge %d (%s <-%s-> %s) carries evidence %q, which is not "+
					"`relationship_type_raw or relationship_type or \"dependency\"` for any "+
					"dependency row with those endpoints and that kind",
				index, row.SourceID, row.EdgeType, row.TargetID, row.Evidence,
			)
		}
		if len(allowed) == 1 {
			pinned++
		}
	}
	if pinned*4 < len(document.Edges)*3 {
		t.Fatalf(
			"only %d of %d evidence values are pinned to a single derived string; the oracle "+
				"is too loose to detect a corrupted evidence field",
			pinned, len(document.Edges),
		)
	}
	t.Logf("provenance/evidence: %d of %d pinned to exactly one derived value", pinned, len(document.Edges))
}

// TestFrozenRelationshipValuesNeedNoCaseFolding is the precondition that makes
// dependencyEdgeType's exact matching provably equivalent to the producer's
// `.lower()` for this fixture.
//
// The producer folds; this helper does not, because no folding agrees with
// Python in general (see dependencyEdgeType's note). Exact matching is faithful
// precisely when folding would have been a no-op — so that is asserted rather
// than assumed. A regeneration that brought in "BLOCKS", or any value whose
// lowercase form differs from itself, fails here and tells whoever regenerated
// it to look, instead of silently diverging in whichever direction the folding
// rules happen to differ.
func TestFrozenRelationshipValuesNeedNoCaseFolding(t *testing.T) {
	document := loadGolden(t)
	seen := map[string]struct{}{}
	for index, dependency := range document.Dependencies {
		relationship, err := document.String(dependency[2])
		if err != nil {
			t.Fatalf("dependency %d relationship_type: %v", index, err)
		}
		seen[relationship] = struct{}{}
		for _, r := range relationship {
			if r > unicode.MaxASCII {
				t.Fatalf(
					"dependency %d relationship_type %q contains a non-ASCII rune; Go and "+
						"Python do not agree on how to lowercase it, so the exact-match mapper "+
						"cannot be shown equivalent to the producer",
					index, relationship,
				)
			}
		}
		if lowered := strings.ToLower(relationship); lowered != relationship {
			t.Fatalf(
				"dependency %d relationship_type %q is not already lowercase (folds to %q); "+
					"the producer folds and this helper does not, so they would disagree",
				index, relationship, lowered,
			)
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	t.Logf("relationship_type values, all ASCII-lowercase: %v", values)
}
