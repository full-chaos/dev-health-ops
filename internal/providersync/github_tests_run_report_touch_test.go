package providersync

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"
)

// CHAOS-14045: two matrix legs run the same tests, so their totals share every case identity. A mismatch keeps the artifacts it
// touches (a copy link, an equal suite instance), not every artifact that shares an identity: the exact total of the other leg is
// still dropped, in any upload order.

type matrixLegsLayout struct {
	shards        int
	otherLegFirst bool
	totalsFirst   bool
}

// matrixLegs returns the artifacts of two legs of n shards (leg A cases 0.125 s, leg B 0.25 s) and a total per leg; mismatchA
// and mismatchB fail shard 0 in that leg's total only. It also returns the ids of the two totals.
func matrixLegs(layout matrixLegsLayout, mismatchA, mismatchB bool) (artifacts []rowsArtifact, totalA, totalB string) {
	leg := func(dur string, failFirst bool) (shards [][]fxSuite, total []fxSuite) {
		for index := range layout.shards {
			name := fmt.Sprintf("shard-%d", index)
			shard := fxSuite{name: name, cases: []fxCase{{name: name, dur: dur}}}
			shards = append(shards, []fxSuite{shard})
			if index == 0 && failFirst {
				shard = fxSuite{name: name, cases: []fxCase{{name: name, dur: dur, status: "failed"}}}
			}
			total = append(total, shard)
		}
		return shards, total
	}
	shardsA, sumA := leg("0.125", mismatchA)
	shardsB, sumB := leg("0.25", mismatchB)
	add := func(suites []fxSuite) string {
		id := fmt.Sprint(len(artifacts) + 1)
		artifacts = append(artifacts, rowsArtifact{id: id, size: int64(len(artifacts)+1) * 10, body: fxJUnit(suites)})
		return id
	}
	addTotals := func() { totalA, totalB = add(sumA), add(sumB) }
	if layout.totalsFirst {
		addTotals()
	}
	first, second := shardsA, shardsB
	if layout.otherLegFirst {
		first, second = shardsB, shardsA
	}
	for _, shard := range append(first, second...) {
		add(shard)
	}
	if !layout.totalsFirst {
		addTotals()
	}
	return artifacts, totalA, totalB
}

var matrixLegsLayouts = []matrixLegsLayout{
	{shards: 2}, {shards: 2, otherLegFirst: true}, {shards: 2, totalsFirst: true},
	{shards: 4, otherLegFirst: true}, {shards: 5}, {shards: 5, otherLegFirst: true}, {shards: 8, otherLegFirst: true, totalsFirst: true},
}

func (layout matrixLegsLayout) String() string {
	return fmt.Sprintf("shards-%d/other-leg-first-%t/totals-first-%t", layout.shards, layout.otherLegFirst, layout.totalsFirst)
}

// The red cell of CHAOS-14045: leg A's total mismatches its parts, leg B's total is exact; leg B's total is dropped.
func TestGitHubTestsRunReportsDropLegBExactTotalWhenLegAMismatches(t *testing.T) {
	legA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125"}}}, {name: "right", cases: []fxCase{{name: "b", dur: "0.125"}}}}
	legB := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}, {name: "right", cases: []fxCase{{name: "b", dur: "0.25"}}}}
	totalA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125", status: "failed"}}}, legA[1]}
	artifacts := []rowsArtifact{
		{id: "1", size: 10, body: fxJUnit(legA[:1])}, {id: "2", size: 20, body: fxJUnit(legA[1:])},
		{id: "3", size: 30, body: fxJUnit(legB[:1])}, {id: "4", size: 40, body: fxJUnit(legB[1:])},
		{id: "5", size: 50, body: fxJUnit(totalA)}, {id: "6", size: 60, body: fxJUnit(legB)},
	}
	assertGroupDecision(t, artifacts, map[string]string{"5": githubTestsAggregateMismatchCause, "6": githubTestsAggregateOfPartsCause})
}

// The same in every upload order and shard count, and mirrored: the mismatched total is leg B's, its parts sit in leg B, and leg A's
// exact total is the one dropped. Both legs' shards are always committed.
func TestGitHubTestsRunReportsDropTheOtherLegsExactTotalInEveryLayout(t *testing.T) {
	for _, layout := range matrixLegsLayouts {
		for _, mismatchedLeg := range []string{"A", "B"} {
			t.Run(layout.String()+"/mismatch-in-leg-"+mismatchedLeg, func(t *testing.T) {
				artifacts, totalA, totalB := matrixLegs(layout, mismatchedLeg == "A", mismatchedLeg == "B")
				want := map[string]string{totalA: githubTestsAggregateMismatchCause, totalB: githubTestsAggregateOfPartsCause}
				if mismatchedLeg == "B" {
					want = map[string]string{totalA: githubTestsAggregateOfPartsCause, totalB: githubTestsAggregateMismatchCause}
				}
				assertGroupDecision(t, artifacts, want)
			})
		}
	}
}

// Both totals mismatch: both are kept, and no shard of either leg is dropped.
func TestGitHubTestsRunReportsKeepBothMismatchedTotals(t *testing.T) {
	for _, layout := range matrixLegsLayouts {
		t.Run(layout.String(), func(t *testing.T) {
			artifacts, totalA, totalB := matrixLegs(layout, true, true)
			assertGroupDecision(t, artifacts, map[string]string{totalA: githubTestsAggregateMismatchCause, totalB: githubTestsAggregateMismatchCause})
		})
	}
}

// R-IDENT-CONTENT: when the legs hold an equal suite instance (here leg B's "right" shard equals leg A's, so it is a copy), a
// mismatch in leg A's total touches leg B through it: leg B's exact total is kept and reported, as before CHAOS-14045, and no
// shard of either leg is dropped.
func TestGitHubTestsRunReportsKeepTheOtherTotalWhenLegsShareASuiteInstance(t *testing.T) {
	right := fxSuite{name: "right", cases: []fxCase{{name: "b", dur: "0.125"}}}
	legA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125"}}}, right}
	legB := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}, right}
	totalA := []fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.125", status: "failed"}}}, right}
	artifacts := []rowsArtifact{
		{id: "1", size: 10, body: fxJUnit(legA[:1])}, {id: "2", size: 20, body: fxJUnit(legA[1:])},
		{id: "3", size: 30, body: fxJUnit(legB[:1])}, {id: "4", size: 40, body: fxJUnit(legB[1:])},
		{id: "5", size: 50, body: fxJUnit(totalA)}, {id: "6", size: 60, body: fxJUnit(legB)},
	}
	mismatch := githubTestsAggregateMismatchCause
	assertGroupDecision(t, artifacts, map[string]string{"4": mismatch, "5": mismatch, "6": mismatch})
}

// matrixRoleWalk walks the artifacts (each with its role and id; size = its archive size) and returns the decision by role: the
// marker cause, or "kept".
func matrixRoleWalk(t *testing.T, roles []string, artifacts []rowsArtifact) (githubTestsWalk, map[string]string) {
	t.Helper()
	sized := slices.Clone(artifacts)
	for index := range sized {
		members := map[string]string{"junit.xml": sized[index].body}
		maps.Copy(members, sized[index].extra)
		sized[index].size = int64(len(githubTestsZip(t, members)))
	}
	walk, _, _ := rowsWalk(t, 1, sized...)
	causes := markerCauses(walk)
	decisions := map[string]string{}
	for index, role := range roles {
		decisions[role] = "kept"
		if cause, ok := causes[sized[index].id]; ok {
			decisions[role] = cause
		}
	}
	return walk, decisions
}

// CHAOS-14045 round 1 P1: a copy family's representative is chosen by content, so two timed copies of which only one holds
// coverage rows give the same stored rows, markers and watermark whichever is uploaded first. The coverage-bearing copy is the
// representative: the plain copy holds nothing it does not, so it is dropped, and a mismatch is seeded only by a real
// disagreement. The expected suite counts are the rows each distinct suite instance contributes once: an exact total whose touch
// group holds no mismatch is dropped, and so is the plain copy.
func TestGitHubTestsRunReportsCopyRepresentativeIsChosenByContent(t *testing.T) {
	leftA := fxSuite{name: "left", cases: []fxCase{{name: "a", dur: "0.125"}}}
	rightA := fxSuite{name: "right", cases: []fxCase{{name: "b", dur: "0.125"}}}
	badA := fxSuite{name: "left", cases: []fxCase{{name: "a", dur: "0.125", status: "failed"}}}
	leftB := fxSuite{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}
	rightB := fxSuite{name: "right", cases: []fxCase{{name: "b", dur: "0.25"}}}
	drop, mismatch := githubTestsAggregateOfPartsCause, githubTestsAggregateMismatchCause
	for _, coverage := range []struct {
		name  string
		files map[string]string
	}{
		{"lcov", map[string]string{"coverage.info": groupLCOV}},
		{"cobertura", map[string]string{"coverage.xml": groupCobertura}},
		{"both", map[string]string{"coverage.info": groupLCOV, "coverage.xml": groupCobertura}},
	} {
		for _, layout := range []struct {
			name     string
			roles    []string
			suites   [][]fxSuite
			want     map[string]string
			rows     int
			advances bool
		}{
			// One suite instance (left of leg B, with its coverage): 1 suite.
			{"copy-only", []string{"plain", "covered"}, [][]fxSuite{{leftB}, {leftB}},
				map[string]string{"plain": drop, "covered": "kept"}, 1, true},
			// left-B (covered copy) and right-B: 2 suites; the plain copy and total-b hold only those rows.
			{"touching-exact-total", []string{"plain", "covered", "right-b", "total-b"}, [][]fxSuite{{leftB}, {leftB}, {rightB}, {leftB, rightB}},
				map[string]string{"plain": drop, "covered": "kept", "right-b": "kept", "total-b": drop}, 2, true},
			// Leg B as above (2) plus leg A's two shards (2) and its mismatched total (2), which touches only leg A: 6 suites.
			{"exact-total-beside-an-independent-mismatch",
				[]string{"plain", "covered", "right-b", "total-b", "left-a", "right-a", "total-a"},
				[][]fxSuite{{leftB}, {leftB}, {rightB}, {leftB, rightB}, {leftA}, {rightA}, {badA, rightA}},
				map[string]string{"plain": drop, "covered": "kept", "right-b": "kept", "total-b": drop, "left-a": "kept", "right-a": "kept", "total-a": mismatch}, 6, false},
			// The copies are totals of two shards; the covered total disagrees with its shards (coverage), so it and its plain copy
			// are kept and reported: 2 + 2 + 1 + 1 = 6 suites.
			{"copies-of-a-total", []string{"plain", "covered", "left-b", "right-b"}, [][]fxSuite{{leftB, rightB}, {leftB, rightB}, {leftB}, {rightB}},
				map[string]string{"plain": mismatch, "covered": mismatch, "left-b": "kept", "right-b": "kept"}, 6, false},
		} {
			t.Run(coverage.name+"/"+layout.name, func(t *testing.T) {
				for _, coveredFirst := range []bool{false, true} {
					for _, reversed := range []bool{false, true} {
						artifacts := make([]rowsArtifact, len(layout.roles))
						for index, suites := range layout.suites {
							artifacts[index] = rowsArtifact{id: fmt.Sprint(index + 1), body: fxJUnit(suites)}
						}
						artifacts[1].extra = coverage.files
						if coveredFirst {
							artifacts[0].id, artifacts[1].id = artifacts[1].id, artifacts[0].id
						}
						roles := slices.Clone(layout.roles)
						if reversed {
							slices.Reverse(artifacts)
							slices.Reverse(roles)
						}
						walk, got := matrixRoleWalk(t, roles, artifacts)
						if !maps.Equal(got, layout.want) || walk.cursor.Suites != layout.rows || walk.cursor.Cases != layout.rows ||
							walk.cursor.Coverage == 0 || (walk.final.Watermark != nil) != layout.advances {
							t.Fatalf("covered first %t, listing reversed %t: decisions %v, suites %d, cases %d, coverage %d, advanced %t; "+
								"want %v, %d suites and cases, coverage kept, advanced %t", coveredFirst, reversed, got, walk.cursor.Suites,
								walk.cursor.Cases, walk.cursor.Coverage, walk.final.Watermark != nil, layout.want, layout.rows, layout.advances)
						}
					}
				}
			})
		}
	}
}

func matrixJobLog() string {
	log := make([]byte, 600_000)
	samples := rand.NewPCG(63, 79)
	for index := range log {
		log[index] = byte(' ' + samples.Uint64()%95)
	}
	return string(log)
}

// CHAOS-14045 round 1 P2(1): the run's report rows are admitted on the final kept set, after the drops. Dropping leg B's exact
// total frees budget, which an unrelated report of 199,966 rows then fills; leg A's mismatched total (it carries a large test log,
// so it sorts last) no longer fits and is refused with per_run_cap, the committed rows stay within the run bound (199,982 held),
// and the watermark is withheld.
func TestGitHubTestsRunReportsAdmitOnTheFinalKeptSet(t *testing.T) {
	artifacts, late, goodTotal := matrixLegs(matrixLegsLayout{shards: 2}, true, false)
	for index := range artifacts {
		if artifacts[index].id == late {
			artifacts[index].extra = map[string]string{"test.log": matrixJobLog()}
		}
	}
	artifacts = append(artifacts, rowsArtifact{id: "7", body: githubTestsNamedMultiSuiteJUnit("unrelated-", (githubTestsMaxReportRowsPerRun-9)/2)})
	walk, got := matrixRoleWalk(t, []string{"1", "2", "3", "4", "5", "6", "7"}, artifacts)
	var lateCauses []string
	for _, marker := range walk.cursor.SkippedArtifacts {
		if marker.ArtifactID == late {
			lateCauses = append(lateCauses, marker.Cause)
		}
	}
	if rows := committedReportRows(walk); rows > githubTestsMaxReportRowsPerRun {
		t.Fatalf("committed %d report rows, over the run bound %d", rows, githubTestsMaxReportRowsPerRun)
	}
	if !slices.Contains(lateCauses, githubTestsPerRunCapCause) || got["7"] != "kept" || got[goodTotal] != githubTestsAggregateOfPartsCause {
		t.Fatalf("late artifact %s markers %v, decisions %v: want the late artifact refused with per_run_cap, the unrelated report "+
			"kept and leg B's exact total dropped", late, lateCauses, got)
	}
	if observed(walk, githubTestsPerRunCapCause) != 1 {
		t.Fatalf("observations %+v, want per_run_cap once", walk.cursor.Incomplete)
	}
	if walk.final.Watermark != nil {
		t.Fatal("the watermark advanced although the run was cut by the bound")
	}
}

const (
	coverageMissLCOV      = "TN:\nSF:foo.go\nDA:1,1\nDA:2,0\nLF:2\nLH:1\nend_of_record\n"
	coverageMissCobertura = `<coverage lines-valid="2" lines-covered="1"><packages><package name="example"><classes><class filename="foo.go">` +
		`<lines><line number="1" hits="1"/><line number="2" hits="0"/></lines></class></classes></package></packages></coverage>`
)

// coveragePayload is a coverage report in one format ("lcov", "cobertura" or "both"), all lines covered (hit) or half of them.
func coveragePayload(format string, hit bool) map[string]string {
	lcov, cobertura := coverageMissLCOV, coverageMissCobertura
	if hit {
		lcov, cobertura = groupLCOV, groupCobertura
	}
	switch format {
	case "lcov":
		return map[string]string{"coverage.info": lcov}
	case "cobertura":
		return map[string]string{"coverage.xml": cobertura}
	}
	return map[string]string{"coverage.info": lcov, "coverage.xml": cobertura}
}

// markedCopyRole walks two timed copies of one JUnit report (ids 1 and 2; swapped puts the second role at id 1) and returns the
// role marked aggregate_mismatch; both must be kept and the watermark withheld.
func markedCopyRole(t *testing.T, first, second map[string]string, swapped, reversed bool) string {
	t.Helper()
	body := fxJUnit([]fxSuite{{name: "left", cases: []fxCase{{name: "a", dur: "0.25"}}}})
	roles := []string{"first", "second"}
	artifacts := []rowsArtifact{{id: "1", body: body, extra: first}, {id: "2", body: body, extra: second}}
	if swapped {
		artifacts[0].id, artifacts[1].id = artifacts[1].id, artifacts[0].id
	}
	if reversed {
		slices.Reverse(artifacts)
		slices.Reverse(roles)
	}
	walk, got := matrixRoleWalk(t, roles, artifacts)
	marked := ""
	for role, decision := range got {
		switch decision {
		case githubTestsAggregateMismatchCause:
			if marked != "" {
				t.Fatalf("decisions %v: more than one copy reported", got)
			}
			marked = role
		case "kept":
		default:
			t.Fatalf("decisions %v: a coverage-bearing copy was dropped", got)
		}
	}
	if marked == "" || walk.cursor.Suites != 2 || walk.final.Watermark != nil {
		t.Fatalf("decisions %v, %d suites, advanced %t: want both copies kept, one reported, the watermark withheld",
			got, walk.cursor.Suites, walk.final.Watermark != nil)
	}
	return marked
}

// CHAOS-14045 round 2 P1: two copies with identical JUnit and DIFFERENT coverage both hold coverage rows, so the coverage rows
// are in the content key: the same copy is reported in every upload order and listing order, for all 6 pairings of LCOV,
// Cobertura and both.
func TestGitHubTestsRunReportsCoverageChoosesTheRepresentativeOfCoveredCopies(t *testing.T) {
	formats := []string{"lcov", "cobertura", "both"}
	for _, hitFormat := range formats {
		for _, missFormat := range formats {
			if hitFormat == missFormat {
				continue
			}
			t.Run("hit-"+hitFormat+"/miss-"+missFormat, func(t *testing.T) {
				hit, miss := coveragePayload(hitFormat, true), coveragePayload(missFormat, false)
				want := markedCopyRole(t, hit, miss, false, false)
				for _, swapped := range []bool{false, true} {
					for _, reversed := range []bool{false, true} {
						if got := markedCopyRole(t, hit, miss, swapped, reversed); got != want {
							t.Fatalf("ids swapped %t, listing reversed %t: reported %s, want %s as in the first order", swapped, reversed, got, want)
						}
					}
				}
			})
		}
	}
}

// Equal coverage in two formats is equal content (the key holds the stored rows without their format), so the tie goes to the
// lowest upload id: the copy at id 2 is the one reported, whichever format it carries.
func TestGitHubTestsRunReportsEqualCoverageInTwoFormatsTiesToTheLowestID(t *testing.T) {
	for _, hit := range []bool{true, false} {
		lcov, cobertura := coveragePayload("lcov", hit), coveragePayload("cobertura", hit)
		for _, swapped := range []bool{false, true} {
			wantMarked := "second"
			if swapped {
				wantMarked = "first"
			}
			if got := markedCopyRole(t, lcov, cobertura, swapped, false); got != wantMarked {
				t.Fatalf("hit %t, ids swapped %t: reported %s, want %s (the higher id)", hit, swapped, got, wantMarked)
			}
		}
	}
}

// Two coverage rows per copy, the same two rows in each copy but read in archive order (the archive's member order is not fixed):
// the rows are sorted in the key, so the tie still goes to the lowest upload id. Repeated, since the member order is random.
func TestGitHubTestsRunReportsCoverageRowOrderDoesNotChooseTheRepresentative(t *testing.T) {
	first := map[string]string{"coverage.info": groupLCOV, "coverage.xml": coverageMissCobertura}
	second := map[string]string{"coverage.info": coverageMissLCOV, "coverage.xml": groupCobertura}
	for attempt := range 16 {
		for _, swapped := range []bool{false, true} {
			wantMarked := "second"
			if swapped {
				wantMarked = "first"
			}
			if got := markedCopyRole(t, first, second, swapped, false); got != wantMarked {
				t.Fatalf("attempt %d, ids swapped %t: reported %s, want %s (the higher id)", attempt, swapped, got, wantMarked)
			}
		}
	}
}

// Zero line coverage is stored alike in the two formats (CHAOS-14284): LCOV with LH:0 and Cobertura with lines-covered="0" both
// store lines_covered 0, so equal zero coverage is equal content and the tie goes to the lowest upload id in both id orders.
func TestGitHubTestsRunReportsZeroCoverageEqualInLCOVAndCobertura(t *testing.T) {
	lcov := map[string]string{"coverage.info": "TN:\nSF:foo.go\nDA:1,0\nLF:1\nLH:0\nend_of_record\n"}
	cobertura := map[string]string{"coverage.xml": `<coverage lines-valid="1" lines-covered="0"><packages><package name="example"><classes>` +
		`<class filename="foo.go"><lines><line number="1" hits="0"/></lines></class></classes></package></packages></coverage>`}
	for _, swapped := range []bool{false, true} {
		wantMarked := "second"
		if swapped {
			wantMarked = "first"
		}
		if got := markedCopyRole(t, lcov, cobertura, swapped, false); got != wantMarked {
			t.Fatalf("ids swapped %t: reported %s, want %s (the higher id): equal zero coverage is equal content", swapped, got, wantMarked)
		}
	}
}

// The coverage part of a copy family's representative key is an allow-list (githubTestsCoverageContent). Every field of
// coverageSnapshotRow is classified here: content (in the key) or identity (the run, artifact, format, attribution or sync time,
// never in the key). A field added, removed or renamed fails until it is classified, and the content class must equal the
// allow-list exactly.
func TestGitHubTestsCoverageRowFieldsAreClassified(t *testing.T) {
	classes := map[string]string{
		"RepoID": "identity", "RunID": "identity", "SnapshotID": "identity", "ReportFormat": "identity",
		"LinesTotal": "content", "LinesCovered": "content", "LineCoveragePct": "content",
		"BranchesTotal": "content", "BranchesCovered": "content", "BranchCoveragePct": "content",
		"FunctionsTotal": "content", "FunctionsCovered": "content",
		"CommitHash": "identity", "Branch": "identity", "PRNumber": "identity", "TeamID": "identity",
		"ServiceID": "content", "OrgID": "identity", "LastSynced": "identity",
		"ReportSlot": "identity", "RunAttempt": "identity", "LegacySnapshotIDs": "identity",
	}
	row := reflect.TypeFor[coverageSnapshotRow]()
	seen := map[string]bool{}
	var content []string
	for index := range row.NumField() {
		name := row.Field(index).Name
		seen[name] = true
		switch classes[name] {
		case "content":
			content = append(content, name)
		case "identity":
		default:
			t.Errorf("coverageSnapshotRow.%s is not classified: decide whether it is report content (add it to githubTestsCoverageContent) or identity", name)
		}
	}
	for name := range classes {
		if !seen[name] {
			t.Errorf("classified field %s is no longer in coverageSnapshotRow (removed or renamed)", name)
		}
	}
	allow := reflect.TypeFor[githubTestsCoverageContent]()
	var allowed []string
	for index := range allow.NumField() {
		allowed = append(allowed, allow.Field(index).Name)
	}
	slices.Sort(content)
	slices.Sort(allowed)
	if !slices.Equal(content, allowed) {
		t.Errorf("content fields %v, allow-list fields %v: they must be the same", content, allowed)
	}
}

// Two covered copies whose coverage differs only in the service (the report's file paths): the service is content, so the same
// copy is reported in both upload orders.
func TestGitHubTestsRunReportsCoverageServiceIsContent(t *testing.T) {
	cobertura := func(path string) map[string]string {
		return map[string]string{"coverage.xml": `<coverage lines-valid="1" lines-covered="1"><packages><package name="example"><classes>` +
			`<class filename="` + path + `"><lines><line number="1" hits="1"/></lines></class></classes></package></packages></coverage>`}
	}
	frontend, backend := cobertura("services/frontend/foo.go"), cobertura("services/backend/foo.go")
	want := markedCopyRole(t, frontend, backend, false, false)
	if got := markedCopyRole(t, frontend, backend, true, false); got != want {
		t.Fatalf("ids swapped: reported %s, want %s as in the first order: the service must be in the key", got, want)
	}
}

// Two covered copies parsed in two sync passes carry different last_synced times. The sync time is identity, not content, so the
// tie between equal coverage still goes to the lowest upload id, whichever copy was parsed first.
func TestGitHubTestsRunReportsSyncTimeDoesNotChooseTheRepresentative(t *testing.T) {
	one, hundred := int64(1), 100.0
	duration, suiteTime := 0.25, 1.0
	report := func(id string, synced time.Time) githubTestsParsedReport {
		suite := "suite-" + id
		return githubTestsParsedReport{
			ID: json.Number(id), Name: "copy-" + id, Reports: 1,
			Suites: []testSuiteResultRow{{SuiteID: suite, SuiteName: "left", DurationSeconds: &suiteTime, ReportSlot: "copy-" + id}},
			Cases:  []testCaseResultRow{{SuiteID: suite, CaseID: "case-" + id, CaseName: "a", Status: "passed", DurationSeconds: &duration}},
			Coverage: []coverageSnapshotRow{{SnapshotID: "snapshot-" + id, LinesTotal: &one, LinesCovered: &one, LineCoveragePct: &hundred,
				LastSynced: synced, ReportSlot: "copy-" + id}},
		}
	}
	early, late := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC), time.Date(2026, 11, 20, 2, 0, 0, 0, time.UTC)
	for _, earlyAtHigherID := range []bool{false, true} {
		first, second := report("1", early), report("2", late)
		if earlyAtHigherID {
			first, second = report("1", late), report("2", early)
		}
		plan, err := githubTestsPlanRunReports([]githubTestsParsedReport{first, second})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Mismatch) != 1 || plan.Mismatch[0].Index != 1 || !slices.Equal(plan.Mismatch[0].Parts, []int{0}) {
			t.Fatalf("early sync at the higher id %t: mismatch %+v, want copy 2 reported against copy 1 (the lowest id wins the tie)",
				earlyAtHigherID, plan.Mismatch)
		}
	}
}
