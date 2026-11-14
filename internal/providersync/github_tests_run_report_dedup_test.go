package providersync

import (
	"bytes"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
)

// CHAOS-13794: the content test on a run's report artifacts. An artifact is dropped only when its cases (class, name, outcome) equal
// those of an artifact with a lower id, or equal the disjoint union of two or more other artifacts of the run; an aggregate whose
// cases match its parts but whose outcomes differ is kept with all of them. The shapes below are acr's own: go-junit and the ten
// go-unit-shard artifacts of its `ci` run, with the real case counts of run 43608150841.

// fxCase.dur is the case's `time` attribute: "" means 0.1, "-" leaves the attribute out, anything else is written as given.
type fxCase struct{ name, status, dur string }

// fxSuite.ts is the suite's `timestamp` attribute, left out when empty.
type fxSuite struct {
	name  string
	ts    string
	cases []fxCase
}

func fxJUnit(suites []fxSuite) string {
	var body strings.Builder
	body.WriteString(`<testsuites>`)
	for _, suite := range suites {
		stamp := ""
		if suite.ts != "" {
			stamp = ` timestamp="` + suite.ts + `"`
		}
		body.WriteString(`<testsuite name="` + suite.name + `" time="1.0"` + stamp + `>`)
		for _, c := range suite.cases {
			duration := ` time="0.1"`
			switch c.dur {
			case "":
			case "-":
				duration = ""
			default:
				duration = ` time="` + c.dur + `"`
			}
			switch c.status {
			case "failed":
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg"` + duration + `><failure message="boom"/></testcase>`)
			case "skipped":
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg"` + duration + `><skipped/></testcase>`)
			default:
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg"` + duration + `/>`)
			}
		}
		body.WriteString(`</testsuite>`)
	}
	body.WriteString(`</testsuites>`)
	return body.String()
}

// part builds `suites` suites holding `cases` cases between them; every 997th case fails, so the outcomes are not all alike.
func part(prefix string, suites, cases int) []fxSuite {
	out := make([]fxSuite, suites)
	for index := range out {
		out[index].name = fmt.Sprintf("%s-suite-%d", prefix, index)
	}
	for c := 0; c < cases; c++ {
		status := "passed"
		if c%997 == 996 {
			status = "failed"
		}
		suite := &out[c%suites]
		suite.cases = append(suite.cases, fxCase{name: fmt.Sprintf("%s-case-%d", prefix, c), status: status})
	}
	return out
}

func rowsOfSuites(suites []fxSuite) int {
	rows := 0
	for _, suite := range suites {
		rows += 1 + len(suite.cases)
	}
	return rows
}

// acrRun is acr's run 43608150841: the ten go-unit-shard artifacts and the go-junit aggregate (ids and declared sizes as listed).
func acrRun() (parts []rowsArtifact, aggregate rowsArtifact, partRows int) {
	ids := []string{"14119720052", "14119358566", "14119262380", "14119062971", "14119717054", "14118739741", "14120036368", "14118828359", "14118918063", "14118849212"}
	sizes := []int64{386412, 1239328, 2136516, 722887, 319033, 168773, 98650, 109848, 52013, 85481}
	suites := []int{34, 33, 32, 32, 1, 1, 1, 1, 1, 1}
	cases := []int{2458, 9509, 15567, 5153, 2059, 1010, 214, 182, 9, 166}
	var all []fxSuite
	for index := range ids {
		spec := part(fmt.Sprintf("p%d", index+1), suites[index], cases[index])
		all = append(all, spec...)
		parts = append(parts, rowsArtifact{id: ids[index], size: sizes[index], body: fxJUnit(spec)})
		partRows += rowsOfSuites(spec)
	}
	return parts, rowsArtifact{id: "14120460490", size: 577770, body: fxJUnit(all)}, partRows
}

func markerCauses(walk githubTestsWalk) map[string]string {
	out := map[string]string{}
	for _, marker := range walk.cursor.SkippedArtifacts {
		out[marker.ArtifactID] = marker.Cause
	}
	return out
}

func observed(walk githubTestsWalk, cause string) int {
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == githubTestsRunReportsComponent && observation.Cause == cause {
			return observation.Count
		}
	}
	return 0
}

// acr shape: the aggregate equals its ten disjoint parts, so it is dropped and the parts stay.
func TestGitHubTestsRunReportsDropAnAggregateOfItsParts(t *testing.T) {
	parts, aggregate, partRows := acrRun()
	walk, doer, rendered := rowsWalk(t, 1, append([]rowsArtifact{aggregate}, parts...)...)
	if got := committedReportRows(walk); got != partRows {
		t.Fatalf("committed %d rows, want the ten parts (%d), not parts plus aggregate", got, partRows)
	}
	if causes := markerCauses(walk); len(causes) != 1 || causes[aggregate.id] != githubTestsAggregateOfPartsCause {
		t.Fatalf("markers %v, want exactly the aggregate %s as %s", causes, aggregate.id, githubTestsAggregateOfPartsCause)
	}
	if observed(walk, githubTestsAggregateOfPartsCause) != 1 || observed(walk, githubTestsPerRunCapCause) != 0 {
		t.Fatalf("observations %+v, want aggregate_of_parts x1 and no per_run_cap", walk.cursor.Incomplete)
	}
	if len(doer.downloaded) != 11 {
		t.Fatalf("downloaded %d artifacts, want all 11 (the decision needs the aggregate parsed)", len(doer.downloaded))
	}
	want := `component="run_reports",cause="aggregate_of_parts"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("want %q in:\n%s", want, rendered)
	}
	if walk.cursor.Phase != "done" || walk.final.Watermark == nil {
		t.Fatalf("phase %q watermark %v: aggregate_of_parts must advance the watermark", walk.cursor.Phase, walk.final.Watermark)
	}
}

// One part missing: the aggregate no longer equals the union of what is present, so everything is kept.
func TestGitHubTestsRunReportsKeepTheAggregateWhenAPartIsMissing(t *testing.T) {
	parts, aggregate, partRows := acrRun()
	missing := parts[len(parts)-1]
	walk, _, _ := rowsWalk(t, 1, append([]rowsArtifact{aggregate}, parts[:len(parts)-1]...)...)
	wantRows := partRows - rowsOfSuites(part("p10", 1, 166)) + partRows
	if got := committedReportRows(walk); got != wantRows {
		t.Fatalf("committed %d rows without part %s, want the aggregate and nine parts (%d)", got, missing.id, wantRows)
	}
	if len(walk.cursor.SkippedArtifacts) != 0 || observed(walk, githubTestsAggregateOfPartsCause) != 0 {
		t.Fatalf("markers %+v observations %+v, want none", walk.cursor.SkippedArtifacts, walk.cursor.Incomplete)
	}
}

// Matrix legs of one suite that overlap fully but disagree on an outcome are not duplicates and not an aggregate: both stay.
func TestGitHubTestsRunReportsKeepMatrixLegsThatDisagree(t *testing.T) {
	legA := part("m", 4, 200)
	legB := part("m", 4, 200)
	legB[0].cases[0].status = "skipped"
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "21", size: 10, body: fxJUnit(legA)}, rowsArtifact{id: "22", size: 20, body: fxJUnit(legB)})
	if got, want := committedReportRows(walk), rowsOfSuites(legA)+rowsOfSuites(legB); got != want {
		t.Fatalf("committed %d rows, want both legs (%d)", got, want)
	}
	if len(walk.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("markers %+v, want none for legs that differ", walk.cursor.SkippedArtifacts)
	}
}

// Two artifacts with equal cases are duplicates: the one with the higher id goes, whichever way the listing and sizes put them.
func TestGitHubTestsRunReportsDropAnExactDuplicateByHigherID(t *testing.T) {
	content := fxJUnit(part("d", 3, 90))
	low := rowsArtifact{id: "30", size: 900, body: content}
	high := rowsArtifact{id: "40", size: 100, body: content}
	for name, listing := range map[string][]rowsArtifact{"low first": {low, high}, "high first": {high, low}} {
		walk, _, _ := rowsWalk(t, 1, listing...)
		if got, want := committedReportRows(walk), rowsOfSuites(part("d", 3, 90)); got != want {
			t.Fatalf("%s: committed %d rows, want one copy (%d)", name, got, want)
		}
		if causes := markerCauses(walk); len(causes) != 1 || causes["40"] != githubTestsAggregateOfPartsCause {
			t.Fatalf("%s: markers %v, want only the higher id 40 as aggregate_of_parts", name, causes)
		}
	}
}

// An aggregate whose cases match its parts but whose outcomes differ stays, with every part, and is reported; it does not advance.
func TestGitHubTestsRunReportsKeepAllAndMarkAMismatch(t *testing.T) {
	first, second := part("x", 2, 100), part("y", 2, 100)
	all := append(append([]fxSuite(nil), first...), second...)
	aggregate := make([]fxSuite, len(all))
	for index, suite := range all {
		aggregate[index] = fxSuite{name: suite.name, cases: append([]fxCase(nil), suite.cases...)}
	}
	aggregate[0].cases[1].status = "failed"
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	walk, _, rendered := rowsWalk(t, 1, rowsArtifact{id: "51", size: 10, body: fxJUnit(first)}, rowsArtifact{id: "52", size: 20, body: fxJUnit(second)},
		rowsArtifact{id: "53", size: 30, body: fxJUnit(aggregate)})
	slog.SetDefault(previous)
	// ONE warning line, naming the run and every artifact involved.
	var warnings []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "disagree on outcome") {
			warnings = append(warnings, line)
		}
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "artifact_ids") || !strings.Contains(warnings[0], "53") ||
		!strings.Contains(warnings[0], "51") || !strings.Contains(warnings[0], "52") || !strings.Contains(warnings[0], "run=") {
		t.Fatalf("warnings %q, want one line with the run id and artifact ids 51, 52 and 53", warnings)
	}
	if got, want := committedReportRows(walk), rowsOfSuites(first)+rowsOfSuites(second)+rowsOfSuites(aggregate); got != want {
		t.Fatalf("committed %d rows, want all three artifacts (%d)", got, want)
	}
	if causes := markerCauses(walk); len(causes) != 1 || causes["53"] != githubTestsAggregateMismatchCause {
		t.Fatalf("markers %v, want only the aggregate 53 as aggregate_mismatch", causes)
	}
	if observed(walk, githubTestsAggregateMismatchCause) != 1 {
		t.Fatalf("observations %+v, want aggregate_mismatch x1", walk.cursor.Incomplete)
	}
	if !strings.Contains(rendered, `component="run_reports",cause="aggregate_mismatch"} 1`) {
		t.Fatalf("metric missing in:\n%s", rendered)
	}
	if walk.final.Watermark != nil {
		t.Fatalf("watermark %v, want none: a mismatch must not advance", walk.final.Watermark)
	}
}

// The decision does not depend on the order the artifacts are listed in.
func TestGitHubTestsRunReportsDecisionDoesNotDependOnListingOrder(t *testing.T) {
	parts, aggregate, partRows := acrRun()
	forward := append([]rowsArtifact{aggregate}, parts...)
	backward := slices.Clone(forward)
	slices.Reverse(backward)
	for name, listing := range map[string][]rowsArtifact{"forward": forward, "backward": backward} {
		walk, _, _ := rowsWalk(t, 1, listing...)
		if got := committedReportRows(walk); got != partRows {
			t.Fatalf("%s: committed %d rows, want %d", name, got, partRows)
		}
		if causes := markerCauses(walk); len(causes) != 1 || causes[aggregate.id] != githubTestsAggregateOfPartsCause {
			t.Fatalf("%s: markers %v", name, causes)
		}
	}
}

// R3: the duplicate test runs before the bound. 80,000 rows of parts plus their 80,000-row aggregate would pass the bound, but the
// aggregate is dropped first, so nothing is skipped as over the bound. The aggregate is declared the smallest, so it comes first in
// admission order and would take the bound if it counted.
func TestGitHubTestsRunReportsDedupRunsBeforeTheBound(t *testing.T) {
	first, second := part("u", 20_000, 20_000), part("v", 20_000, 20_000)
	all := append(append([]fxSuite(nil), first...), second...)
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "61", size: 100, body: fxJUnit(first)}, rowsArtifact{id: "62", size: 200, body: fxJUnit(second)},
		rowsArtifact{id: "63", size: 50, body: fxJUnit(all)})
	want := rowsOfSuites(first) + rowsOfSuites(second)
	if got := committedReportRows(walk); got != want {
		t.Fatalf("committed %d rows, want the two parts (%d)", got, want)
	}
	if observed(walk, githubTestsPerRunCapCause) != 0 {
		t.Fatalf("observations %+v: the dropped aggregate must not count against the bound", walk.cursor.Incomplete)
	}
	if causes := markerCauses(walk); len(causes) != 1 || causes["63"] != githubTestsAggregateOfPartsCause {
		t.Fatalf("markers %v, want only the aggregate", causes)
	}
}

// One artifact repeating a case identity in two suites (two projects) is a single artifact: nothing in it is dropped.
func TestGitHubTestsRunReportsKeepRepeatedIdentitiesInsideOneArtifact(t *testing.T) {
	suites := []fxSuite{
		{name: "chromium", cases: []fxCase{{"login", "passed", ""}, {"logout", "passed", ""}}},
		{name: "firefox", cases: []fxCase{{"login", "passed", ""}, {"logout", "passed", ""}}},
	}
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "71", size: 10, body: fxJUnit(suites)})
	if got := committedReportRows(walk); got != rowsOfSuites(suites) {
		t.Fatalf("committed %d rows, want all %d", got, rowsOfSuites(suites))
	}
	if len(walk.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("markers %+v, want none", walk.cursor.SkippedArtifacts)
	}
}

// Parts that overlap are not a partition: an artifact equal to their union (a case counted twice) is kept, because dropping it would
// need the overlapping case to be one result and the test cannot tell that from two real ones.
func TestGitHubTestsRunReportsKeepAnArtifactWhosePartsOverlap(t *testing.T) {
	shared := fxCase{"shared", "passed", ""}
	left := []fxSuite{{name: "s1", cases: []fxCase{{"a", "passed", ""}, {"b", "passed", ""}, shared}}}
	right := []fxSuite{{name: "s2", cases: []fxCase{shared, {"d", "passed", ""}}}}
	both := []fxSuite{
		{name: "s1", cases: []fxCase{{"a", "passed", ""}, {"b", "passed", ""}, shared}},
		{name: "s2", cases: []fxCase{shared, {"d", "passed", ""}}},
	}
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "81", size: 10, body: fxJUnit(left)}, rowsArtifact{id: "82", size: 20, body: fxJUnit(right)},
		rowsArtifact{id: "83", size: 30, body: fxJUnit(both)})
	if got, want := committedReportRows(walk), rowsOfSuites(left)+rowsOfSuites(right)+rowsOfSuites(both); got != want {
		t.Fatalf("committed %d rows, want all three (%d)", got, want)
	}
	if len(walk.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("markers %+v, want none", walk.cursor.SkippedArtifacts)
	}
}

func withDurations(suites []fxSuite, duration string) []fxSuite {
	out := make([]fxSuite, len(suites))
	for index, suite := range suites {
		out[index] = fxSuite{name: suite.name, ts: suite.ts, cases: append([]fxCase(nil), suite.cases...)}
		for position := range out[index].cases {
			out[index].cases[position].dur = duration
		}
	}
	return out
}

// Legs that agree on every outcome but ran for different times are two real executions: both stay.
func TestGitHubTestsRunReportsKeepIdenticalOutcomeLegsWithDifferentDurations(t *testing.T) {
	base := part("m", 4, 200)
	legA, legB := withDurations(base, "0.1"), withDurations(base, "0.2")
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "91", size: 10, body: fxJUnit(legA)}, rowsArtifact{id: "92", size: 20, body: fxJUnit(legB)})
	if got, want := committedReportRows(walk), rowsOfSuites(legA)+rowsOfSuites(legB); got != want {
		t.Fatalf("committed %d rows, want both legs (%d)", got, want)
	}
	if len(walk.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("markers %+v, want none", walk.cursor.SkippedArtifacts)
	}
}

// A true copy of one execution (same outcomes, durations and suite timestamps) loses its higher id.
func TestGitHubTestsRunReportsDropATrueCopyOfOneExecution(t *testing.T) {
	base := withDurations(part("c", 3, 90), "0.3")
	for index := range base {
		base[index].ts = "2026-11-14T15:04:10Z"
	}
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "93", size: 10, body: fxJUnit(base)}, rowsArtifact{id: "94", size: 20, body: fxJUnit(base)})
	if got, want := committedReportRows(walk), rowsOfSuites(base); got != want {
		t.Fatalf("committed %d rows, want one copy (%d)", got, want)
	}
	if causes := markerCauses(walk); len(causes) != 1 || causes["94"] != githubTestsAggregateOfPartsCause {
		t.Fatalf("markers %v, want only the higher id 94", causes)
	}
}

// Without durations (absent, or all zero) two reports cannot be told from two executions: both stay.
func TestGitHubTestsRunReportsKeepBothWhenThereAreNoDurations(t *testing.T) {
	for name, duration := range map[string]string{"absent": "-", "zero": "0"} {
		base := withDurations(part("z", 3, 90), duration)
		walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "95", size: 10, body: fxJUnit(base)}, rowsArtifact{id: "96", size: 20, body: fxJUnit(base)})
		if got, want := committedReportRows(walk), rowsOfSuites(base)*2; got != want {
			t.Fatalf("%s durations: committed %d rows, want both copies (%d)", name, got, want)
		}
		if len(walk.cursor.SkippedArtifacts) != 0 {
			t.Fatalf("%s durations: markers %+v, want none", name, walk.cursor.SkippedArtifacts)
		}
	}
}

// Same outcomes and durations but different suite timestamps are two executions: both stay.
func TestGitHubTestsRunReportsKeepLegsWithDifferentSuiteTimestamps(t *testing.T) {
	legA, legB := withDurations(part("t", 2, 60), "0.2"), withDurations(part("t", 2, 60), "0.2")
	for index := range legA {
		legA[index].ts, legB[index].ts = "2026-11-14T15:04:10Z", "2026-11-14T15:09:41Z"
	}
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "97", size: 10, body: fxJUnit(legA)}, rowsArtifact{id: "98", size: 20, body: fxJUnit(legB)})
	if got, want := committedReportRows(walk), rowsOfSuites(legA)*2; got != want {
		t.Fatalf("committed %d rows, want both legs (%d)", got, want)
	}
}
