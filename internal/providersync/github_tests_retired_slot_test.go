package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14259: a report the planner drops as an aggregate of its parts may have been stored as its own slot by an earlier sync
// (before CHAOS-14045, or before its parts were listed). The run's batch then carries one zero-measure row per suite of the
// dropped report, keyed as parsed and flagged retracted, so the readers retire the stale slot.

type retiredFxCase struct{ name, status, dur string }

type retiredFxSuite struct {
	name, time string
	cases      []retiredFxCase
}

func retiredFxJUnit(suites ...retiredFxSuite) string {
	var body strings.Builder
	body.WriteString(`<testsuites>`)
	for _, suite := range suites {
		body.WriteString(`<testsuite name="` + suite.name + `" time="` + suite.time + `" timestamp="2026-07-22T10:02:00">`)
		for _, c := range suite.cases {
			if c.status == "failed" {
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg" time="` + c.dur + `"><failure message="boom"/></testcase>`)
			} else {
				body.WriteString(`<testcase name="` + c.name + `" classname="pkg" time="` + c.dur + `"/>`)
			}
		}
		body.WriteString(`</testsuite>`)
	}
	body.WriteString(`</testsuites>`)
	return body.String()
}

// retiredFxMatrix is the CHAOS-14045 matrix: legs A and B run cases a and b, total-a fails case a (a mismatch of leg A), total-b
// is leg B exactly (an aggregate of leg B's two parts). Artifact names are test-results-<id>.
func retiredFxMatrix() map[string]rowsArtifact {
	legA := []retiredFxSuite{{"left", "1.0", []retiredFxCase{{"a", "", "0.125"}}}, {"right", "1.0", []retiredFxCase{{"b", "", "0.125"}}}}
	legB := []retiredFxSuite{{"left", "3.0", []retiredFxCase{{"a", "", "0.25"}}}, {"right", "3.0", []retiredFxCase{{"b", "", "0.25"}}}}
	totalA := []retiredFxSuite{{"left", "1.0", []retiredFxCase{{"a", "failed", "0.125"}}}, legA[1]}
	return map[string]rowsArtifact{
		"1": {id: "1", size: 10, body: retiredFxJUnit(legA[0])}, "2": {id: "2", size: 11, body: retiredFxJUnit(legA[1])},
		"3": {id: "3", size: 12, body: retiredFxJUnit(legB[0])}, "4": {id: "4", size: 13, body: retiredFxJUnit(legB[1])},
		"5": {id: "5", size: 20, body: retiredFxJUnit(totalA...)}, "6": {id: "6", size: 21, body: retiredFxJUnit(legB...)},
	}
}

// retiredWalk walks one run with the listed artifacts and returns its test_suite_results and test_case_results rows as JSON
// objects, in emission order.
func retiredWalk(t *testing.T, artifacts ...rowsArtifact) (suites, cases []map[string]any) {
	t.Helper()
	suites, cases, _ = retiredWalkWith(t, GitHubTestsRouteHandler{}, artifacts...)
	return suites, cases
}

// retiredWalkWith is retiredWalk through handler; it also returns the rendered provider metrics of the walk.
func retiredWalkWith(t *testing.T, handler GitHubTestsRouteHandler, artifacts ...rowsArtifact) (suites, cases []map[string]any, metrics string) {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(&githubTestsRowsDoer{t: t, runs: 1, artifacts: artifacts}))
	client.Metrics = providerfoundation.NewMetrics()
	final, effects := walkGitHubTestsChunksCapturingEffects(t, handler, nativeTestClaim("github", "cicd"), client, 8)
	for _, effect := range append(effects, final.Effects...) {
		var into *[]map[string]any
		switch effect.Destination {
		case "test_suite_results":
			into = &suites
		case "test_case_results":
			into = &cases
		default:
			continue
		}
		for _, raw := range effect.Rows {
			row := map[string]any{}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			*into = append(*into, row)
		}
	}
	var rendered strings.Builder
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	return suites, cases, rendered.String()
}

func retiredFxListing(ids ...string) []rowsArtifact {
	matrix := retiredFxMatrix()
	out := make([]rowsArtifact, 0, len(ids))
	for _, id := range ids {
		out = append(out, matrix[id])
	}
	return out
}

// The dropped aggregate (total-b, artifact 6) gets one zero-measure retracted row per parsed suite in its run's suite batch, at the
// keys its own rows would have had; it gets no case row.
func TestGitHubTestsRouteRetractsTheSlotOfADroppedAggregate(t *testing.T) {
	suites, cases := retiredWalk(t, retiredFxListing("3", "4", "6")...)
	var live, retracted []map[string]any
	for index, row := range suites {
		if row["retracted"] == true {
			retracted = append(retracted, row)
			continue
		}
		if _, present := row["retracted"]; present {
			t.Errorf("live suite row %d carries retracted=%v: a live row keeps its JSON (and effect digest)", index, row["retracted"])
		}
		live = append(live, row)
	}
	if len(live) != 2 || len(retracted) != 2 {
		t.Fatalf("suite rows: %d live, %d retracted; want leg B's 2 parts live and total-b's 2 suites retracted:\n%v", len(live), len(retracted), suites)
	}
	names := map[string]bool{}
	for _, row := range retracted {
		names[row["suite_name"].(string)] = true
		if row["report_slot"] != "test-results-6" || row["run_attempt"] != float64(1) {
			t.Errorf("retracted row slot %v attempt %v, want test-results-6 attempt 1", row["report_slot"], row["run_attempt"])
		}
		for _, measure := range []string{"total_count", "passed_count", "failed_count", "skipped_count", "error_count", "quarantined_count", "retried_count"} {
			if row[measure] != float64(0) {
				t.Errorf("retracted row %s = %v, want 0", measure, row[measure])
			}
		}
		if row["duration_seconds"] != nil {
			t.Errorf("retracted row duration_seconds = %v, want null", row["duration_seconds"])
		}
		if row["started_at"] == nil {
			t.Errorf("retracted row has no started_at: it must keep the parsed day placement")
		}
		if ids, _ := row["legacy_suite_ids"].([]any); len(ids) == 0 {
			t.Errorf("retracted row lists no legacy suite id: it must keep the parsed legacy ids")
		}
	}
	if !names["left"] || !names["right"] {
		t.Errorf("retracted suite names %v, want left and right", names)
	}
	retractedIDs := map[any]bool{}
	for _, row := range retracted {
		retractedIDs[row["suite_id"]] = true
	}
	for _, row := range live {
		if retractedIDs[row["suite_id"]] {
			t.Errorf("a retracted row shares the key of a live row: %v", row["suite_id"])
		}
	}
	// Its cases get a row each at their keys under the report's suites (attempt 1 is the slot's only label): no name, no status,
	// retracted. A live case row is never under a retracted suite.
	liveCases, retractedCases := 0, 0
	for _, row := range cases {
		if row["retracted"] == true {
			retractedCases++
			if !retractedIDs[row["suite_id"]] || row["case_name"] != "" || row["status"] != "" || row["failure_message"] != nil {
				t.Errorf("case retraction row %v, want one under a retracted suite with no name, status or failure text", row)
			}
			continue
		}
		liveCases++
		if retractedIDs[row["suite_id"]] {
			t.Errorf("a live case row was written under a retracted suite: %v", row)
		}
	}
	if liveCases != 2 || retractedCases != 2 {
		t.Errorf("case rows: %d live, %d retracted; want leg B's 2 parts and total-b's 2 cases retracted", liveCases, retractedCases)
	}
}

// The case retraction rows of an earlier attempt sit at the keys the parser gives the same report under that attempt's label:
// for a slot at attempt 3, the parsed keys under labels 1, 2 and 3 (the case key re-keyed per label, its ordinal kept).
func TestGitHubTestsCaseRetractionRowsCoverEveryAttemptLabel(t *testing.T) {
	body := retiredFxJUnit(retiredFxSuite{"left", "3.0", []retiredFxCase{{"a", "", "0.25"}, {"a", "", "0.5"}}}, retiredFxSuite{"left", "3.0", []retiredFxCase{{"b", "", "0.25"}}})
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	parse := func(attempt int64) githubTestsReportRows {
		rows, err := parseGitHubTestsReportArtifact(githubTestsZip(t, map[string]string{"junit.xml": body}), "6", githubTestsReportSlot{Name: "total-b", Attempt: attempt},
			nil, "c7198fbc-1945-3717-05d8-eb78866b4e79", "1", "org-acme", nil, nil, at)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	current := parse(3)
	rows := githubTestsCaseRetractionRows(githubTestsRetirement{
		parsed:  []githubTestsParsedReport{{Suites: current.Suites, Cases: current.Cases}},
		dropped: []githubTestsReportDrop{{Index: 0, Cause: githubTestsAggregateOfPartsCause}},
	})
	want := map[string]bool{}
	for _, attempt := range []int64{1, 2, 3} {
		for _, row := range parse(attempt).Cases {
			want[row.SuiteID+"/"+row.CaseID] = true
		}
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row.SuiteID+"/"+row.CaseID] = true
		if !row.Retracted || row.CaseName != "" || row.Status != "" {
			t.Errorf("case retraction row %+v, want retracted with no name or status", row)
		}
	}
	if len(rows) != 9 || !maps.Equal(got, want) {
		t.Errorf("%d case retraction rows at %v, want the 9 parsed keys of labels 1-3 %v", len(rows), got, want)
	}
}

// K12: only a planner drop writes retraction rows. A mismatched total (total-a: its parts are leg A but it fails case a) is kept
// with its parts and marked, an artifact over the run bound is skipped whole: neither retires a slot, since a later sync may still
// hold their rows. (An expired or unreadable artifact is never parsed, so it is never in the plan.)
func TestGitHubTestsRouteRetractsOnlyAPlannerDrop(t *testing.T) {
	suites, _ := retiredWalk(t, retiredFxListing("1", "2", "5")...)
	for _, row := range suites {
		if _, present := row["retracted"]; present {
			t.Errorf("a mismatched total wrote a retraction row: %v", row)
		}
	}
	if len(suites) != 4 {
		t.Errorf("suite rows %d, want leg A's 2 parts and total-a's 2 suites, all live", len(suites))
	}
	first, second := part("ob1", 1, 60_000), part("ob2", 1, 60_000)
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "71", size: 10, body: fxJUnit(first)}, rowsArtifact{id: "72", size: 20, body: fxJUnit(second)})
	if observed(walk, githubTestsPerRunCapCause) != 1 {
		t.Fatalf("observations %+v, want artifact 72 over the run bound", walk.cursor.Incomplete)
	}
	overBound, _ := retiredWalk(t, rowsArtifact{id: "71", size: 10, body: fxJUnit(first)}, rowsArtifact{id: "72", size: 20, body: fxJUnit(second)})
	for _, row := range overBound {
		if _, present := row["retracted"]; present {
			t.Fatalf("an artifact over the run bound wrote a retraction row: %v", row)
		}
	}
	if len(overBound) != 1 {
		t.Errorf("suite rows %d, want artifact 71's one suite", len(overBound))
	}
}

// When one row per suite does not fit the run's suite batch, the dropped report's slot is still retired in the same walk: one
// minimal row (a retired-slot key, no name, team or service), a counter and a Warn line, and the unit finishes. A walk with the room
// of the effect contract writes one row per suite again.
func TestGitHubTestsRouteRetiresTheSlotWithOneRowWhenTheSuiteRowsDoNotFit(t *testing.T) {
	records := captureMembershipLogs(t)
	// Leg B's two parts are admitted (2 suites); total-b's 2 retraction rows would make 4, the room is 3.
	limited := GitHubTestsRouteHandler{retractionRoom: githubTestsRetractionRoom{rows: 3}}
	suites, _, metrics := retiredWalkWith(t, limited, retiredFxListing("3", "4", "6")...)
	var retracted []map[string]any
	for _, row := range suites {
		if row["retracted"] == true {
			retracted = append(retracted, row)
		}
	}
	if len(suites) != 3 || len(retracted) != 1 {
		t.Fatalf("suite rows %d (%d retracted), want leg B's 2 parts and 1 minimal retraction row:\n%v", len(suites), len(retracted), suites)
	}
	row := retracted[0]
	if row["report_slot"] != "test-results-6" || row["suite_name"] != "" || row["framework"] != nil || row["service_id"] != nil ||
		row["total_count"] != float64(0) || row["duration_seconds"] != nil || !strings.HasPrefix(row["suite_id"].(string), "~retired-") {
		t.Errorf("minimal retraction row %v, want slot test-results-6 at a retired-slot key, no name, framework or service, zero measures", row)
	}
	want := `dev_health_provider_report_retraction_limited_total{provider="github",reason="one_row_per_slot"} 1`
	if !strings.Contains(metrics, want) {
		t.Errorf("want %q in:\n%s", want, metrics)
	}
	warned := 0
	for _, record := range *records {
		if record.Message != "dropped report retraction rows limited to fit the run's batch" {
			continue
		}
		warned++
		attrs := membershipLogAttrs(record)
		if record.Level != slog.LevelWarn || attrs["slots"] != int64(1) || attrs["provider"] != "github" || attrs["reason"] != "one_row_per_slot" {
			t.Errorf("limit line level %s attrs %v, want WARN with slots 1", record.Level, attrs)
		}
		for key, value := range attrs {
			if _, isError := value.(error); isError {
				t.Errorf("limit line carries an error operand %s", key)
			}
		}
	}
	if warned != 1 {
		t.Errorf("%d limit lines, want 1", warned)
	}

	full, _, metrics := retiredWalkWith(t, GitHubTestsRouteHandler{}, retiredFxListing("3", "4", "6")...)
	retracted = nil
	for _, row := range full {
		if row["retracted"] == true {
			retracted = append(retracted, row)
		}
	}
	if len(retracted) != 2 || strings.Contains(metrics, "dev_health_provider_report_retraction_limited_total{") {
		t.Errorf("a walk with the contract's room wrote %d retraction rows and limited %t, want 2 and no limit", len(retracted),
			strings.Contains(metrics, "dev_health_provider_report_retraction_limited_total{"))
	}
}

// The minimal rows always fit: the run bound keeps one row of the effect contract per listed artifact free, and the largest
// minimal row set (every listed artifact but one dropped, each row listing every artifact as a legacy id) is far inside the
// payload headroom.
func TestGitHubTestsMinimalRetractionRowsAlwaysFitTheEffectContract(t *testing.T) {
	if githubTestsMaxReportRowsPerRun+githubTestsMaxArtifacts-1 > maxEffectRows {
		t.Fatalf("run bound %d + %d minimal rows passes maxEffectRows %d", githubTestsMaxReportRowsPerRun, githubTestsMaxArtifacts-1, maxEffectRows)
	}
	legacy := make([]string, githubTestsMaxArtifacts)
	for index := range legacy {
		legacy[index] = strings.Repeat("f", 64)
	}
	team, service := strings.Repeat("t", 256), strings.Repeat("s", 256)
	parsed := make([]githubTestsParsedReport, githubTestsMaxArtifacts)
	var dropped []githubTestsReportDrop
	for index := range parsed {
		long := strings.Repeat("n", 1<<20) // a suite name of 1 MiB: the minimal row drops it
		parsed[index].Suites = []testSuiteResultRow{{OrgID: strings.Repeat("o", 64), RepoID: strings.Repeat("r", 36), RunID: strings.Repeat("9", 20),
			SuiteID: strings.Repeat("a", 64), SuiteName: long, TeamID: &team, ServiceID: &service, ReportSlot: strings.Repeat("x", 256),
			RunAttempt: 1, LegacySuiteIDs: legacy}}
		if index > 0 {
			dropped = append(dropped, githubTestsReportDrop{Index: index, Cause: githubTestsAggregateOfPartsCause})
		}
	}
	rows := githubTestsMinimalRetractionRows(githubTestsRetirement{parsed: parsed, dropped: dropped})
	if len(rows) != len(dropped) {
		t.Fatalf("%d minimal rows, want one per dropped report (%d)", len(rows), len(dropped))
	}
	size := 0
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		size += len(encoded)
	}
	if githubTestsMaxReportRowsPerRun+len(rows) > maxEffectRows || size > maxEffectPayloadBytes-githubTestsMaxReportPayloadBytesPerDestination {
		t.Fatalf("%d minimal rows of %d bytes do not fit the room left by a full run", len(rows), size)
	}
	if full := githubTestsRetractionRows(parsed[:2], dropped[:1]); !(githubTestsRetractionRoom{}).fits(2, full) {
		t.Fatalf("one full row of a small run does not fit the room")
	}
}

// The bound on the rows a run holds before planning stays 2 x 100,000 = 200,000: the retraction reserve of the admitted
// bound (CHAOS-14259) must not refuse a report the planner would keep after deduplication.
func TestGitHubTestsHeldRowBoundKeepsItsBaseValue(t *testing.T) {
	copyOf := func(prefix string, cases int) string { return fxJUnit(part(prefix, 1, cases)) }
	// 199,951 held: three exact copies of 49,988 rows and an independent report of 49,987. After deduplication 99,975 are
	// admitted, the run bound exactly.
	copies := copyOf("copy", 49_987)
	walk, _, _ := rowsWalk(t, 1, rowsArtifact{id: "81", size: 10, body: copies}, rowsArtifact{id: "82", size: 11, body: copies},
		rowsArtifact{id: "83", size: 12, body: copies}, rowsArtifact{id: "84", size: 13, body: copyOf("independent", 49_986)})
	if got := committedReportRows(walk); got != 99_975 || observed(walk, githubTestsPerRunCapCause) != 0 {
		t.Errorf("199,951 held rows: committed %d, per_run_cap %d; want 99,975 and none (the independent report kept)",
			got, observed(walk, githubTestsPerRunCapCause))
	}
	// 200,000 held: four exact copies of 50,000 rows deduplicate to one.
	copies = copyOf("four", 49_999)
	walk, _, _ = rowsWalk(t, 1, rowsArtifact{id: "91", size: 10, body: copies}, rowsArtifact{id: "92", size: 11, body: copies},
		rowsArtifact{id: "93", size: 12, body: copies}, rowsArtifact{id: "94", size: 13, body: copies})
	if got := committedReportRows(walk); got != 50_000 || observed(walk, githubTestsPerRunCapCause) != 0 {
		t.Errorf("200,000 held rows: committed %d, per_run_cap %d; want 50,000 and none (all four deduplicated)",
			got, observed(walk, githubTestsPerRunCapCause))
	}
	if githubTestsMaxHeldReportRowsPerRun != 2*maxEffectRows {
		t.Errorf("held-row bound %d, want 2 x maxEffectRows = %d", githubTestsMaxHeldReportRowsPerRun, 2*maxEffectRows)
	}
}

// retiredLetters is n deterministic letters: a field that neither compresses nor needs XML escaping.
func retiredLetters(n int) string {
	samples := rand.NewPCG(14259, 4)
	out := make([]byte, n)
	for index := range out {
		out[index] = byte('a' + samples.Uint64()%26)
	}
	return string(out)
}

// The retraction rows join the run's suite batch, whose rows the chunk split pairs with the case rows at the same offsets. A
// full retraction row that carries a large field and lands next to a large case must not fail the unit at chunk preparation:
// the route then retires the slot with a minimal row that sorts after every live suite row and holds no large field.
// Input (round r1-6906 finding 4): leg A's right case b fails with a 1.1 MiB failure message (total-a carries the same case), and leg B's
// right suite and total-b's copy of it name a 1.1 MiB service path.
func TestGitHubTestsRetractionRowsDoNotBreakTheChunkSplit(t *testing.T) {
	large := retiredLetters(1_153_434)
	legA := []retiredFxSuite{{"left", "1.0", []retiredFxCase{{"a", "", "0.125"}}}, {"right", "1.0", []retiredFxCase{{"b", "failed", "0.125"}}}}
	legB := []retiredFxSuite{{"left", "3.0", []retiredFxCase{{"a", "", "0.25"}}}, {"right", "3.0", []retiredFxCase{{"b", "", "0.25"}}}}
	totalA := []retiredFxSuite{{"left", "1.0", []retiredFxCase{{"a", "failed", "0.125"}}}, legA[1]}
	body := func(suites ...retiredFxSuite) string {
		junit := retiredFxJUnit(suites...)
		junit = strings.ReplaceAll(junit, `<testcase name="b" classname="pkg" time="0.125"><failure message="boom"/>`,
			`<testcase name="b" classname="pkg" time="0.125"><failure message="`+large+`"/>`)
		junit = strings.ReplaceAll(junit, `<testsuite name="right" time="3.0"`, `<testsuite name="right" time="3.0" file="services/`+large+`/b_test.go"`)
		return junit
	}
	artifacts := []rowsArtifact{
		{id: "1", size: 10, body: body(legA[0])}, {id: "2", size: 11, body: body(legA[1])},
		{id: "3", size: 12, body: body(legB[0])}, {id: "4", size: 13, body: body(legB[1])},
		{id: "5", size: 20, body: body(totalA...)}, {id: "6", size: 21, body: body(legB...)},
	}
	client := githubTestsClient(t, fakehttp.Client(&githubTestsRowsDoer{t: t, runs: 1, artifacts: artifacts}))
	client.Metrics = providerfoundation.NewMetrics()
	var emissions []CompleteRouteBatch
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
			emissions = append(emissions, emission.Batch)
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	retired, largeSuites, largeCases := 0, 0, 0
	for index, batch := range emissions {
		if len(batch.Effects) == 0 {
			continue
		}
		if _, err := splitCompleteRouteBatch(CompleteRouteBatch{Effects: batch.Effects}, ShadowComparison{}, TestOpsRouteChunkPolicy()); err != nil {
			t.Errorf("emission %d does not split into prepared chunks: %v", index, err)
		}
		for _, effect := range batch.Effects {
			for _, raw := range effect.Rows {
				row := map[string]any{}
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				switch {
				case effect.Destination == "test_suite_results" && row["retracted"] == true && row["report_slot"] == "test-results-6":
					retired++
				case effect.Destination == "test_suite_results" && len(raw) > len(large):
					largeSuites++
				case effect.Destination == "test_case_results" && len(raw) > len(large):
					largeCases++
				}
			}
		}
	}
	if retired == 0 || largeSuites != 1 || largeCases != 2 {
		t.Errorf("retraction rows of total-b %d, large live suites %d, large cases %d; want total-b retired, 1 and 2 (the fixture's large rows)",
			retired, largeSuites, largeCases)
	}
}

// A minimal retraction row takes a prepared chunk of its own, after every other row of the run: it cannot break a chunk the run's
// own rows fit, even next to a case row at the bound (round r1-6906: no "not_retired" fallback). When the run's own rows do not
// split, chunk preparation refuses the run whatever it holds, and the minimal row is kept.
func TestGitHubTestsMinimalRetractionRowsTakeTheirOwnChunk(t *testing.T) {
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	suite := func(id, slot string) testSuiteResultRow {
		return testSuiteResultRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", SuiteID: id, SuiteName: "s",
			TotalCount: 1, PassedCount: 1, LastSynced: at, ReportSlot: slot, RunAttempt: 1}
	}
	caseRow := func(id, message string) testCaseResultRow {
		return testCaseResultRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", SuiteID: "a1", CaseID: id, CaseName: id,
			Status: "failed", FailureMessage: &message, LastSynced: at}
	}
	retirement := githubTestsRetirement{
		parsed:  []githubTestsParsedReport{{Suites: []testSuiteResultRow{suite("a1", "kept")}}, {Suites: []testSuiteResultRow{suite("b1", "dropped")}}},
		dropped: []githubTestsReportDrop{{Index: 1, Cause: githubTestsAggregateOfPartsCause}},
	}
	// The largest case message the run's own rows still split with (searched down in 64-byte steps).
	letters := retiredLetters(githubTestsRetractionSplitPolicy().MaxPreparedBytes)
	near := ""
	for size := len(letters); size > 0 && near == ""; size -= 64 {
		base, err := testOpsEffects(nil, nil, nil, []testSuiteResultRow{suite("a1", "kept")}, []testCaseResultRow{caseRow("c0", "x"), caseRow("c1", letters[:size])}, nil)
		if err == nil && githubTestsEffectsSplit(base) {
			near = letters[:size]
		}
	}
	effects, limited, err := githubTestsRunReportEffects(githubTestsRetractionRoom{}, retirement, []testSuiteResultRow{suite("a1", "kept")},
		[]testCaseResultRow{caseRow("c0", "x"), caseRow("c1", near)}, nil, nil)
	if err != nil || limited != "one_row_per_slot" {
		t.Fatalf("a slot next to a near-bound case: limited %q (%v), want the minimal row (one_row_per_slot)", limited, err)
	}
	chunks, err := splitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, ShadowComparison{}, githubTestsRetractionSplitPolicy())
	if err != nil {
		t.Fatalf("the run with its minimal row does not split: %v", err)
	}
	own := 0
	for _, chunk := range chunks {
		minimal, others := 0, 0
		for _, effect := range chunk.Effects {
			for _, row := range effect.Rows {
				if strings.Contains(string(row), `"suite_id":"~retired-`) {
					minimal++
				} else if effect.Destination != testOpsRunReportCompletenessDestination {
					others++
				}
			}
		}
		if minimal > 0 {
			own++
			if others > 0 {
				t.Errorf("the minimal row shares a chunk with %d report rows", others)
			}
		}
	}
	if own != 1 {
		t.Errorf("%d chunks hold the minimal row, want 1", own)
	}
	over := retiredLetters(githubTestsRetractionSplitPolicy().MaxPreparedBytes + 10)
	effects, limited, err = githubTestsRunReportEffects(githubTestsRetractionRoom{}, retirement, []testSuiteResultRow{suite("a1", "kept")},
		[]testCaseResultRow{caseRow("c0", over)}, nil, nil)
	retracted := 0
	for _, effect := range effects {
		if effect.Destination == "test_suite_results" {
			retracted = len(effect.Rows) - 1
		}
	}
	if err != nil || limited != "one_row_per_slot" || retracted != 1 {
		t.Errorf("a run whose own rows do not split: limited %q, %d retraction rows (%v); want the minimal row kept", limited, retracted, err)
	}
}

// retiredLargeMatrix is a run whose dropped aggregate is large: two parts of 25,001 cases each and their exact aggregate
// (50,002 cases). The admitted rows (2 suites, 50,002 cases) fit the run bound; the case retraction rows (50,002 more) would take
// the case batch past maxEffectRows (100,004 rows), so the run takes the "suite_rows_only" fallback.
func retiredLargeMatrix() []rowsArtifact {
	suite := func(name string) retiredFxSuite {
		out := retiredFxSuite{name: name, time: "3.0"}
		for index := 0; index < 25_001; index++ {
			out.cases = append(out.cases, retiredFxCase{name + "-" + strconv.Itoa(index), "", "0.01"})
		}
		return out
	}
	left, right := suite("left"), suite("right")
	return []rowsArtifact{
		{id: "1", size: 10, body: retiredFxJUnit(left)}, {id: "2", size: 11, body: retiredFxJUnit(right)},
		{id: "3", size: 20, body: retiredFxJUnit(left, right)},
	}
}

// The "suite_rows_only" fallback: when the case retraction rows do not fit the case batch, the suite rows alone are written
// (the slot is retired for the readers; an old reader after a rollback still sees the slot's stale cases), with the counter
// and a Warn line that name the reason.
func TestGitHubTestsRouteWritesSuiteRowsOnlyWhenTheCaseRowsDoNotFit(t *testing.T) {
	records := captureMembershipLogs(t)
	suites, cases, metrics := retiredWalkWith(t, GitHubTestsRouteHandler{}, retiredLargeMatrix()...)
	retractedSuites, retractedCases := 0, 0
	for _, row := range suites {
		if row["retracted"] == true {
			retractedSuites++
		}
	}
	for _, row := range cases {
		if row["retracted"] == true {
			retractedCases++
		}
	}
	if retractedSuites != 2 || retractedCases != 0 || len(cases) != 50_002 {
		t.Fatalf("retraction rows: %d suite, %d case; %d case rows; want the aggregate's 2 suites, no case row, the parts' 50,002 cases",
			retractedSuites, retractedCases, len(cases))
	}
	if want := `dev_health_provider_report_retraction_limited_total{provider="github",reason="suite_rows_only"} 1`; !strings.Contains(metrics, want) {
		t.Errorf("want %q in the metrics", want)
	}
	warned := 0
	for _, record := range *records {
		if record.Message != "dropped report retraction rows limited to fit the run's batch" {
			continue
		}
		warned++
		attrs := membershipLogAttrs(record)
		if record.Level != slog.LevelWarn || attrs["reason"] != "suite_rows_only" || attrs["slots"] != int64(1) {
			t.Errorf("limit line level %s attrs %v, want WARN, reason suite_rows_only, 1 slot", record.Level, attrs)
		}
		for key, value := range attrs {
			if _, isError := value.(error); isError {
				t.Errorf("limit line carries an error operand %s", key)
			}
		}
	}
	if warned != 1 {
		t.Errorf("%d limit lines, want 1", warned)
	}
	row, err := json.Marshal(testCaseResultRow{RepoID: strings.Repeat("r", 36), RunID: strings.Repeat("9", 11), SuiteID: strings.Repeat("a", 64),
		CaseID: strings.Repeat("b", 64), OrgID: strings.Repeat("o", 36), LastSynced: time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), Retracted: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("one case retraction row is %d bytes", len(row))
}

// CHAOS-14555: a minimal retraction row lists the legacy ids of every suite of the dropped report (deduplicated, in parse
// order), so a pre-slot copy of any of its suites is hidden with the slot, not only the first suite's.
func TestGitHubTestsMinimalRetractionRowsListEveryDroppedSuitesLegacyIDs(t *testing.T) {
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	suite := func(id string, legacy ...string) testSuiteResultRow {
		return testSuiteResultRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", SuiteID: id, SuiteName: id,
			LastSynced: at, ReportSlot: "total-b", RunAttempt: 1, LegacySuiteIDs: legacy}
	}
	rows := githubTestsMinimalRetractionRows(githubTestsRetirement{
		parsed: []githubTestsParsedReport{{Suites: []testSuiteResultRow{
			suite("left", "legacy-left", "legacy-shared"), suite("right", "legacy-right", "legacy-shared"),
		}}},
		dropped: []githubTestsReportDrop{{Index: 0, Cause: githubTestsAggregateOfPartsCause}},
	})
	if len(rows) != 1 || !slices.Equal(rows[0].LegacySuiteIDs, []string{"legacy-left", "legacy-shared", "legacy-right"}) {
		t.Fatalf("minimal rows %+v, want one row listing every suite's legacy ids once, in parse order", rows)
	}
}

// A large union is split into rows of at most 16,384 ids, each keyed with the retired-slot prefix (its own chunks): 20,000 ids
// make 2 rows, the first at the slot's retired key.
func TestGitHubTestsMinimalRetractionRowsSplitALargeLegacyUnion(t *testing.T) {
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	suites := make([]testSuiteResultRow, 20_000)
	for index := range suites {
		id := fmt.Sprintf("suite-%05d", index)
		suites[index] = testSuiteResultRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", SuiteID: id, SuiteName: id,
			LastSynced: at, ReportSlot: "total-b", RunAttempt: 2, LegacySuiteIDs: []string{"legacy-" + id}}
	}
	rows := githubTestsMinimalRetractionRows(githubTestsRetirement{
		parsed:  []githubTestsParsedReport{{Suites: suites}},
		dropped: []githubTestsReportDrop{{Index: 0, Cause: githubTestsAggregateOfPartsCause}},
	})
	if len(rows) != 2 || len(rows[0].LegacySuiteIDs) != 16_384 || len(rows[1].LegacySuiteIDs) != 20_000-16_384 {
		t.Fatalf("%d minimal rows, want 2 of 16,384 and 3,616 legacy ids", len(rows))
	}
	if rows[0].SuiteID != githubTestsRetiredSlotKey("total-b", 2) || rows[1].SuiteID == rows[0].SuiteID ||
		!strings.HasPrefix(rows[1].SuiteID, "~retired-") || !rows[1].Retracted {
		t.Errorf("minimal row keys %q and %q, want the slot's retired key, then another retired key", rows[0].SuiteID, rows[1].SuiteID)
	}
	for _, row := range rows {
		effect, err := effectBatchFromValues("test_suite_results", EffectReadbackRequired, []testSuiteResultRow{row})
		if err != nil {
			t.Fatal(err)
		}
		if _, own := effectOwnChunkRows(effect); len(own) != 1 {
			t.Errorf("minimal row %s does not take a chunk of its own", row.SuiteID)
		}
	}
}

// CHAOS-14555: a dropped report's cases are also neutralised at their pre-slot keys (the artifact-id scope of a build before
// slots, and each superseded artifact's), so an old reader after a rollback drops a stale pre-slot copy of a case too.
func TestGitHubTestsCaseRetractionRowsCoverThePreSlotKeys(t *testing.T) {
	_, cases := retiredWalk(t, retiredFxListing("3", "4", "6")...)
	legB := []retiredFxSuite{{"left", "3.0", []retiredFxCase{{"a", "", "0.25"}}}, {"right", "3.0", []retiredFxCase{{"b", "", "0.25"}}}}
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	archive := githubTestsZip(t, map[string]string{"junit.xml": retiredFxJUnit(legB...)})
	want := map[string]bool{}
	slotted, err := parseGitHubTestsReportArtifact(archive, "6", githubTestsReportSlot{Name: "test-results-6", Attempt: 1}, nil,
		"c7198fbc-1945-3717-05d8-eb78866b4e79", "1", "org-acme", nil, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	preSlot, err := parseGitHubTestsArtifact(archive, "6", "c7198fbc-1945-3717-05d8-eb78866b4e79", "1", "org-acme", nil, nil, at)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range append(slotted.Cases, preSlot.Cases...) {
		want[row.SuiteID+"/"+row.CaseID] = true
	}
	got := map[string]bool{}
	for _, row := range cases {
		if row["retracted"] == true {
			got[row["suite_id"].(string)+"/"+row["case_id"].(string)] = true
		}
	}
	if !maps.Equal(got, want) {
		t.Errorf("case retraction keys %v, want the slotted and the pre-slot keys of total-b's cases %v", got, want)
	}
}

// The case batch holds the slot-label and pre-slot case retraction rows up to 33,333 admitted cases (3 x 33,333 = 99,999 rows);
// at 33,334 the pre-slot rows are skipped (counter and Warn reason legacy_case_rows_skipped) and the slot-label rows stay.
func TestGitHubTestsLegacyCaseRowsFitUpTo33333Cases(t *testing.T) {
	matrix := func(left, right int) []rowsArtifact {
		suite := func(name string, count int) retiredFxSuite {
			out := retiredFxSuite{name: name, time: "3.0"}
			for index := 0; index < count; index++ {
				out.cases = append(out.cases, retiredFxCase{name + "-" + strconv.Itoa(index), "", "0.01"})
			}
			return out
		}
		l, r := suite("left", left), suite("right", right)
		return []rowsArtifact{{id: "1", size: 10, body: retiredFxJUnit(l)}, {id: "2", size: 11, body: retiredFxJUnit(r)}, {id: "3", size: 20, body: retiredFxJUnit(l, r)}}
	}
	for _, cell := range []struct {
		left, right, slotRows, legacyRows int
		limited                           bool
	}{{16_666, 16_667, 33_333, 33_333, false}, {16_667, 16_667, 33_334, 0, true}} {
		suites, cases, metrics := retiredWalkWith(t, GitHubTestsRouteHandler{}, matrix(cell.left, cell.right)...)
		retracted := map[any]bool{}
		for _, row := range suites {
			if row["retracted"] == true {
				retracted[row["suite_id"]] = true
			}
		}
		slotRows, legacyRows := 0, 0
		for _, row := range cases {
			if row["retracted"] != true {
				continue
			}
			if retracted[row["suite_id"]] {
				slotRows++
			} else {
				legacyRows++
			}
		}
		limited := strings.Contains(metrics, `dev_health_provider_report_retraction_limited_total{provider="github",reason="legacy_case_rows_skipped"} 1`)
		if slotRows != cell.slotRows || legacyRows != cell.legacyRows || limited != cell.limited {
			t.Errorf("%d cases: %d slot-label and %d pre-slot case retraction rows, legacy_case_rows_skipped %t; want %d, %d, %t",
				cell.left+cell.right, slotRows, legacyRows, limited, cell.slotRows, cell.legacyRows, cell.limited)
		}
	}
}

// When even the minimal rows with every legacy id do not fit the suite batch (the effect contract's 64 MiB), the slot is retired
// by today's minimal row (the first suite's legacy ids), counted as legacy_ids_capped.
func TestGitHubTestsRunReportEffectsCapsTheMinimalLegacyIDs(t *testing.T) {
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	base := testSuiteResultRow{OrgID: "org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "1", LastSynced: at, ReportSlot: "kept", RunAttempt: 1}
	live := base
	live.SuiteID, live.SuiteName = "live", retiredLetters(60<<20)
	dropped := make([]testSuiteResultRow, 100_000)
	for index := range dropped {
		dropped[index] = base
		dropped[index].ReportSlot = "dropped"
		dropped[index].SuiteID = fmt.Sprintf("d%06d", index)
		dropped[index].SuiteName = dropped[index].SuiteID
		dropped[index].LegacySuiteIDs = []string{fmt.Sprintf("%064d", index)}
	}
	effects, limited, err := githubTestsRunReportEffects(githubTestsRetractionRoom{rows: 1}, githubTestsRetirement{
		parsed:  []githubTestsParsedReport{{Suites: []testSuiteResultRow{live}}, {Suites: dropped}},
		dropped: []githubTestsReportDrop{{Index: 1, Cause: githubTestsAggregateOfPartsCause}},
	}, []testSuiteResultRow{live}, nil, nil, nil)
	if err != nil || limited != "legacy_ids_capped" {
		t.Fatalf("limited %q (%v), want legacy_ids_capped", limited, err)
	}
	for _, effect := range effects {
		if effect.Destination == "test_suite_results" && len(effect.Rows) != 2 {
			t.Errorf("%d suite rows, want the live row and one capped minimal row", len(effect.Rows))
		}
	}
}
