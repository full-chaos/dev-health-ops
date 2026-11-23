package providersync

import (
	"encoding/json"
	"strings"
	"testing"

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
	client := githubTestsClient(t, fakehttp.Client(&githubTestsRowsDoer{t: t, runs: 1, artifacts: artifacts}))
	client.Metrics = providerfoundation.NewMetrics()
	final, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, nativeTestClaim("github", "cicd"), client, 8)
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
	return suites, cases
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
	if len(cases) != 2 {
		t.Errorf("case rows %d, want leg B's 2 parts only", len(cases))
	}
	for _, row := range cases {
		if retractedIDs[row["suite_id"]] {
			t.Errorf("a case row was written under a retracted suite: %v", row)
		}
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

// The retraction rows of a run never take its suite batch past the effect contract: past either bound they are not written.
func TestGitHubTestsRetractionRowsFitTheEffectContract(t *testing.T) {
	row := testSuiteResultRow{OrgID: "org", RepoID: "repo", RunID: "run", SuiteID: "suite", Retracted: true}
	if !githubTestsRetractionsFit(maxEffectRows-2, []testSuiteResultRow{row, row}) {
		t.Errorf("two rows that fit the row bound were refused")
	}
	if githubTestsRetractionsFit(maxEffectRows-1, []testSuiteResultRow{row, row}) {
		t.Errorf("rows past maxEffectRows were accepted")
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	room := (maxEffectPayloadBytes - githubTestsMaxReportPayloadBytesPerDestination) / len(encoded)
	many := make([]testSuiteResultRow, room+1)
	for index := range many {
		many[index] = row
	}
	if room+1 <= maxEffectRows && githubTestsRetractionsFit(0, many) {
		t.Errorf("%d rows of %d bytes past the payload headroom were accepted", len(many), len(encoded))
	}
	if !githubTestsRetractionsFit(0, many[:room]) {
		t.Errorf("%d rows of %d bytes inside the payload headroom were refused", room, len(encoded))
	}
}
