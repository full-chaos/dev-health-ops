package goapiproof

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// This file drives GET /api/v1/people/{person_id}/drilldown/prs' own
// corpus requests through the path go-api-rest-prove's Runner takes
// (DecodeRESTSnapshot, InjectRESTDedupKeys with the request's own dedup
// declaration, Compare with the request's own Parity) against response
// bodies captured from a production proof run. In every captured
// baseline, 9 pull requests arrive as 17 or 18 rows: git_pull_requests
// and repos both hold unmerged physical versions, so the reference's
// unFINALed join repeats each row 2 or 4 times. One pull request
// (d29d160a.../916) carries two physical versions whose title differs
// (the upstream title was edited between two syncs), each repeated by
// the repos fan-out; the candidate carries one of the two titles.
//
// Request cases share bodies: limit_above_ceiling returned the default
// case's own baseline and candidate; limit_zero_falls_back_to_default
// returned the default candidate and a baseline carrying the same rows
// with the two 916 title variants in the opposite physical order.
type personDrilldownPRsRetitleCase struct {
	request   string
	baseline  string
	candidate string
}

var personDrilldownPRsRetitleCases = []personDrilldownPRsRetitleCase{
	{"drilldown_prs_default", "testdata/persondrilldownprs_retitle_baseline_default_6dc142e6.json", "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json"},
	{"valid_cursor", "testdata/persondrilldownprs_retitle_baseline_cursor_f02ed0d8.json", "testdata/persondrilldownprs_retitle_candidate_cursor_d86755f6.json"},
	{"limit_above_ceiling", "testdata/persondrilldownprs_retitle_baseline_default_6dc142e6.json", "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json"},
	{"limit_zero_falls_back_to_default", "testdata/persondrilldownprs_retitle_baseline_limitzero_9aeed1f7.json", "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json"},
}

const personDrilldownPRsOperation = "REST:GET:/api/v1/people/{person_id}/drilldown/prs"

// personDrilldownPRsRequest reads one request's own declaration from the
// live corpus, never a hand-copied Options value.
func personDrilldownPRsRequest(t *testing.T, name string) RESTRequest {
	t.Helper()
	spec, err := SpecForREST(personDrilldownPRsOperation)
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	for _, req := range spec.Requests {
		if req.Name == name {
			return req
		}
	}
	t.Fatalf("request %q not found", name)
	return RESTRequest{}
}

// compareAsRunner decodes both bodies, injects the request's own dedup
// keys and compares under the request's own Parity.
func compareAsRunner(t *testing.T, req RESTRequest, baselineBody, candidateBody []byte) Result {
	t.Helper()
	baseline, err := DecodeRESTSnapshot(baselineBody)
	if err != nil {
		t.Fatalf("decode baseline: %v", err)
	}
	candidate, err := DecodeRESTSnapshot(candidateBody)
	if err != nil {
		t.Fatalf("decode candidate: %v", err)
	}
	InjectRESTDedupKeys(baseline.Data, req.DedupListPath, req.DedupKeyFields)
	InjectRESTDedupKeys(candidate.Data, req.DedupListPath, req.DedupKeyFields)
	return Compare(baseline, candidate, req.Parity)
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return body
}

// TestPersonDrilldownPRs_CapturedRetitleCasesHaveNothingOutside pins the
// corpus verdict on every captured case: the length finding and every
// other finding is admitted by a declared mechanism.
func TestPersonDrilldownPRs_CapturedRetitleCasesHaveNothingOutside(t *testing.T) {
	for _, c := range personDrilldownPRsRetitleCases {
		t.Run(c.request, func(t *testing.T) {
			req := personDrilldownPRsRequest(t, c.request)
			result := compareAsRunner(t, req, readFixture(t, c.baseline), readFixture(t, c.candidate))
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			if !hasFinding(result, "$.data.items", ShapeLength) {
				t.Fatalf("no $.data.items length finding -- the fixture no longer exercises the length admission: %+v", result.Findings)
			}
		})
	}
}

// retitleItems decodes a fixture's items list for mutation.
func retitleItems(t *testing.T, path string) (map[string]any, []any) {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(readFixture(t, path), &body); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	items, ok := body["items"].([]any)
	if !ok {
		t.Fatalf("%s: items is not a list", path)
	}
	return body, items
}

func encodeBody(t *testing.T, body map[string]any) []byte {
	t.Helper()
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return out
}

func isRetitledPullRequest(item any) bool {
	row, ok := item.(map[string]any)
	return ok && row["number"] == float64(916) && strings.HasPrefix(row["repo_id"].(string), "d29d160a")
}

// TestPersonDrilldownPRs_CapturedRetitleRefusesEachNeighbouringCase keeps
// every refusal the copy rule declares on the captured default case: the
// family's candidate accounting refuses the list, so exactly two findings
// stay outside -- the list's length and next_cursor's rendering, which is
// admitted only on an accounted list.
func TestPersonDrilldownPRs_CapturedRetitleRefusesEachNeighbouringCase(t *testing.T) {
	const basePath = "testdata/persondrilldownprs_retitle_baseline_default_6dc142e6.json"
	const candPath = "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json"
	req := personDrilldownPRsRequest(t, "drilldown_prs_default")

	cases := []struct {
		name          string
		mutateBase    func(items []any)
		mutateCand    func(items []any)
		wantLengthOut bool
	}{
		{"candidate title matches neither physical copy", nil, func(items []any) {
			for _, item := range items {
				if isRetitledPullRequest(item) {
					item.(map[string]any)["title"] = "ABC-123 a title neither plane stored"
				}
			}
		}, true},
		{"copies differ in created_at", func(items []any) {
			for _, item := range items {
				if isRetitledPullRequest(item) {
					item.(map[string]any)["created_at"] = "2024-01-01T00:00:00"
					return
				}
			}
		}, nil, true},
		{"candidate mixes the merged_at of one copy with the title of the other", func(items []any) {
			for _, item := range items {
				row, ok := item.(map[string]any)
				if ok && isRetitledPullRequest(item) && strings.HasSuffix(row["title"].(string), "all-actions group with 2 updates") {
					row["merged_at"] = "2024-01-02T00:00:00"
				}
			}
		}, func(items []any) {
			for _, item := range items {
				if isRetitledPullRequest(item) {
					item.(map[string]any)["merged_at"] = "2024-01-02T00:00:00Z"
				}
			}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseBody, baseItems := retitleItems(t, basePath)
			candBody, candItems := retitleItems(t, candPath)
			if c.mutateBase != nil {
				c.mutateBase(baseItems)
			}
			if c.mutateCand != nil {
				c.mutateCand(candItems)
			}
			result := compareAsRunner(t, req, encodeBody(t, baseBody), encodeBody(t, candBody))
			if result.DifferencesOutsideBaselineDefect != 2 {
				t.Fatalf("outside = %d, want exactly 2 (length and cursor) -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			if !hasFinding(result, "$.data.items", ShapeLength) || !hasFinding(result, "$.data.next_cursor", ShapeValue) {
				t.Fatalf("the outside findings are not $.data.items' length and $.data.next_cursor: %+v", result.Findings)
			}
		})
	}
}

func hasFinding(result Result, path, shape string) bool {
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && f.Path == path && f.Shape == shape {
			return true
		}
	}
	return false
}

// TestPersonDrilldownPRs_CapturedRetitleAdmitsARewrittenAuthor: the
// writer stores the upstream login on every sync, so one physical copy
// carrying a different author is a rewritten field, and a candidate equal
// to the other copy stays admitted.
func TestPersonDrilldownPRs_CapturedRetitleAdmitsARewrittenAuthor(t *testing.T) {
	baseBody, baseItems := retitleItems(t, "testdata/persondrilldownprs_retitle_baseline_default_6dc142e6.json")
	candBody, _ := retitleItems(t, "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json")
	for _, item := range baseItems {
		if isRetitledPullRequest(item) {
			item.(map[string]any)["author"] = "ABC-123"
			break
		}
	}
	result := compareAsRunner(t, personDrilldownPRsRequest(t, "drilldown_prs_default"), encodeBody(t, baseBody), encodeBody(t, candBody))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestPersonDrilldownPRs_CapturedCaseRefusesAWrongOrderOrCursor keeps the
// two differences no declaration admits on the captured default case: a
// candidate listing the right rows in reverse order leaves the length
// finding outside, with next_cursor's rendering beside it on the refused
// list, and a next_cursor naming a different instant stays outside on its
// own.
func TestPersonDrilldownPRs_CapturedCaseRefusesAWrongOrderOrCursor(t *testing.T) {
	const basePath = "testdata/persondrilldownprs_retitle_baseline_default_6dc142e6.json"
	const candPath = "testdata/persondrilldownprs_retitle_candidate_default_617a2956.json"
	req := personDrilldownPRsRequest(t, "drilldown_prs_default")
	cases := []struct {
		name        string
		mutate      func(body map[string]any, items []any)
		wantPath    string
		wantShape   string
		wantOutside int
	}{
		{"candidate rows in reverse order", func(body map[string]any, items []any) {
			for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
				items[i], items[j] = items[j], items[i]
			}
		}, "$.data.items", ShapeLength, 2},
		{"candidate cursor names a different instant", func(body map[string]any, items []any) {
			body["next_cursor"] = "2000-01-01T00:00:00Z"
		}, "$.data.next_cursor", ShapeValue, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseBody, _ := retitleItems(t, basePath)
			candBody, candItems := retitleItems(t, candPath)
			c.mutate(candBody, candItems)
			result := compareAsRunner(t, req, encodeBody(t, baseBody), encodeBody(t, candBody))
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want exactly %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
			if !hasFinding(result, c.wantPath, c.wantShape) {
				t.Fatalf("no %s %s finding: %+v", c.wantPath, c.wantShape, result.Findings)
			}
		})
	}
}
