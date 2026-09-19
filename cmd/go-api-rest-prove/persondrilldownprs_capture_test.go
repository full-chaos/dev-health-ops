package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestRunMeasurement_PersonDrilldownPRsCapturedBodiesHaveNothingOutside
// runs the whole corpus the way an operator does, with both planes
// answering GET /api/v1/people/{person_id}/drilldown/prs from bodies
// captured on a production proof run: the baseline repeats each pull
// request 2 or 4 times and one pull request's copies carry two titles.
// Every request of that route is admitted with nothing outside.
func TestRunMeasurement_PersonDrilldownPRsCapturedBodiesHaveNothingOutside(t *testing.T) {
	const build = "build123"
	const fixtures = "../../internal/goapiproof/testdata/"
	read := func(name string) []byte {
		body, err := os.ReadFile(fixtures + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return body
	}
	baselineDefault := read("persondrilldownprs_retitle_baseline_default_6dc142e6.json")
	baselineCursor := read("persondrilldownprs_retitle_baseline_cursor_f02ed0d8.json")
	baselineLimitZero := read("persondrilldownprs_retitle_baseline_limitzero_9aeed1f7.json")
	candidateDefault := read("persondrilldownprs_retitle_candidate_default_617a2956.json")
	candidateCursor := read("persondrilldownprs_retitle_candidate_cursor_d86755f6.json")

	handler := func(candidate bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if candidate {
				w.Header().Set("x-dev-health-build", build)
			}
			switch {
			case r.URL.Path == "/api/v1/people":
				_, _ = w.Write([]byte(`[{"person_id":"p-1"}]`))
			case r.URL.Path == "/api/v1/people/p-1/drilldown/prs":
				query := r.URL.Query()
				switch {
				case query.Get("cursor") != "" && candidate:
					_, _ = w.Write(candidateCursor)
				case query.Get("cursor") != "":
					_, _ = w.Write(baselineCursor)
				case candidate:
					_, _ = w.Write(candidateDefault)
				case query.Get("limit") == "0":
					_, _ = w.Write(baselineLimitZero)
				default:
					_, _ = w.Write(baselineDefault)
				}
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		}
	}
	candidate := httptest.NewServer(handler(true))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(handler(false)))
	defer baseline.Close()
	dir := t.TempDir()
	reportPath := dir + "/report.json"
	artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
	if err != nil {
		t.Fatalf("NewArtifactStore: %v", err)
	}
	f := flags{
		queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL,
		org: "org-1", recordedBy: "chris", reviewEvidence: "test",
		timeout: 5 * time.Second, dryRun: true, reportPath: reportPath,
	}
	_ = captureStdout(t, func() {
		_ = runMeasurement(context.Background(), goapiproof.NewLegClient(0), f,
			staticCredentialForTest(), staticCredentialForTest(), sameProverBuildForTest(build), nil, artifacts)
	})
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	want := map[string]bool{"drilldown_prs_default": false, "valid_cursor": false, "limit_above_ceiling": false, "limit_zero_falls_back_to_default": false}
	for _, o := range report.Outcomes {
		if o.Operation != "REST:GET:/api/v1/people/{person_id}/drilldown/prs" {
			continue
		}
		if _, tracked := want[o.Request]; !tracked {
			continue
		}
		want[o.Request] = true
		t.Logf("%s -> admitted=%v terminal=%q outside=%d matched=%v", o.Request, o.Admitted, o.TerminalState, o.DifferencesOutsideBaselineDefect, o.BaselineDefectsMatched)
		if !o.Admitted || o.TerminalState != goapiproof.TerminalStateMismatch || o.DifferencesOutsideBaselineDefect != 0 {
			t.Errorf("%s = admitted %v terminal %q outside %d, want an admitted comparison with nothing outside", o.Request, o.Admitted, o.TerminalState, o.DifferencesOutsideBaselineDefect)
		}
	}
	for request, seen := range want {
		if !seen {
			t.Errorf("report has no %s outcome", request)
		}
	}
}
