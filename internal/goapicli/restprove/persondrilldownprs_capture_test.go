package restprove

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// TestProveOneRESTRequest_PersonDrilldownPRsCapturedBodiesHaveNothingOutside
// drives each of GET /api/v1/people/{person_id}/drilldown/prs' own four
// requests directly through proveOneRESTRequest, with both planes
// answering from bodies captured on a production proof run: the baseline
// repeats each pull request 2 or 4 times and one pull request's copies
// carry two titles. Every request of that route is admitted with nothing
// outside.
//
// GET /api/v1/people/{person_id}/drilldown/prs is a deleted-Python-body
// route (CHAOS-6241, goapiproof/restdeletedbody.go): the committed corpus
// now declares every one of its requests' baseline as the fixed sentinel,
// never 200, so the real dedup-key-injection/baseline-copy coverage this
// test exists to pin -- unaffected by that ticket, it is entirely about
// Compare()'s own logic against a real captured production shape -- is
// exercised here on a LOCAL copy of each request restored to its
// pre-deletion shape, not through the full runMeasurement/corpus-driven
// pipeline this test used to use (which would refuse every one of these
// under the now-committed sentinel). person_id is a plain literal here
// (not resolved through GET /api/v1/people, which this test no longer
// needs to fake) -- proveOneRESTRequest never re-resolves IDBindings
// itself; only the caller's own boundIDs matters for its path-literal
// safety check.
func TestProveOneRESTRequest_PersonDrilldownPRsCapturedBodiesHaveNothingOutside(t *testing.T) {
	const build = "build123"
	const fixtures = "../../goapiproof/testdata/"
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
		}
	}
	candidate := httptest.NewServer(handler(true))
	defer candidate.Close()
	baseline := httptest.NewServer(referencePlane(handler(false)))
	defer baseline.Close()

	spec, err := goapiproof.SpecForREST("REST:GET:/api/v1/people/{person_id}/drilldown/prs")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	spec.Path = "/api/v1/people/p-1/drilldown/prs"
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 5 * time.Second}
	want := map[string]bool{"drilldown_prs_default": false, "valid_cursor": false, "limit_above_ceiling": false, "limit_zero_falls_back_to_default": false}
	for _, request := range spec.Requests {
		if _, tracked := want[request.Name]; !tracked {
			continue
		}
		want[request.Name] = true
		t.Run(request.Name, func(t *testing.T) {
			request.WantBaselineStatus = 200
			request.BodyMode = goapiproof.RESTBodyModeJSON
			request.StatusDivergenceReason = ""
			boundIDs := map[string]string{}
			for _, binding := range request.IDBindings {
				if binding.PathParam == "person_id" {
					boundIDs[binding.Producer] = "p-1"
				}
			}
			out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/api/v1/people/{person_id}/drilldown/prs", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, boundIDs)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			t.Logf("%s -> %s", request.Name, out.line())
			if !out.Admitted || out.TerminalState != goapiproof.TerminalStateMismatch || out.DifferencesOutsideBaselineDefect != 0 {
				t.Errorf("%s = admitted %v terminal %q outside %d, want an admitted comparison with nothing outside", request.Name, out.Admitted, out.TerminalState, out.DifferencesOutsideBaselineDefect)
			}
		})
	}
	for request, seen := range want {
		if !seen {
			t.Errorf("corpus has no %s request", request)
		}
	}
}
