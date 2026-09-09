package goapiproof

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// cEdge answers both legs with full control over status, body and headers.
type cEdge struct {
	candidateBody, baselineBody     string
	candidateStatus, baselineStatus int
	candidateBuild                  string
	stampBuild                      bool
}

func (e *cEdge) run(t *testing.T, mode string, viaProofRoute bool) []Outcome {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		isBaseline := strings.Contains(parsed.Query, "python-plane control")

		if isBaseline {
			w.Header().Set(planeHeader, "python")
			w.Header().Set("Content-Type", "application/json")
			status := e.baselineStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(e.baselineBody))
			return
		}
		w.Header().Set(planeHeader, "go")
		w.Header().Set("Content-Type", "application/json")
		if e.stampBuild {
			w.Header().Set(buildHeader, e.candidateBuild)
		}
		status := e.candidateStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(e.candidateBody))
	}))
	t.Cleanup(server.Close)

	runner := newRunner(t, &fakeEdge{goBody: e.candidateBody, pythonBody: e.baselineBody}, mode)
	runner.Client = server.Client()
	if viaProofRoute {
		runner.Config.GoProofURL = server.URL
		runner.Config.PythonEdgeURL = server.URL
	} else {
		runner.Config.PythonEdgeURL = server.URL
	}
	outcomes, _, _ := runner.Run(context.Background(), nil)
	return outcomes
}

// The proof route stamps the serving build on every response, so an ABSENT
// build header there means the response did not come from the proof route
// -- or came from a build that cannot say what it is. Either way it cannot
// back a receipt that names a build. (Confirmation pass C1: this produced
// route=proof terminal=match build="" with no refusal at all.)
func TestProofRouteRefusesAnUnboundServingBuild(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{candidateBody: body, baselineBody: body, stampBuild: false}).run(t, "shadow", true)
	if out[0].RefusalReason != RefusalBuildUnbound {
		t.Fatalf("expected %s, got %s (terminal=%s admitted=%v)", RefusalBuildUnbound, out[0].RefusalReason, out[0].TerminalState, out[0].Admitted)
	}
	if out[0].Admitted {
		t.Fatal("an unbound serving build must not be admitted")
	}
}

// Two planes failing the same way is agreement about a failure, not parity.
// (Confirmation pass C2: identical HTTP 500s carrying decodable data
// produced terminal=match executed=true.)
func TestIdenticalNonSuccessStatusIsRefused(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		candidateStatus: 500, baselineStatus: 500,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalNonSuccessStatus {
		t.Fatalf("expected %s, got %s (terminal=%s)", RefusalNonSuccessStatus, out[0].RefusalReason, out[0].TerminalState)
	}
	if out[0].Admitted || out[0].Executed {
		t.Fatalf("a non-2xx pair must be neither admitted nor executed: admitted=%v executed=%v", out[0].Admitted, out[0].Executed)
	}
	if out[0].TerminalState != "dependency_failed" {
		t.Fatalf("the recorded terminal state must stay meaningful, got %s", out[0].TerminalState)
	}
}

// A decoder stops at the end of the first JSON value, so bytes after it are
// never compared -- the comparison would not be over what was served.
// (Confirmation pass C3: a 67-byte candidate and a 39-byte baseline
// produced terminal=match.)
func TestTrailingBytesAreRefused(t *testing.T) {
	clean := `{"data":{"featureFlags":[{"key":"a"}]}}`
	out := (&cEdge{
		candidateBody: clean + `{"data":{"featureFlags":[]}}`,
		baselineBody:  clean,
		stampBuild:    true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalTrailingBytes {
		t.Fatalf("expected %s, got %s (terminal=%s, candidate %d bytes vs baseline %d)",
			RefusalTrailingBytes, out[0].RefusalReason, out[0].TerminalState,
			len(out[0].Candidate.Body), len(out[0].Baseline.Body))
	}
	if out[0].Admitted {
		t.Fatal("a body with trailing bytes must not be admitted")
	}
}

// An empty `data` object means nothing was resolved: the operation's own
// root field is absent. (Confirmation pass C4: `{"data":{}}` produced
// terminal=match executed=true.)
func TestEmptyDataObjectIsRefused(t *testing.T) {
	body := `{"data":{}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if out[0].RefusalReason != RefusalEmptyResponseRoot {
		t.Fatalf("expected %s, got %s (terminal=%s)", RefusalEmptyResponseRoot, out[0].RefusalReason, out[0].TerminalState)
	}
	if out[0].Admitted {
		t.Fatal("a response with no resolved root field must not be admitted")
	}
}

// The inverse control: an empty LIST is a legitimate result -- "this org has
// no feature flags" is a real answer -- and must still be admitted, or the
// instrument could never prove an operation over empty data.
func TestEmptyListRootIsStillAdmitted(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	out := (&cEdge{
		candidateBody: body, baselineBody: body,
		stampBuild: true, candidateBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36",
	}).run(t, "canary", false)
	if !out[0].Admitted || out[0].TerminalState != TerminalStateMatch {
		t.Fatalf("an empty list is a real result: admitted=%v terminal=%s refusal=%s (%s)",
			out[0].Admitted, out[0].TerminalState, out[0].RefusalReason, out[0].RefusalDetail)
	}
}
