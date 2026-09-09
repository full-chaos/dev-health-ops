package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func buildInfoServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestFetchBuildIdentityReturnsTheRunningCommit(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`)
	commit, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err != nil {
		t.Fatalf("FetchBuildIdentity: %v", err)
	}
	if commit != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("got %q", commit)
	}
}

// Every way the process can fail to identify itself is a REFUSAL. None of
// them may fall back to an operator-supplied name.
func TestFetchBuildIdentityRefusesAnUnidentifiableBuild(t *testing.T) {
	for name, testCase := range map[string]struct {
		status int
		body   string
	}{
		"empty commit":    {http.StatusOK, `{"commit":"","modified":false}`},
		"unknown commit":  {http.StatusOK, `{"commit":"unknown","modified":false}`},
		"modified tree":   {http.StatusOK, `{"commit":"b18e56fa7","modified":true}`},
		"route not there": {http.StatusNotFound, `not found`},
	} {
		t.Run(name, func(t *testing.T) {
			server := buildInfoServer(t, testCase.status, testCase.body)
			_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
				StaticCredential("Authorization", "test", "Bearer x"))
			if !errors.Is(err, ErrNoBuildIdentity) {
				t.Fatalf("expected ErrNoBuildIdentity, got %v", err)
			}
		})
	}
}

func TestFetchBuildIdentitySendsTheEnvelope(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"abc","modified":false}`)
	// No Authorization header: the stub answers 401, and that must be a
	// distinct failure from "cannot identify its build" -- one is a
	// credential problem, the other is a deployment problem.
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, nil)
	if err == nil {
		t.Fatal("an unauthenticated /buildinfo read must fail")
	}
	if errors.Is(err, ErrNoBuildIdentity) {
		t.Fatalf("a 401 is not an unidentifiable build: %v", err)
	}
}

// --candidate-build can FAIL a run; it can never supply the value.
func TestVerifyCandidateBuildTreatsTheFlagAsACrossCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", "abc", nil); err != nil {
		t.Fatalf("a matching cross-check must pass: %v", err)
	}
	err := VerifyCandidateBuild("abc", "def", nil)
	if err == nil {
		t.Fatal("a mismatched --candidate-build must fail the run")
	}
	if !strings.Contains(err.Error(), "cross-check, never the source") {
		t.Fatalf("the failure must say what the flag is for, got %v", err)
	}
}

// A routing row naming a build the process is not is REPORTED, not
// refused. It was a refusal until CHAOS-5484: see StaleRoutingRows for
// why the comparison never protected what the refusal claimed to, and why
// enforcing it made every shadow operation permanently unprovable after a
// redeploy.
func TestStaleRoutingRowsNamesDisagreeingRowsAndOnlyThose(t *testing.T) {
	stale := StaleRoutingRows("abc", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"hotspots":     {Mode: "canary", CandidateBuild: "stale-sha"},
		"flowMatrix":   {Mode: "shadow", CandidateBuild: "older-sha"},
		"unregistered": {Mode: "shadow"},
	})
	if got := stale["hotspots"]; got != "stale-sha" {
		t.Fatalf("a disagreeing canary row must be reported with the build it names, got %q", got)
	}
	// The row that made this a defect: a shadow row cannot be re-pointed
	// by any supported verb, so refusing on it made the operation
	// permanently unprovable.
	if got := stale["flowMatrix"]; got != "older-sha" {
		t.Fatalf("a disagreeing shadow row must be reported too, got %q", got)
	}
	if _, reported := stale["featureFlags"]; reported {
		t.Fatal("an agreeing row must not be reported")
	}
	if _, reported := stale["unregistered"]; reported {
		t.Fatal("a row naming no build at all has nothing to disagree with")
	}
	if len(stale) != 2 {
		t.Fatalf("expected exactly the two disagreeing rows, got %v", stale)
	}
}

// A stale routing row REFUSES the run again (r1 P1).
//
// The demotion assumed /buildinfo identifies the process that served the
// measured request. With more than one query-api replica and an edge that
// drops the per-request build header, it does not: /buildinfo can be
// answered by replica A while the measurement is served by replica B, and
// the resulting receipt names A. The routing row's build is the remaining
// cross-check that the fleet is on ONE build.
func TestAStaleRoutingRowRefusesTheRun(t *testing.T) {
	err := VerifyCandidateBuild("abc", "", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"flowMatrix":   {Mode: "shadow", CandidateBuild: "older-sha"},
	})
	if err == nil {
		t.Fatal("a routing row naming another build must refuse the run")
	}
	if !strings.Contains(err.Error(), "flowMatrix points at older-sha") {
		t.Fatalf("the refusal must NAME the disagreeing row, got %v", err)
	}
	if strings.Contains(err.Error(), "featureFlags") {
		t.Fatalf("an agreeing row must not be reported, got %v", err)
	}
	// The refusal has to tell the operator how to clear it, or it is a
	// wall rather than a gate.
	if !strings.Contains(err.Error(), "routing enable --candidate-build") {
		t.Fatalf("the refusal must say how to re-point the rows, got %v", err)
	}
}

func TestRowsAgreeingWithTheRunningBuildPassTheCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", "", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"unregistered": {Mode: "shadow"},
	}); err != nil {
		t.Fatalf("every row agrees, yet the run was refused: %v", err)
	}
}

func TestFetchRegistryRefusesAnEmptyRegistration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("a process registering no operations has nothing to prove")
	}
}

// A build that moves DURING a run invalidates every receipt the run wrote,
// because each names the build read before it started.
func TestVerifyBuildStableRefusesAMovedBuild(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"commit":"build-after","modified":false}`))
	}))
	t.Cleanup(server.Close)

	err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "build-before")
	if err == nil {
		t.Fatal("a build that moved mid-run must fail the run")
	}
	if !strings.Contains(err.Error(), "build-before") || !strings.Contains(err.Error(), "build-after") {
		t.Fatalf("the failure must name BOTH builds, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly one re-read, got %d", calls)
	}
}

func TestVerifyBuildStableAcceptsAnUnchangedBuild(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"commit":"same-build","modified":false}`))
	}))
	t.Cleanup(server.Close)

	if err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "same-build"); err != nil {
		t.Fatalf("an unchanged build must pass: %v", err)
	}
}

// A re-read that FAILS is not the same as a stable build -- the run cannot
// show its receipts name the build that served them.
func TestVerifyBuildStableRefusesWhenTheRereadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	if err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "some-build"); err == nil {
		t.Fatal("a failed re-read must fail the run, not pass silently")
	}
}

// r1 P2: the refusal path built its own prose and dropped the provenance
// the success path carried, so the receipts that most needed context had
// the least. Both paths now use ONE constructor, and the result is a JSON
// object rather than generated text appended to operator text.
func TestBothReceiptPathsCarryTheSameStructuredProvenance(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Config.ReviewEvidence = "CHAOS-5425 first deployed-executed run"

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	success, err := runner.ReceiptsFor(outcomes, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	refusal, err := runner.RefusalReceipts(outcomes, time.Now().UTC(), "the serving build moved DURING the run")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	if len(success) != 1 || len(refusal) != 1 {
		t.Fatalf("expected one receipt on each path, got %d and %d", len(success), len(refusal))
	}

	for name, receipt := range map[string]Receipt{"success": success[0], "refusal": refusal[0]} {
		t.Run(name, func(t *testing.T) {
			var provenance ReceiptProvenance
			if err := json.Unmarshal([]byte(receipt.ReviewEvidence), &provenance); err != nil {
				t.Fatalf("review_evidence is not a JSON object -- a reader cannot tell which half a machine wrote: %q (%v)", receipt.ReviewEvidence, err)
			}
			// The operator's words are kept VERBATIM and in their own key,
			// never concatenated with generated text.
			if provenance.Operator != "CHAOS-5425 first deployed-executed run" {
				t.Fatalf("the operator's own evidence was altered: %q", provenance.Operator)
			}
			if provenance.MeasurementRoute != RouteEdge {
				t.Fatalf("route not recorded: %q", provenance.MeasurementRoute)
			}
			// The fake edge stamps no build header, so this run has the
			// binding CHAOS-5479 leaves us with -- and it must SAY so.
			if provenance.EdgeBuildBinding != EdgeBuildAbsent {
				t.Fatalf("edge build binding not recorded: %q", provenance.EdgeBuildBinding)
			}
		})
	}

	// Only the refusal receipt carries the run-level cause and counts.
	var refusalProvenance ReceiptProvenance
	if err := json.Unmarshal([]byte(refusal[0].ReviewEvidence), &refusalProvenance); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(refusalProvenance.Refusal, "build moved") {
		t.Fatalf("the refusal cause is missing: %q", refusalProvenance.Refusal)
	}
	if refusalProvenance.Attempted != 1 || refusalProvenance.Measured != 1 {
		t.Fatalf("the refusal receipt must say how much work it reports on: %+v", refusalProvenance)
	}

	var successProvenance ReceiptProvenance
	if err := json.Unmarshal([]byte(success[0].ReviewEvidence), &successProvenance); err != nil {
		t.Fatal(err)
	}
	if successProvenance.Refusal != "" {
		t.Fatalf("a success receipt must carry no refusal: %q", successProvenance.Refusal)
	}
}

// An operator note containing the separator the old design used must not
// be able to forge machine-written provenance. This is why it is JSON.
func TestAnOperatorNoteCannotForgeProvenance(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Config.ReviewEvidence = `nice try | routing row named build DEADBEEF at measurement time"}`

	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	receipts, err := runner.ReceiptsFor(outcomes, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	var provenance ReceiptProvenance
	if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil {
		t.Fatalf("an operator note broke the encoding: %v", err)
	}
	if provenance.RoutingRowBuild != "" {
		t.Fatalf("operator text was read as machine provenance: %q", provenance.RoutingRowBuild)
	}
	if provenance.Operator != runner.Config.ReviewEvidence {
		t.Fatalf("the operator's text was not preserved verbatim: %q", provenance.Operator)
	}
}
