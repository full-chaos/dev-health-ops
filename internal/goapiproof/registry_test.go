package goapiproof

import (
	"context"
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
	if err := VerifyCandidateBuild("abc", "abc"); err != nil {
		t.Fatalf("a matching cross-check must pass: %v", err)
	}
	err := VerifyCandidateBuild("abc", "def")
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

// The whole point of the demotion: a stale row must not stop the run.
func TestAStaleRoutingRowDoesNotFailTheCrossCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", ""); err != nil {
		t.Fatalf("no --candidate-build supplied and nothing to cross-check, yet the run was refused: %v", err)
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

// The demotion, end to end at the runner: a stale routing row must not
// stop the measurement, and the fact must survive into the receipt.
//
// Before CHAOS-5484 this run did not happen at all -- VerifyCandidateBuild
// refused before a single request was sent, which is how JOB 4's attempt E
// ended with zero rows written and fifteen operations unmeasured.
func TestAStaleRoutingRowIsRecordedOnTheReceiptNotRefused(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	// The row names the build it was enabled at; the process is something
	// else. This is the state of every row on the stack after a redeploy.
	runner.Routing["featureFlags"] = RoutingRow{Mode: "canary", CandidateBuild: "0000000000000000000000000000000000000000"}
	runner.Config.ReviewEvidence = "CHAOS-5425 first deployed-executed run"

	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !outcomes[0].Executed {
		t.Fatalf("a stale routing row must not stop the measurement: %s %s", outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
	if summary.StaleRoutingRows != 1 {
		t.Fatalf("expected the stale row to be counted, got %d", summary.StaleRoutingRows)
	}
	if outcomes[0].RoutingRowBuild != "0000000000000000000000000000000000000000" {
		t.Fatalf("the outcome must name the build the ROW claimed, got %q", outcomes[0].RoutingRowBuild)
	}

	receipts, err := runner.ReceiptsFor(outcomes, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if len(receipts) != 1 {
		t.Fatalf("expected one receipt, got %d", len(receipts))
	}
	// The receipt names what SERVED the request, never the row's build.
	if receipts[0].CandidateBuild != runner.Registry.BuildIdentity {
		t.Fatalf("the receipt named %q, not the running build", receipts[0].CandidateBuild)
	}
	if !strings.Contains(receipts[0].ReviewEvidence, "0000000000000000000000000000000000000000") {
		t.Fatalf("the stale row must reach go_api_proof_run, not just a report file: %q", receipts[0].ReviewEvidence)
	}
	if !strings.Contains(receipts[0].ReviewEvidence, "CHAOS-5425 first deployed-executed run") {
		t.Fatalf("the operator's own evidence must be kept, not replaced: %q", receipts[0].ReviewEvidence)
	}
}

// And nothing is added when the row agrees: a receipt should not carry a
// sentence saying that nothing was wrong.
func TestAnAgreeingRoutingRowAddsNothingToTheReceipt(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Config.ReviewEvidence = "CHAOS-5425 first deployed-executed run"

	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.StaleRoutingRows != 0 {
		t.Fatalf("no row was stale, got %d", summary.StaleRoutingRows)
	}
	receipts, err := runner.ReceiptsFor(outcomes, time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	if receipts[0].ReviewEvidence != "CHAOS-5425 first deployed-executed run" {
		t.Fatalf("review evidence was modified with nothing to report: %q", receipts[0].ReviewEvidence)
	}
}
