package restprove

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// deadlineRun is one run of the real loop against stub planes.
type deadlineRun struct {
	report jsonReport
	stdout string
	err    error
}

// runUnderDeadline runs runMeasurement over the whole corpus against stub
// planes, with the given run context and baseline credential, and a
// per-request timeout far longer than any run deadline used here -- so a
// leg can only end early because the run did.
func runUnderDeadline(t *testing.T, ctx context.Context, stall *stalledRequest, stallCandidate bool, baselineCredential *goapiproof.Credential, runDeadline time.Duration) deadlineRun {
	t.Helper()
	return runUnderDeadlineWith(t, ctx, stall, stallCandidate, baselineCredential, runDeadline, nil)
}

// runUnderDeadlineWith is runUnderDeadline with an optional wrapper
// around the candidate plane's handler.
func runUnderDeadlineWith(t *testing.T, ctx context.Context, stall *stalledRequest, stallCandidate bool, baselineCredential *goapiproof.Credential, runDeadline time.Duration, wrapCandidate func(http.Handler) http.Handler) deadlineRun {
	t.Helper()
	const build = "build123"
	var candidateStall, baselineStall *stalledRequest
	if stallCandidate {
		candidateStall = stall
	} else {
		baselineStall = stall
	}
	var candidateHandler http.Handler = genericRESTStubHandler(t, build, candidateStall)
	if wrapCandidate != nil {
		candidateHandler = wrapCandidate(candidateHandler)
	}
	candidate := httptest.NewServer(candidateHandler)
	t.Cleanup(candidate.Close)
	baseline := httptest.NewServer(referencePlane(genericRESTStubHandler(t, "", baselineStall)))
	t.Cleanup(baseline.Close)
	dir := t.TempDir()
	artifacts, err := goapiproof.NewArtifactStore(dir + "/artifacts")
	if err != nil {
		t.Fatal(err)
	}
	f := flags{queryAPIURL: candidate.URL, pythonAPIURL: baseline.URL, org: "org-1", recordedBy: "r", reviewEvidence: "e",
		timeout: time.Minute, runDeadline: runDeadline, dryRun: true, reportPath: dir + "/report.json"}
	var run deadlineRun
	run.stdout = captureStdout(t, func() {
		run.err = runMeasurement(ctx, goapiproof.NewLegClient(0), f, staticCredentialForTest(), baselineCredential, sameProverBuildForTest(build), nil, artifacts)
	})
	raw, err := os.ReadFile(f.reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if err := json.Unmarshal(raw, &run.report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	return run
}

// corpusRequestKeys is every request key the corpus declares, built from
// the corpus itself (KnownRESTOperations and each endpoint's own
// Requests), not from the run's planner, so a request the planner drops
// is still expected.
func corpusRequestKeys(t *testing.T) []string {
	t.Helper()
	var keys []string
	for _, operation := range goapiproof.KnownRESTOperations() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range spec.Requests {
			keys = append(keys, operation+"/"+request.Name)
			if !spec.PublicNoAuth {
				keys = append(keys, operation+"/"+request.Name+" (edge-credential-on-candidate)")
			}
		}
	}
	return keys
}

// accountFor checks the report accounts for every request the corpus
// declares exactly once -- an outcome, or not_run -- with no exception,
// and names no request the corpus does not declare.
func accountFor(t *testing.T, report jsonReport) {
	t.Helper()
	seen := map[string]int{}
	for _, o := range report.Outcomes {
		seen[o.Operation+"/"+o.Request]++
	}
	for _, key := range report.NotRun {
		seen[key]++
	}
	expected := map[string]bool{}
	for _, key := range corpusRequestKeys(t) {
		expected[key] = true
		if seen[key] != 1 {
			t.Errorf("corpus request %q is accounted for %d time(s), want exactly once (an outcome or not_run)", key, seen[key])
		}
	}
	for key := range seen {
		if !expected[key] {
			t.Errorf("the report names %q, which the corpus does not declare", key)
		}
	}
}

// countingCredential counts how many values it is asked for.
func countingCredential(calls *atomic.Int32) *goapiproof.Credential {
	return goapiproof.MintedCredential("Authorization", "baseline bearer", time.Nanosecond, func(context.Context) (string, error) {
		calls.Add(1)
		return "Bearer test-token", nil
	})
}

// mintOnCall returns a baseline credential that mints at once on every
// call except the nth, where it calls reached, waits for its request's
// context to end and then either returns a token (the run ends between
// the candidate leg and the baseline leg's send) or the context's error
// (the run ends during the mint).
func mintOnCall(n int32, returnToken bool, reached func()) *goapiproof.Credential {
	var calls atomic.Int32
	return goapiproof.MintedCredential("Authorization", "baseline bearer", time.Nanosecond, func(ctx context.Context) (string, error) {
		if calls.Add(1) == n {
			reached()
			<-ctx.Done()
			if !returnToken {
				return "", ctx.Err()
			}
		}
		return "Bearer test-token", nil
	})
}

// stallTheRunsLastLeg holds, until its request ends, the edge-credential
// leg of the last request in RESTRunOrder -- the last leg a complete run
// sends -- recognised by the baseline credential's token on the candidate
// plane and by that request's own body -- calling reached as it arrives.
func stallTheRunsLastLeg(t *testing.T, edgeToken string, reached func()) func(http.Handler) http.Handler {
	t.Helper()
	order := goapiproof.RESTRunOrder()
	last := order[len(order)-1]
	spec, err := goapiproof.SpecForREST(last)
	if err != nil {
		t.Fatal(err)
	}
	final := spec.Requests[len(spec.Requests)-1]
	want, err := json.Marshal(final.Body)
	if err != nil {
		t.Fatal(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == spec.Method && r.URL.Path == spec.Path && r.Header.Get("Authorization") == edgeToken {
				raw, _ := io.ReadAll(r.Body)
				var got, wanted any
				_ = json.Unmarshal(raw, &got)
				_ = json.Unmarshal(want, &wanted)
				if reflect.DeepEqual(got, wanted) {
					reached()
					<-r.Context().Done()
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// TestRunDeadlineEveryPointAtWhichItCanStrike proves, through the real
// loop, that a run which does not attempt every corpus case says so,
// says why, and exits non-zero, and that a leg the run deadline cut is
// never reported as a leg timeout -- at every point the deadline can
// strike: before the first case, mid candidate leg, mid baseline leg,
// between a case's candidate and baseline legs, during a credential
// mint, and after the last case; plus a signal mid-leg.
func TestRunDeadlineEveryPointAtWhichItCanStrike(t *testing.T) {
	// Every cell ends its run at the event it names -- the leg reaching
	// its server, the mint being asked for -- with the error the run's own
	// deadline or a signal carries; none waits on a clock, so a slow host
	// cannot move the end to another point of the run.
	stallHome := func(ctx *endableContext, err error) *stalledRequest {
		return &stalledRequest{method: http.MethodGet, path: "/api/v1/home", rawQuery: "", reached: func() { ctx.end(err) }}
	}
	endsAt := func(ctx *endableContext, err error) func() { return func() { ctx.end(err) } }
	const runDeadline = 1500 * time.Millisecond
	cells := []struct {
		name      string
		run       func(t *testing.T) deadlineRun
		wantCause string
		wantCut   string
	}{
		{"before the first case", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			ctx.end(context.DeadlineExceeded)
			return runUnderDeadline(t, ctx, nil, false, staticCredentialForTest(), time.Nanosecond)
		}, partialCauseRunDeadline, ""},
		{"mid candidate leg", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, stallHome(ctx, context.DeadlineExceeded), true, staticCredentialForTest(), runDeadline)
		}, partialCauseRunDeadline, goapiproof.RESTRefusalCandidateLegCutByRunDeadline},
		{"mid baseline leg", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, stallHome(ctx, context.DeadlineExceeded), false, staticCredentialForTest(), runDeadline)
		}, partialCauseRunDeadline, goapiproof.RESTRefusalBaselineLegCutByRunDeadline},
		{"between a case's candidate and baseline legs", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, nil, false, mintOnCall(5, true, endsAt(ctx, context.DeadlineExceeded)), runDeadline)
		}, partialCauseRunDeadline, goapiproof.RESTRefusalBaselineLegCutByRunDeadline},
		{"during a credential mint", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, nil, false, mintOnCall(5, false, endsAt(ctx, context.DeadlineExceeded)), runDeadline)
		}, partialCauseRunDeadline, ""},
		{"during the run's last credential mint", func(t *testing.T) deadlineRun {
			var calls atomic.Int32
			runUnderDeadline(t, context.Background(), nil, false, countingCredential(&calls), time.Minute)
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, nil, false, mintOnCall(calls.Load(), false, endsAt(ctx, context.DeadlineExceeded)), runDeadline)
		}, partialCauseRunDeadline, ""},
		{"a signal mid-leg", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			return runUnderDeadline(t, ctx, stallHome(ctx, context.Canceled), false, staticCredentialForTest(), defaultRunDeadline)
		}, partialCauseSignal, goapiproof.RESTRefusalBaselineLegCutBySignal},
		{"mid the run's last leg", func(t *testing.T) deadlineRun {
			ctx := newEndableContext()
			edge := goapiproof.StaticCredential("Authorization", "baseline bearer", "Bearer edge-token")
			return runUnderDeadlineWith(t, ctx, nil, false, edge, runDeadline, stallTheRunsLastLeg(t, "Bearer edge-token", endsAt(ctx, context.DeadlineExceeded)))
		}, partialCauseRunDeadline, goapiproof.RESTRefusalCandidateLegCutByRunDeadline},
		{"after the last case", func(t *testing.T) deadlineRun {
			return runUnderDeadline(t, context.Background(), nil, false, staticCredentialForTest(), defaultRunDeadline)
		}, "", ""},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			run := cell.run(t)
			accountFor(t, run.report)
			if run.report.Outcomes == nil {
				t.Fatal("the report's outcomes is null, want a list")
			}
			if run.report.PartialCause != cell.wantCause {
				t.Fatalf("partial_cause = %q, want %q (err %v)", run.report.PartialCause, cell.wantCause, run.err)
			}
			if run.report.RunDeadline == "" || !strings.Contains(run.stdout, "run_deadline="+run.report.RunDeadline+"\n") || !strings.Contains(run.stdout, "attempted=") {
				t.Fatalf("run_deadline %q; summary line present=%v", run.report.RunDeadline, strings.Contains(run.stdout, "attempted="))
			}
			cut := 0
			for _, o := range run.report.Outcomes {
				switch o.Refusal {
				case goapiproof.RESTRefusalCandidateLegTimedOut, goapiproof.RESTRefusalBaselineLegTimedOut,
					goapiproof.RESTRefusalCandidateLegTransportError, goapiproof.RESTRefusalBaselineLegTransportError:
					t.Errorf("%s/%s: a leg ended by the run is labelled %q", o.Operation, o.Request, o.Refusal)
				case goapiproof.RESTRefusalCandidateLegCutByRunDeadline, goapiproof.RESTRefusalBaselineLegCutByRunDeadline,
					goapiproof.RESTRefusalCandidateLegCutBySignal, goapiproof.RESTRefusalBaselineLegCutBySignal:
					if o.Refusal != cell.wantCut {
						t.Errorf("%s/%s: refusal %q, want %q", o.Operation, o.Request, o.Refusal, cell.wantCut)
					}
					cut++
				}
			}
			if cell.wantCause == "" {
				if run.report.Partial || len(run.report.NotRun) != 0 || strings.Contains(run.stdout, "partial_cause=") {
					t.Fatalf("a run that reached its last case reads as partial: not_run=%v", run.report.NotRun)
				}
				return
			}
			if run.err == nil {
				t.Fatal("a run stopped short exited zero")
			}
			wantErr := map[string]error{partialCauseRunDeadline: context.DeadlineExceeded, partialCauseSignal: context.Canceled}[cell.wantCause]
			if wantErr != nil && !errors.Is(run.err, wantErr) {
				t.Fatalf("err = %v, want the run's own %v: the exit must say why the run stopped", run.err, wantErr)
			}
			if !strings.Contains(run.stdout, "partial_cause="+cell.wantCause) {
				t.Fatalf("stdout does not name partial_cause=%s", cell.wantCause)
			}
			if cell.name == "mid the run's last leg" || cell.name == "during the run's last credential mint" {
				// Every request but the last leg was attempted.
				if cell.name == "during the run's last credential mint" && len(run.report.NotRun) != 1 {
					t.Fatalf("not_run = %v, want exactly the leg whose credential mint the deadline cut", run.report.NotRun)
				}
			}
			if cell.name == "mid the run's last leg" {
				// Every request was attempted; the run's last leg was cut.
				if len(run.report.NotRun) != 0 {
					t.Fatalf("not_run = %v, want empty: every request was attempted", run.report.NotRun)
				}
			} else if cell.name == "during the run's last credential mint" {
				if !run.report.Partial {
					t.Fatal("partial = false with a request in not_run")
				}
			} else if !run.report.Partial || len(run.report.NotRun) == 0 {
				t.Fatalf("partial=%v not_run=%d: the cases the run never reached are not named", run.report.Partial, len(run.report.NotRun))
			}
			wantCuts := 0
			if cell.wantCut != "" {
				wantCuts = 1
			}
			if cut != wantCuts {
				t.Fatalf("%d outcome(s) name a leg cut by the run, want %d: only the leg in flight is cut, and no leg is sent after the run ended", cut, wantCuts)
			}
		})
	}
}

// TestStopCauseIsReadFromTheRunContext pins the precedence: the run
// context's own state first, and a request's own timeout in an error
// chain is a tool error, never the run deadline.
func TestStopCauseIsReadFromTheRunContext(t *testing.T) {
	requestTimeout := errors.New("mint baseline bearer credential: " + context.DeadlineExceeded.Error())
	cells := []struct {
		name             string
		runEnded, runErr error
		want             string
	}{
		{"live, no error", nil, nil, ""},
		{"run deadline", context.DeadlineExceeded, nil, partialCauseRunDeadline},
		{"run deadline with a loop error", context.DeadlineExceeded, errors.New("x"), partialCauseRunDeadline},
		{"signal", context.Canceled, nil, partialCauseSignal},
		{"tool error", nil, errors.New("store artifact: permission denied"), partialCauseToolError},
		{"a request's own timeout", nil, requestTimeout, partialCauseToolError},
		{"a request's own timeout, wrapped", nil, wrapDeadline(), partialCauseToolError},
	}
	for _, cell := range cells {
		if got := stopCause(cell.runEnded, cell.runErr); got != cell.want {
			t.Errorf("%s: stopCause = %q, want %q", cell.name, got, cell.want)
		}
	}
}

func wrapDeadline() error {
	return errors.Join(errors.New("baseline leg"), context.DeadlineExceeded)
}

// TestParseFlags_RunDeadline pins -run-deadline: default 30m, and any
// value not greater than zero refused.
func TestParseFlags_RunDeadline(t *testing.T) {
	base := []string{"-org", "org-1", "-recorded-by", "r", "-review-evidence", "e", "-dry-run",
		"-candidate-bearer-exec", `["true"]`, "-baseline-bearer-exec", `["true"]`, "-artifact-dir", t.TempDir(),
		"-python-api-url", "http://api:8000"}
	for value, want := range map[string]string{"": "30m0s", "45m": "45m0s", "1ns": "1ns", "0s": "refused", "-1m": "refused"} {
		args := append([]string(nil), base...)
		if value != "" {
			args = append(args, "-run-deadline", value)
		}
		f, err := parseFlags(args)
		switch {
		case want == "refused":
			if err == nil || !strings.Contains(err.Error(), "-run-deadline must be greater than zero") {
				t.Errorf("-run-deadline %q: err = %v, want the refusal", value, err)
			}
		case err != nil || f.runDeadline.String() != want:
			t.Errorf("-run-deadline %q: got %s err=%v, want %s", value, f.runDeadline, err, want)
		}
	}
}

// TestLegTransportOutcomeNamesWhatCutTheLeg enumerates the run context's
// state x the leg x the transport failure class: a leg is named timed_out
// or transport_error only while the run itself is live; once the run's
// deadline or a signal has ended it, the leg is named for that.
func TestLegTransportOutcomeNamesWhatCutTheLeg(t *testing.T) {
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	signalled, cancelSignalled := context.WithCancel(context.Background())
	cancelSignalled()
	failures := map[string]error{
		"timeout": goapiproof.NewTransportFailure("http://api:8000/x", context.DeadlineExceeded),
		"refused": goapiproof.NewTransportFailure("http://api:8000/x", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}),
	}
	names := map[string]map[string]string{
		"run deadline": {"candidate": goapiproof.RESTRefusalCandidateLegCutByRunDeadline, "baseline": goapiproof.RESTRefusalBaselineLegCutByRunDeadline},
		"signal":       {"candidate": goapiproof.RESTRefusalCandidateLegCutBySignal, "baseline": goapiproof.RESTRefusalBaselineLegCutBySignal},
		"timeout":      {"candidate": goapiproof.RESTRefusalCandidateLegTimedOut, "baseline": goapiproof.RESTRefusalBaselineLegTimedOut},
		"refused":      {"candidate": goapiproof.RESTRefusalCandidateLegTransportError, "baseline": goapiproof.RESTRefusalBaselineLegTransportError},
	}
	for runName, runCtx := range map[string]context.Context{"live": context.Background(), "run deadline": expired, "signal": signalled} {
		for _, leg := range []string{"candidate", "baseline"} {
			for class, failure := range failures {
				out, ok := legTransportOutcome(runCtx, "op", "req", leg, nil, failure)
				if !ok {
					t.Fatalf("%s/%s/%s: not classified as a leg failure", runName, leg, class)
				}
				want := names[class][leg]
				if runName != "live" {
					want = names[runName][leg]
				}
				if out.Refusal != want {
					t.Errorf("run %s, %s leg, %s: refusal %q, want %q", runName, leg, class, out.Refusal, want)
				}
			}
		}
	}
	if _, ok := legTransportOutcome(context.Background(), "op", "req", "baseline", nil, errors.New("not a transport failure")); ok {
		t.Fatal("a non-transport error was classified as a leg failure")
	}
}

// TestRunDeadlineFlagBoundsTheRunContext: the run context ends at the
// -run-deadline value it is given.
func TestRunDeadlineFlagBoundsTheRunContext(t *testing.T) {
	ctx, cancel := runContext(50 * time.Millisecond)
	defer cancel()
	awaitEvent(t, ctx.Done(), "the run context reaching its deadline")
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx.Err() = %v, want context.DeadlineExceeded", ctx.Err())
	}
}

// TestARunStoppedBeforeItsFirstRequestNamesEveryPlannedRequest pins the
// report of a run that stops before its request loop: every planned
// request in not_run, and partial_cause from the run context's own state.
func TestARunStoppedBeforeItsFirstRequestNamesEveryPlannedRequest(t *testing.T) {
	for name, cell := range map[string]struct {
		runEnded error
		want     string
	}{
		"run still live":      {nil, partialCauseRefusedBeforeMeasuring},
		"run deadline passed": {context.DeadlineExceeded, partialCauseRunDeadline},
		"signal":              {context.Canceled, partialCauseSignal},
	} {
		f := flags{reportPath: t.TempDir() + "/report.json", runDeadline: 45 * time.Minute}
		var err error
		captureStdout(t, func() {
			err = writeStoppedBeforeMeasuringReport(f, nil, cell.runEnded, errors.New("read the candidate build from /buildinfo: refused"))
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		raw, err := os.ReadFile(f.reportPath)
		if err != nil {
			t.Fatal(err)
		}
		var report jsonReport
		if err := json.Unmarshal(raw, &report); err != nil {
			t.Fatal(err)
		}
		if report.PartialCause != cell.want || !report.Partial || report.PartialError == "" || report.RunDeadline != "45m0s" {
			t.Errorf("%s: partial_cause=%q partial=%v partial_error=%q run_deadline=%q", name, report.PartialCause, report.Partial, report.PartialError, report.RunDeadline)
		}
		accountFor(t, report)
	}
}

// TestRunWritesTheReportWhenItStopsBeforeItsFirstRequest drives run()
// itself to a refusal before its loop (an unreadable query-api source)
// and reads back the report it leaves.
func TestRunWritesTheReportWhenItStopsBeforeItsFirstRequest(t *testing.T) {
	dir := t.TempDir()
	stale := []byte(`{"outcomes":[],"partial":false}`)
	f := flags{queryAPIURL: "http://127.0.0.1:1", pythonAPIURL: "http://127.0.0.1:1", queryAPISrc: dir + "/no-such-query-api-source",
		org: "org-1", recordedBy: "r", reviewEvidence: "e", artifactDir: dir + "/artifacts", dryRun: true,
		reportPath: dir + "/report.json", timeout: time.Second, runDeadline: time.Minute}
	if err := os.WriteFile(f.reportPath, stale, 0o600); err != nil {
		t.Fatal(err)
	}
	var err error
	captureStdout(t, func() { err = run(f) })
	if err == nil {
		t.Fatal("run: want the refusal")
	}
	raw, readErr := os.ReadFile(f.reportPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.PartialCause != partialCauseRefusedBeforeMeasuring || !report.Partial || report.RunDeadline != "1m0s" {
		t.Fatalf("report left by run(): partial_cause=%q partial=%v run_deadline=%q (a stale report must be replaced)", report.PartialCause, report.Partial, report.RunDeadline)
	}
	accountFor(t, report)
}

// TestRunContextIsCancelledBySIGTERM delivers a real SIGTERM to this
// process while runContext holds it: the context is cancelled (the loop
// then stops and writes the report) instead of the process being killed.
func TestRunContextIsCancelledBySIGTERM(t *testing.T) {
	ctx, cancel := runContext(time.Minute)
	defer cancel()
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	awaitEvent(t, ctx.Done(), "SIGTERM cancelling the run context")
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("ctx.Err() = %v, want context.Canceled", ctx.Err())
	}
}

// TestRunIsBoundedByTheRunDeadlineFlag drives run() with -run-deadline
// 1ns: the run context it builds from the flag has already ended when
// run() stops, so the report it leaves says run_deadline.
func TestRunIsBoundedByTheRunDeadlineFlag(t *testing.T) {
	dir := t.TempDir()
	f := flags{queryAPIURL: "http://127.0.0.1:1", pythonAPIURL: "http://127.0.0.1:1", queryAPISrc: dir + "/no-such-query-api-source",
		org: "org-1", recordedBy: "r", reviewEvidence: "e", artifactDir: dir + "/artifacts", dryRun: true,
		reportPath: dir + "/report.json", timeout: time.Second, runDeadline: time.Nanosecond}
	var err error
	captureStdout(t, func() { err = run(f) })
	if err == nil {
		t.Fatal("run: want the refusal")
	}
	raw, readErr := os.ReadFile(f.reportPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	var report jsonReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	if report.PartialCause != partialCauseRunDeadline || report.RunDeadline != "1ns" {
		t.Fatalf("partial_cause=%q run_deadline=%q, want %q and 1ns: the run context is not bounded by -run-deadline", report.PartialCause, report.RunDeadline, partialCauseRunDeadline)
	}
}
