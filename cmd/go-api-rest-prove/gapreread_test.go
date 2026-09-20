package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// seqServers serves candidateReads / baselineReads in call order (the last
// repeats); "" answers HTTP 500.
func seqServers(t *testing.T, build string, candidateReads, baselineReads []string) (candidateURL, baselineURL string, calls func() (int, int)) {
	t.Helper()
	var mu sync.Mutex
	var cn, bn int
	serve := func(reads []string, n *int, header bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			i := *n
			*n++
			mu.Unlock()
			body := reads[len(reads)-1]
			if i < len(reads) {
				body = reads[i]
			}
			if header {
				w.Header().Set("x-dev-health-build", build)
			}
			if body == "" {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"detail":"x"}`))
				return
			}
			_, _ = w.Write([]byte(body))
		})
	}
	c := httptest.NewServer(serve(candidateReads, &cn, true))
	t.Cleanup(c.Close)
	b := httptest.NewServer(referencePlane(serve(baselineReads, &bn, false)))
	t.Cleanup(b.Close)
	return c.URL, b.URL, func() (int, int) { mu.Lock(); defer mu.Unlock(); return cn, bn }
}

func gapTestState(delay, budget time.Duration) (*gapRereadState, *[]time.Duration) {
	slept := &[]time.Duration{}
	g := newGapRereadState(delay, budget)
	g.sleep = func(ctx context.Context, d time.Duration) error {
		*slept = append(*slept, d)
		return ctx.Err()
	}
	return g, slept
}

// TestProveOneRESTRequest_GapReread drives the delayed stage through the
// real proveOneRESTRequest: bracketed re-read (value leaves only), then
// the delayed reads of both planes.
func TestProveOneRESTRequest_GapReread(t *testing.T) {
	const build = "abc123def456"
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
	request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	const (
		c   = `{"a":2}`
		b1  = `{"a":1}`
		cx  = `{"a":2,"x":1}`
		bx1 = `{"a":1}`
	)
	for _, cell := range []struct {
		name         string
		cand, base   []string
		delay        time.Duration
		budget       time.Duration
		disabled     bool
		ctxTimeout   time.Duration
		wantOutcome  goapiproof.GapRereadOutcome
		wantOutside  int
		wantAdmitted bool
		wantCalls    [2]int // candidate, baseline
		wantSleeps   int
	}{
		{name: "value gap: B2 still in the gap, B3 caught up", cand: []string{c}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadAdmitted, wantOutside: 0, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "presence gap: never bracket-re-read, B3 caught up", cand: []string{cx}, base: []string{bx1, cx}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadAdmitted, wantOutside: 0, wantAdmitted: true, wantCalls: [2]int{2, 2}, wantSleeps: 1},
		{name: "pure presence gap: B3 caught up", cand: []string{`{"a":1,"x":1}`}, base: []string{`{"a":1}`, `{"a":1,"x":1}`}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadAdmitted, wantOutside: 0, wantAdmitted: true, wantCalls: [2]int{2, 2}, wantSleeps: 1},
		{name: "value: stable difference", cand: []string{c}, base: []string{b1}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadBaselineUnchanged, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "presence: stable difference", cand: []string{cx}, base: []string{bx1}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadBaselineUnchanged, wantOutside: 2, wantAdmitted: true, wantCalls: [2]int{2, 2}, wantSleeps: 1},
		{name: "candidate unstable: C2 differs", cand: []string{c, `{"a":9}`}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadCandidateUnstable, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "baseline moved to a third value", cand: []string{c}, base: []string{b1, b1, `{"a":7}`}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadNoCleanMatch, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "delayed baseline read HTTP 500: first verdict stands", cand: []string{c}, base: []string{b1, b1, ""}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadReadFailed, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "delayed candidate read HTTP 500", cand: []string{c, ""}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadReadFailed, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "delayed baseline not JSON", cand: []string{c}, base: []string{b1, b1, "not-json"}, delay: 15 * time.Second, budget: time.Minute,
			wantOutcome: goapiproof.GapRereadReadFailed, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{2, 3}, wantSleeps: 1},
		{name: "budget exhausted", cand: []string{c}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: 10 * time.Second,
			wantOutcome: goapiproof.GapRereadBudgetExhausted, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{1, 2}, wantSleeps: 0},
		{name: "run deadline too close", cand: []string{c}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: time.Minute, ctxTimeout: 20 * time.Second,
			wantOutcome: goapiproof.GapRereadDeadlinePressure, wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{1, 2}, wantSleeps: 0},
		{name: "stage disabled: main behaviour", cand: []string{c}, base: []string{b1, b1, c}, delay: 15 * time.Second, budget: time.Minute, disabled: true,
			wantOutcome: "", wantOutside: 1, wantAdmitted: true, wantCalls: [2]int{1, 2}, wantSleeps: 0},
	} {
		t.Run(cell.name, func(t *testing.T) {
			candidateURL, baselineURL, calls := seqServers(t, build, cell.cand, cell.base)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 2 * time.Second}
			g, slept := gapTestState(cell.delay, cell.budget)
			if !cell.disabled {
				f.gapReread = g
			}
			ctx := context.Background()
			if cell.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, cell.ctxTimeout)
				defer cancel()
			}
			writer := &fakeReceiptWriter{}
			out, err := proveOneRESTRequest(ctx, goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			cn, bn := calls()
			t.Logf("%s -> %s", cell.name, out.line())
			if out.Admitted != cell.wantAdmitted || out.DifferencesOutsideBaselineDefect != cell.wantOutside || cn != cell.wantCalls[0] || bn != cell.wantCalls[1] || len(*slept) != cell.wantSleeps {
				t.Fatalf("admitted=%v outside=%d candidate calls=%d baseline calls=%d sleeps=%d; want %v %d %v sleeps %d",
					out.Admitted, out.DifferencesOutsideBaselineDefect, cn, bn, len(*slept), cell.wantAdmitted, cell.wantOutside, cell.wantCalls, cell.wantSleeps)
			}
			if cell.wantOutcome == "" {
				if out.GapReread != nil {
					t.Fatalf("gap reread record %+v, want none", out.GapReread)
				}
				return
			}
			if out.GapReread == nil || out.GapReread.Outcome != cell.wantOutcome {
				t.Fatalf("gap reread = %+v, want outcome %s", out.GapReread, cell.wantOutcome)
			}
			if !strings.Contains(out.line(), "gap_reread="+string(cell.wantOutcome)) {
				t.Fatalf("line %q does not name the outcome", out.line())
			}
			if out.ObservedAt.IsZero() || out.CandidateObservedAt.IsZero() {
				t.Fatalf("per-case observed_at missing: %v %v", out.ObservedAt, out.CandidateObservedAt)
			}
			cited := false
			for _, d := range out.BaselineDefectsMatched {
				if d == goapiproof.GapRereadCitation {
					cited = true
				}
			}
			if cited != (cell.wantOutcome == goapiproof.GapRereadAdmitted) {
				t.Fatalf("baseline_defect=%v: citation present=%v", out.BaselineDefectsMatched, cited)
			}
			if cell.wantOutcome == goapiproof.GapRereadAdmitted {
				if len(writer.receipts) != 1 || writer.receipts[0].DifferencesOutsideBaselineDefect != 0 || out.TerminalState != goapiproof.TerminalStateMismatch {
					t.Fatalf("receipts=%d terminal=%q; want one receipt with outside 0 on the mismatch terminal state", len(writer.receipts), out.TerminalState)
				}
				if got := gapAdmittedByOperation([]outcome{out}); got["REST:GET:/thing"] != 1 {
					t.Fatalf("gapAdmittedByOperation = %v", got)
				}
			}
		})
	}
}

// The bracketed re-read's own verdicts are final: admitted, refused and
// R7 stands never reach the delayed stage.
func TestProveOneRESTRequest_GapRereadNeverOverridesTheBracketedVerdict(t *testing.T) {
	const build = "abc123def456"
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
	request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	for _, cell := range []struct {
		name       string
		cand, base []string
		wantSleeps int
	}{
		{"bracket admits (B2 == C1)", []string{`{"a":2}`}, []string{`{"a":1}`, `{"a":2}`}, 0},
		{"bracket refuses (third value)", []string{`{"a":2}`}, []string{`{"a":1}`, `{"a":7}`}, 0},
		{"bracket R7 stands (leaf agrees, other differs)", []string{`{"a":2,"b":2}`}, []string{`{"a":1,"b":1}`, `{"a":2,"b":5}`}, 0},
	} {
		t.Run(cell.name, func(t *testing.T) {
			candidateURL, baselineURL, _ := seqServers(t, build, cell.cand, cell.base)
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 2 * time.Second}
			g, slept := gapTestState(15*time.Second, time.Minute)
			f.gapReread = g
			out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), &fakeReceiptWriter{}, nil, false, nil)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			if out.GapReread != nil || len(*slept) != cell.wantSleeps {
				t.Fatalf("gap reread %+v, sleeps %d; want none", out.GapReread, len(*slept))
			}
		})
	}
}

func TestGapRereadState_ContextEndsDuringTheDelay(t *testing.T) {
	g := newGapRereadState(time.Hour, 2*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record, _, _ := g.run(ctx, goapiproof.NewLegClient(0), flags{}, goapiproof.RESTEndpointSpec{}, goapiproof.RESTRequest{}, nil, nil, 0, "", goapiproof.RESTAdmission{}, goapiproof.Result{}, nil)
	if record.Outcome != goapiproof.GapRereadCancelled {
		t.Fatalf("outcome %s, want cancelled_during_delay", record.Outcome)
	}
}

func TestGapRereadFlagBounds(t *testing.T) {
	g := newGapRereadState(15*time.Second, 30*time.Second)
	for i, want := range []bool{true, true, false} {
		if _, ok := g.reserve(context.Background(), time.Second); ok != want {
			t.Fatalf("reserve %d ok=%v, want %v", i, ok, want)
		}
	}
}

func TestValidateGapRereadFlags(t *testing.T) {
	for _, cell := range []struct {
		delay, budget time.Duration
		ok            bool
	}{
		{0, 0, true}, {0, time.Minute, true}, {15 * time.Second, 4 * time.Minute, true},
		{60 * time.Second, 60 * time.Second, true}, {60*time.Second + 1, time.Hour, false},
		{-1, time.Minute, false}, {15 * time.Second, 15*time.Second - 1, false}, {15 * time.Second, 15 * time.Second, true},
	} {
		if err := validateGapRereadFlags(cell.delay, cell.budget); (err == nil) != cell.ok {
			t.Errorf("delay=%s budget=%s: err=%v, want ok=%v", cell.delay, cell.budget, err, cell.ok)
		}
	}
}

// A re-read admission's receipt names BOTH pairs: the first comparison (the
// mismatch the citation excuses) in its response refs, and the pair the
// admission stands on in review_evidence. Every cell runs the real
// proveOneRESTRequest against real HTTP servers and a real artifact store;
// refs are content digests, so each is checked against the body it must hold.
func TestProveOneRESTRequest_ReceiptNamesTheAdmittedPair(t *testing.T) {
	const build = "abc123def456"
	spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/thing"}
	request := goapiproof.RESTRequest{Name: "thing", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	digestRef := func(store *goapiproof.ArtifactStore, body string) string {
		ref, err := store.Put([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	for _, cell := range []struct {
		name                   string
		cand, base             []string
		wantAdmittedB, wantAdC string // body each admitted ref must hold; "" = no admitted refs
	}{
		{"delayed gap admission: B3 and C1", []string{`{"a":2}`}, []string{`{"a":1}`, `{"a":1}`, `{"a":2}`}, `{"a":2}`, `{"a":2}`},
		{"delayed gap admission, presence: B3 and C1", []string{`{"a":2,"x":1}`}, []string{`{"a":1}`, `{"a":2,"x":1}`}, `{"a":2,"x":1}`, `{"a":2,"x":1}`},
		{"bracketed write-skew admission: B2 and C1", []string{`{"a":2}`}, []string{`{"a":1}`, `{"a":2}`}, `{"a":2}`, `{"a":2}`},
		{"stable difference: no admitted pair", []string{`{"a":2}`}, []string{`{"a":1}`}, "", ""},
		{"delayed reread refused (third value): no admitted pair", []string{`{"a":2}`}, []string{`{"a":1}`, `{"a":1}`, `{"a":7}`}, "", ""},
	} {
		t.Run(cell.name, func(t *testing.T) {
			candidateURL, baselineURL, _ := seqServers(t, build, cell.cand, cell.base)
			store, err := goapiproof.NewArtifactStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			f := flags{queryAPIURL: candidateURL, pythonAPIURL: baselineURL, org: "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: 2 * time.Second}
			f.gapReread, _ = gapTestState(15*time.Second, time.Minute)
			writer := &fakeReceiptWriter{}
			if _, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/thing", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, store, false, nil); err != nil {
				t.Fatal(err)
			}
			if len(writer.receipts) != 1 {
				t.Fatalf("receipts = %d, want 1", len(writer.receipts))
			}
			receipt := writer.receipts[0]
			var evidence restReviewEvidence
			if err := json.Unmarshal([]byte(receipt.ReviewEvidence), &evidence); err != nil {
				t.Fatalf("review_evidence %q: %v", receipt.ReviewEvidence, err)
			}
			// The first comparison always stays in the response refs.
			if receipt.BaselineResponseRef != digestRef(store, cell.base[0]) || receipt.CandidateResponseRef != digestRef(store, cell.cand[0]) {
				t.Fatalf("response refs %q/%q are not the first pair", receipt.BaselineResponseRef, receipt.CandidateResponseRef)
			}
			if cell.wantAdmittedB == "" {
				if evidence.AdmittedBaselineRef != "" || evidence.AdmittedCandidateRef != "" {
					t.Fatalf("admitted refs %q/%q on a case no re-read admitted", evidence.AdmittedBaselineRef, evidence.AdmittedCandidateRef)
				}
				return
			}
			if evidence.AdmittedBaselineRef != digestRef(store, cell.wantAdmittedB) || evidence.AdmittedCandidateRef != digestRef(store, cell.wantAdC) {
				t.Fatalf("admitted refs %q/%q; want the digests of %s / %s", evidence.AdmittedBaselineRef, evidence.AdmittedCandidateRef, cell.wantAdmittedB, cell.wantAdC)
			}
			if evidence.AdmittedBaselineRef == receipt.BaselineResponseRef {
				t.Fatal("admitted baseline ref equals the first baseline ref: the baseline did not change")
			}
		})
	}
}
