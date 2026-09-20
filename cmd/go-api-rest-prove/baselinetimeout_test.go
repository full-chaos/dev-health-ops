package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

type timeoutLeg struct {
	stall  bool
	closed bool
	status int
	build  string
	body   string
}

func startTimeoutLeg(t *testing.T, leg timeoutLeg, calls *int, mu *sync.Mutex) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*calls++
		mu.Unlock()
		if leg.stall {
			<-r.Context().Done()
			return
		}
		if leg.build != "" {
			w.Header().Set("x-dev-health-build", leg.build)
		} else {
			w.Header().Set("Server", goapiproof.ReferencePlaneServer)
		}
		if leg.status != 0 {
			w.WriteHeader(leg.status)
		}
		_, _ = w.Write([]byte(leg.body))
	}))
	if leg.closed {
		srv.Close()
	} else {
		t.Cleanup(srv.Close)
	}
	return srv.URL
}

// TestProveOneRESTRequest_BaselineTimeoutDeclaration runs one cell for each
// combination of baseline behaviour, candidate behaviour and declaration
// presence through the production entry point. The admitted cell checks the
// receipt the enablement predicate reads (mismatch, cited, nothing outside, no
// baseline body); every other cell checks no receipt is written and names the
// refusal, so the declaration is falsifiable on each leg.
func TestProveOneRESTRequest_BaselineTimeoutDeclaration(t *testing.T) {
	const build = "abc123def456"
	const ticket = "ABC-123"
	good := timeoutLeg{build: build, body: `{"items":[{"id":"x"}]}`}
	answer := timeoutLeg{body: `{"items":[{"id":"x"}]}`}
	stall := timeoutLeg{stall: true}
	decl := func(min time.Duration) *goapiproof.BaselineTimeoutDeclaration {
		return &goapiproof.BaselineTimeoutDeclaration{Ticket: ticket, Reason: "fixture", MinTimeout: min, NonEmptyPaths: []string{"data.items"}}
	}
	const budget = 300 * time.Millisecond

	for _, cell := range []struct {
		name               string
		baseline, cand     timeoutLeg
		decl               *goapiproof.BaselineTimeoutDeclaration
		wantRefusal        string
		wantCandidateCalls int
		wantTimeoutCited   bool // receipt cites the ticket and carries no baseline body
		wantReceipt        bool
	}{
		{"baseline stalls, candidate answers, declared: admitted on the candidate", stall, good, decl(250 * time.Millisecond), "", 1, true, true},
		{"baseline stalls, no declaration: refused as before, candidate never sent", stall, good, nil, goapiproof.RESTRefusalBaselineLegTimedOut, 0, false, false},
		{"baseline stalls shorter than the declared minimum wait", stall, good, decl(time.Hour), goapiproof.RESTRefusalBaselineTimeoutTooShort, 0, false, false},
		{"baseline stalls, candidate list empty", stall, timeoutLeg{build: build, body: `{"items":[]}`}, decl(250 * time.Millisecond), goapiproof.RESTRefusalCandidateEmptyUnderBaselineTimeout, 1, false, false},
		{"baseline stalls, candidate 500", stall, timeoutLeg{build: build, status: 500, body: `{"items":[1]}`}, decl(250 * time.Millisecond), goapiproof.RESTRefusalUnexpectedStatus, 1, false, false},
		{"baseline stalls, candidate wrong build", stall, timeoutLeg{build: "other", body: `{"items":[1]}`}, decl(250 * time.Millisecond), goapiproof.RESTRefusalBuildUnbound, 1, false, false},
		{"baseline stalls, candidate stalls", stall, stall, decl(250 * time.Millisecond), goapiproof.RESTRefusalCandidateLegTimedOut, 1, false, false},
		{"baseline stalls, candidate transport error", stall, timeoutLeg{closed: true}, decl(250 * time.Millisecond), goapiproof.RESTRefusalCandidateLegTransportError, 0, false, false},
		{"baseline answers, declared: declaration void, ordinary comparison", answer, good, decl(250 * time.Millisecond), "", 1, false, true},
		{"baseline 500, declared: refused on status, not admitted on silence", timeoutLeg{status: 500, body: `{}`}, good, decl(250 * time.Millisecond), goapiproof.RESTRefusalUnexpectedStatus, 1, false, false},
		{"baseline transport error, declared: refused, not a timeout", timeoutLeg{closed: true}, good, decl(250 * time.Millisecond), goapiproof.RESTRefusalBaselineLegTransportError, 0, false, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var mu sync.Mutex
			var baselineCalls, candidateCalls int
			f := flags{
				pythonAPIURL: startTimeoutLeg(t, cell.baseline, &baselineCalls, &mu),
				queryAPIURL:  startTimeoutLeg(t, cell.cand, &candidateCalls, &mu),
				org:          "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: time.Minute,
			}
			spec := goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things"}
			request := goapiproof.RESTRequest{
				Name: "team", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
				Timeout: budget, BaselineTimeoutDeclared: cell.decl,
			}
			writer := &fakeReceiptWriter{}
			out, err := proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/things", spec, request,
				staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
			if err != nil {
				t.Fatalf("proveOneRESTRequest: %v", err)
			}
			t.Logf("%s", out.line())
			mu.Lock()
			calls := candidateCalls
			mu.Unlock()
			if out.Refusal != cell.wantRefusal || calls != cell.wantCandidateCalls {
				t.Fatalf("refusal=%q candidate calls=%d, want %q / %d", out.Refusal, calls, cell.wantRefusal, cell.wantCandidateCalls)
			}
			if (len(writer.receipts) == 1) != cell.wantReceipt || len(writer.receipts) > 1 {
				t.Fatalf("receipts written = %d, want written=%v", len(writer.receipts), cell.wantReceipt)
			}
			if out.Refusal != "" && out.Admitted {
				t.Fatalf("a refused outcome must not be admitted: %+v", out)
			}
			if !cell.wantReceipt {
				return
			}
			receipt := writer.receipts[0]
			cited := len(receipt.BaselineDefects) == 1 && receipt.BaselineDefects[0] == ticket
			if cited != cell.wantTimeoutCited {
				t.Fatalf("baseline_defect = %v, want cited=%v", receipt.BaselineDefects, cell.wantTimeoutCited)
			}
			if !cell.wantTimeoutCited {
				if out.BaselineTimedOutAfter != "" || strings.Contains(receipt.ReviewEvidence, "baseline_timed_out_after") {
					t.Fatalf("an ordinary comparison must not claim a baseline timeout: %+v %s", out, receipt.ReviewEvidence)
				}
				return
			}
			if receipt.TerminalState != goapiproof.EnablementCitedMismatchState || receipt.DifferencesOutsideBaselineDefect != 0 ||
				receipt.BuildBinding != goapiproof.EdgeBuildPresent || receipt.CandidateBuild != build ||
				receipt.BaselineResponseRef != "" || receipt.Stage != goapiproof.EnablementProofStage {
				t.Fatalf("receipt = %+v, want the cited-mismatch shape with no baseline body", receipt)
			}
			if !strings.HasPrefix(out.BaselineTimedOutAfter, "0.") || !strings.Contains(out.line(), "baseline timed out after "+out.BaselineTimedOutAfter) ||
				!strings.Contains(receipt.ReviewEvidence, `"baseline_timed_out_after":"`+out.BaselineTimedOutAfter+`"`) {
				t.Fatalf("the measured wait must be on the report line and the receipt: line=%q evidence=%q", out.line(), receipt.ReviewEvidence)
			}
		})
	}
}

// A run that is cut by its own deadline while the baseline waits is not a
// baseline timeout: the declaration must not fire.
func TestProveOneRESTRequest_RunDeadlineDuringBaselineIsNotABaselineTimeout(t *testing.T) {
	const build = "abc123def456"
	var mu sync.Mutex
	var b, c int
	f := flags{
		pythonAPIURL: startTimeoutLeg(t, timeoutLeg{stall: true}, &b, &mu),
		queryAPIURL:  startTimeoutLeg(t, timeoutLeg{build: build, body: `{"items":[1]}`}, &c, &mu),
		org:          "org-1", recordedBy: "chris", reviewEvidence: "test", timeout: time.Minute,
	}
	request := goapiproof.RESTRequest{
		Name: "team", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON,
		Timeout:                 time.Minute,
		BaselineTimeoutDeclared: &goapiproof.BaselineTimeoutDeclaration{Ticket: "ABC-123", Reason: "fixture", MinTimeout: 0, NonEmptyPaths: []string{"data.items"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	writer := &fakeReceiptWriter{}
	out, err := proveOneRESTRequest(ctx, goapiproof.NewLegClient(0), f, "REST:GET:/things", goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/things"}, request,
		staticCredentialForTest(), staticCredentialForTest(), build, goapiproof.AuthContext{}, time.Now().UTC(), writer, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if out.Refusal != goapiproof.RESTRefusalBaselineLegCutByRunDeadline || c != 0 || len(writer.receipts) != 0 {
		t.Fatalf("refusal=%q candidate calls=%d receipts=%d, want the run-deadline refusal and nothing else", out.Refusal, c, len(writer.receipts))
	}
}
