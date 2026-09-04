package workgraph

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const testCompatibilityToken = "s3cret-bearer-token-value"

func newTestExecutor(t *testing.T, endpoint string, client *http.Client) *HTTPCompatibilityExecutor {
	t.Helper()
	executor, err := NewHTTPCompatibilityExecutor(client, HTTPCompatibilityConfig{
		Endpoint: endpoint, BearerToken: testCompatibilityToken, AllowInsecureInternal: true,
	})
	if err != nil {
		t.Fatalf("NewHTTPCompatibilityExecutor: %v", err)
	}
	return executor
}

// bridgeResponse describes one fake bridge reply. Every case below drives a
// DIFFERENT return site of Execute, so the table is a per-return-site
// enumeration rather than a sample: before this change all of them collapsed
// to one bare ErrUnavailable and the caller could not tell them apart.
type bridgeResponse struct {
	status int
	body   string
}

func TestExecuteClassifiesEveryBridgeResponseReturnSite(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		response bridgeResponse
		want     error
		notWant  []error
	}{
		{
			// A rejected credential is a decision the bridge made about the
			// request; it never ran the work.
			name:     "401 unauthorized is refused",
			response: bridgeResponse{status: http.StatusUnauthorized, body: `{"detail":"bad token"}`},
			want:     ErrCompatibilityRefused,
			notWant:  []error{ErrCompatibilityUnknown, ErrCompatibilityNotSent},
		},
		{
			// 409 is the ambiguous-claim refusal shape. It is still a
			// decision, not an interrupted attempt.
			name:     "409 conflict is refused",
			response: bridgeResponse{status: http.StatusConflict, body: `{"detail":{"reason":"ambiguous_refused"}}`},
			want:     ErrCompatibilityRefused,
			notWant:  []error{ErrCompatibilityUnknown, ErrCompatibilityNotSent},
		},
		{
			// 408 is pulled OUT of the 4xx refused range on purpose: the
			// bridge can time out after it already started work.
			name:     "408 request timeout is unknown",
			response: bridgeResponse{status: http.StatusRequestTimeout, body: `{}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			name:     "429 too many requests is unknown",
			response: bridgeResponse{status: http.StatusTooManyRequests, body: `{}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			name:     "500 internal error is unknown",
			response: bridgeResponse{status: http.StatusInternalServerError, body: `{"detail":"boom"}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			// A 2xx this side cannot decode most likely means the bridge DID
			// run and the evidence was lost in transit -- Unknown, never
			// Refused, or a completed execution would be retried as if it
			// had been declined.
			name:     "malformed 200 body is unknown",
			response: bridgeResponse{status: http.StatusOK, body: `{"status":"suc`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			// The bridge RAISES on failure, so a non-success word in a 2xx
			// body is undocumented -- version skew, not a decline. Cannot
			// prove it did not commit, so Unknown.
			name:     "unrecognised status in a 200 body is unknown, not refused",
			response: bridgeResponse{status: http.StatusOK, body: `{"status":"declined"}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			// r2 P1. The bridge reports success only AFTER committing, so a
			// success whose evidence is unusable means the work HAPPENED and
			// only the proof was lost. Refused would make it retryable and
			// re-run an applied execution; this case previously codified
			// exactly that mistake.
			name:     "success with unusable output evidence is unknown, never refused",
			response: bridgeResponse{status: http.StatusOK, body: `{"status":"success"}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			name:     "success with a non-object output evidence is unknown",
			response: bridgeResponse{status: http.StatusOK, body: `{"status":"success","output_evidence":1}`},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
		{
			// The body is read before the status is judged, so an oversize
			// body on an otherwise fine response is its own return site.
			name:     "oversize body is unknown",
			response: bridgeResponse{status: http.StatusOK, body: strings.Repeat("x", maxCompatibilityResponseBytes+64)},
			want:     ErrCompatibilityUnknown,
			notWant:  []error{ErrCompatibilityRefused, ErrCompatibilityNotSent},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(testCase.response.status)
				_, _ = writer.Write([]byte(testCase.response.body))
			}))
			defer server.Close()
			executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 5 * time.Second})
			_, err := executor.Execute(t.Context(), *testClaim(time.Second))
			if err == nil {
				t.Fatal("Execute succeeded, want a classified failure")
			}
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Execute = %v, want it to wrap %v", err, testCase.want)
			}
			for _, unwanted := range testCase.notWant {
				if errors.Is(err, unwanted) {
					t.Fatalf("Execute = %v, must not also wrap %v", err, unwanted)
				}
			}
			// Every classified failure must still read as ErrUnavailable, or
			// a caller that only asks the old question silently changes
			// behaviour on upgrade.
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Execute = %v, want it to still wrap ErrUnavailable", err)
			}
			wantStatus := fmt.Sprintf("status=%d", testCase.response.status)
			if !strings.Contains(err.Error(), wantStatus) {
				t.Fatalf("Execute = %q, want it to carry %q", err.Error(), wantStatus)
			}
		})
	}
}

// A refused dial is the one transport failure that PROVES no byte reached the
// bridge -- the case a real ECONNREFUSED produces, not a synthesized error
// value, so the classifier is exercised against what the net stack actually
// returns.
func TestExecuteClassifiesConnectionRefusedAsNotSent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + listener.Addr().String() + "/internal/worker/workgraph/v1/execute"
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	executor := newTestExecutor(t, endpoint, &http.Client{Timeout: 5 * time.Second})
	_, executeErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if executeErr == nil {
		t.Fatal("Execute succeeded against a closed port")
	}
	if !errors.Is(executeErr, ErrCompatibilityNotSent) {
		t.Fatalf("Execute = %v, want ErrCompatibilityNotSent", executeErr)
	}
	if errors.Is(executeErr, ErrCompatibilityUnknown) || errors.Is(executeErr, ErrCompatibilityRefused) {
		t.Fatalf("Execute = %v, must not also be Unknown or Refused", executeErr)
	}
}

// An unresolvable host fails before the dial for a different reason
// (*net.DNSError, not a dial *net.OpError), so it is a separate NotSent path
// from the refused-connection test above.
func TestExecuteClassifiesDNSFailureAsNotSent(t *testing.T) {
	endpoint := "http://no-such-workgraph-bridge-8f3a2c1e.internal/internal/worker/workgraph/v1/execute"
	executor := newTestExecutor(t, endpoint, &http.Client{Timeout: 5 * time.Second})
	_, executeErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if executeErr == nil {
		t.Fatal("Execute succeeded against an unresolvable host")
	}
	if !errors.Is(executeErr, ErrCompatibilityNotSent) {
		t.Fatalf("Execute = %v, want ErrCompatibilityNotSent", executeErr)
	}
}

// A deadline that fires after the request was written may have been received
// and acted on, so it must stay Unknown -- classifying it NotSent would
// retry an execution that already ran.
func TestExecuteClassifiesTimeoutAfterSendAsUnknown(t *testing.T) {
	released := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		<-released
		writer.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(released)
		server.Close()
	}()
	executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 150 * time.Millisecond})
	_, executeErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if executeErr == nil {
		t.Fatal("Execute succeeded against a hanging bridge")
	}
	if !errors.Is(executeErr, ErrCompatibilityUnknown) {
		t.Fatalf("Execute = %v, want ErrCompatibilityUnknown", executeErr)
	}
	if errors.Is(executeErr, ErrCompatibilityNotSent) {
		t.Fatalf("Execute = %v, a post-send timeout must never read as NotSent", executeErr)
	}
}

// The local-validation and wiring return sites never touch the network at
// all, so they are unambiguously NotSent.
func TestExecuteClassifiesLocalRejectionsAsNotSent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"status":"success","output_evidence":{"edges":1}}`))
	}))
	defer server.Close()
	endpoint := server.URL + "/internal/worker/workgraph/v1/execute"

	var nilExecutor *HTTPCompatibilityExecutor
	if _, err := nilExecutor.Execute(t.Context(), *testClaim(time.Second)); !errors.Is(err, ErrCompatibilityNotSent) {
		t.Fatalf("nil executor = %v, want ErrCompatibilityNotSent", err)
	}
	if _, err := (&HTTPCompatibilityExecutor{}).Execute(t.Context(), *testClaim(time.Second)); !errors.Is(err, ErrCompatibilityNotSent) {
		t.Fatalf("unconfigured executor = %v, want ErrCompatibilityNotSent", err)
	}

	executor := newTestExecutor(t, endpoint, &http.Client{Timeout: 5 * time.Second})
	badToken := *testClaim(time.Second)
	badToken.Token = "not-a-uuid"
	if _, err := executor.Execute(t.Context(), badToken); !errors.Is(err, ErrCompatibilityNotSent) {
		t.Fatalf("invalid claim token = %v, want ErrCompatibilityNotSent", err)
	}
	badRequest := *testClaim(time.Second)
	badRequest.Request.ID = "not-a-uuid"
	if _, err := executor.Execute(t.Context(), badRequest); !errors.Is(err, ErrCompatibilityNotSent) {
		t.Fatalf("invalid request = %v, want ErrCompatibilityNotSent", err)
	}
}

// The wrapped detail travels into the ledger's failure_detail column, so it
// must never carry the bearer token -- including on the paths where the
// endpoint URL itself ends up in the error text.
func TestClassifiedFailureNeverCarriesTheBearerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Echo the credential back, which is the worst case a hostile or
		// merely careless bridge could do.
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"detail":"` + request.Header.Get("Authorization") + `"}`))
	}))
	defer server.Close()
	executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 5 * time.Second})
	_, err := executor.Execute(t.Context(), *testClaim(time.Second))
	if err == nil {
		t.Fatal("Execute succeeded, want a classified failure")
	}
	// A control: the assertion below is only meaningful if the token would
	// otherwise be visible, so prove the echo actually happened by checking
	// the same body reaches a plain read.
	if !strings.Contains(err.Error(), "status=400") {
		t.Fatalf("Execute = %q, want the 400 status recorded", err.Error())
	}
	if strings.Contains(err.Error(), testCompatibilityToken) {
		t.Fatalf("Execute = %q leaked the bearer token", err.Error())
	}
	if strings.Contains(compatibilityAmbiguousDetail(err), testCompatibilityToken) {
		t.Fatalf("ledger detail leaked the bearer token: %q", compatibilityAmbiguousDetail(err))
	}
}

// The inverse of the leak test above, and the reason it is not vacuous: the
// bridge's own response text does NOT reach the detail at all any more. This
// replaced an earlier test asserting the opposite -- carrying the bridge's
// prose was how a JSON-escaped bearer token got through (r1 P1b): an
// exact-substring redaction cannot match a token that untrusted text has
// re-encoded, so nothing free-form crosses this boundary now.
func TestBridgeResponseTextNeverReachesTheDurableDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"detail":{"reason":"ambiguous_refused","state":"ambiguous"}}`))
	}))
	defer server.Close()
	executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 5 * time.Second})
	_, err := executor.Execute(t.Context(), *testClaim(time.Second))
	if err == nil {
		t.Fatal("Execute succeeded, want a classified failure")
	}
	detail := compatibilityAmbiguousDetail(err)
	for _, leaked := range []string{"ambiguous_refused", "detail", "{", "}"} {
		if strings.Contains(detail, leaked) {
			t.Fatalf("ledger detail %q carries bridge response text (%q)", detail, leaked)
		}
	}
	// The discriminator that DOES survive: the classification and the status.
	if !strings.Contains(detail, "status=409") || !errors.Is(err, ErrCompatibilityRefused) {
		t.Fatalf("detail %q lost the discriminator it is supposed to keep", detail)
	}
}

// r1 P1a. http.NewRequestWithContext sets GetBody for a *bytes.Reader, and
// net/http replays the body on 307/308 -- so a bridge can EXECUTE the request
// and then redirect. If the redirected hop fails to dial, the error is an
// ordinary dial failure, which would be classified NotSent and RETRIED,
// re-running work the bridge already did. Refusing redirects makes the 3xx a
// response, classified Unknown by status.
func TestRedirectIsRefusedSoAnExecutedRequestIsNeverRetriedAsNotSent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	var served int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		served++
		writer.Header().Set("Location", "http://"+dead+"/internal/worker/workgraph/v1/execute")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 5 * time.Second})
	_, execErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if execErr == nil {
		t.Fatal("Execute succeeded against a redirect to a dead address")
	}
	if errors.Is(execErr, ErrCompatibilityNotSent) {
		t.Fatalf("an executed-then-redirected request was classified NotSent: %v", execErr)
	}
	if !errors.Is(execErr, ErrCompatibilityUnknown) {
		t.Fatalf("Execute = %v, want ErrCompatibilityUnknown", execErr)
	}
	if !strings.Contains(execErr.Error(), "status=307") {
		t.Fatalf("Execute = %q, want the 307 recorded rather than the redirected hop's failure", execErr.Error())
	}
	// The redirect must not be FOLLOWED at all -- one request reaches the
	// bridge, never a replay. Without this the test would still pass if the
	// body were re-sent and the classification merely happened to differ.
	if served != 1 {
		t.Fatalf("bridge saw %d requests, want exactly 1 (the body must never be replayed)", served)
	}
}

// A bearer token containing a character that JSON escapes is the exact shape
// that defeated the first redaction: the raw substring never appears in the
// response body, only its escaped form does.
func TestJSONEscapableBearerTokenNeverReachesTheDurableDetail(t *testing.T) {
	token := `ab"cd`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		escaped := strings.ReplaceAll(request.Header.Get("Authorization"), `"`, `\"`)
		_, _ = writer.Write([]byte(`{"detail":"rejected ` + escaped + `"}`))
	}))
	defer server.Close()
	executor, err := NewHTTPCompatibilityExecutor(&http.Client{Timeout: 5 * time.Second},
		HTTPCompatibilityConfig{
			Endpoint:    server.URL + "/internal/worker/workgraph/v1/execute",
			BearerToken: token, AllowInsecureInternal: true,
		})
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if execErr == nil {
		t.Fatal("Execute succeeded, want a classified failure")
	}
	detail := compatibilityAmbiguousDetail(execErr)
	for _, form := range []string{token, `ab\"cd`, `ab"cd`, "cd"} {
		if strings.Contains(detail, form) {
			t.Fatalf("ledger detail %q carries the bearer token as %q", detail, form)
		}
	}
}

// classifyingExecutor returns a fixed pre-classified error, so the handler's
// own branch is exercised without a network at all.
type classifyingExecutor struct{ err error }

func (executor classifyingExecutor) Execute(context.Context, Claim) ([]byte, error) {
	return nil, executor.err
}

func TestHandlerRetriesNotSentAndRefusedWithoutReleasingAmbiguous(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "not sent", err: compatibilityFailure(ErrCompatibilityNotSent, 0, "dial tcp: connect: connection refused")},
		{name: "refused", err: compatibilityFailure(ErrCompatibilityRefused, http.StatusUnauthorized, `{"detail":"bad token"}`)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := &fakeStore{claim: testClaim(time.Second)}
			handler, err := NewBuildHandler(store, classifyingExecutor{err: testCase.err}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			workErr := handler.Work(context.Background(), buildExecution())
			if workErr == nil {
				t.Fatal("Work succeeded, want a retryable failure")
			}
			if !strings.Contains(workErr.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryRetryable)
			}
			if strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
				t.Fatalf("Work = %v, must not be Permanent", workErr)
			}
			// The whole point: this request must NOT be parked in a state
			// only a human /repair call can leave.
			if store.ambiguous != 0 {
				t.Fatalf("ambiguous releases = %d, want 0", store.ambiguous)
			}
			if store.completions != 0 {
				t.Fatalf("completions = %d, want 0", store.completions)
			}
		})
	}
}

func TestHandlerReleasesUnknownAmbiguousWithTheClassifiedDetail(t *testing.T) {
	executeErr := compatibilityFailure(ErrCompatibilityUnknown, http.StatusInternalServerError, `{"detail":"bridge exploded"}`)
	store := &fakeStore{claim: testClaim(time.Second)}
	handler, err := NewBuildHandler(store, classifyingExecutor{err: executeErr}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	workErr := handler.Work(context.Background(), buildExecution())
	if workErr == nil || !strings.Contains(workErr.Error(), string(jobruntime.CategoryPermanent)) {
		t.Fatalf("Work = %v, want category %s", workErr, jobruntime.CategoryPermanent)
	}
	if store.ambiguous != 1 {
		t.Fatalf("ambiguous releases = %d, want 1", store.ambiguous)
	}
	// The fixed literal is exactly what made 22 ledger rows indistinguishable
	// from each other. The detail that reaches the store must now name the
	// classification, the status, and the bridge's own text.
	detail := store.lastAmbiguousDetail
	if detail == "compatibility execution outcome is unknown" {
		t.Fatalf("ledger detail is still the fixed literal: %q", detail)
	}
	for _, want := range []string{"outcome is unknown", "status=500", "bridge exploded"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("ledger detail %q is missing %q", detail, want)
		}
	}
	if length := utf8.RuneCountInString(detail); length == 0 || length > maxAmbiguousDetailBytes {
		t.Fatalf("ledger detail length = %d, want 1..%d", length, maxAmbiguousDetailBytes)
	}
}

// A pre-step failure is not a classified bridge outcome, so it keeps today's
// ambiguous release -- but it must still produce a NON-EMPTY detail, because
// PostgresStore.transition rejects an empty one outright.
func TestAmbiguousDetailIsNeverEmptyOrOverBound(t *testing.T) {
	if detail := compatibilityAmbiguousDetail(nil); detail == "" {
		t.Fatal("a nil error produced an empty ledger detail")
	}
	if detail := compatibilityAmbiguousDetail(errors.New("\x00\x01\x02")); detail == "" {
		t.Fatal("an all-control-character error produced an empty ledger detail")
	}
	long := compatibilityAmbiguousDetail(errors.New(strings.Repeat("verbose ", 4096)))
	if len(long) > maxAmbiguousDetailBytes {
		t.Fatalf("ledger detail is %d bytes, over the %d bound", len(long), maxAmbiguousDetailBytes)
	}
	if !utf8.ValidString(long) {
		t.Fatal("ledger detail is not valid UTF-8 after truncation")
	}
}

func TestSanitizeDetailStripsControlCharactersAndCutsOnRuneBoundaries(t *testing.T) {
	if got := sanitizeDetail("a\nb\tc", 64); got != "a b c" {
		t.Fatalf("sanitizeDetail = %q, want %q", got, "a b c")
	}
	if got := sanitizeDetail("   ", 64); got != "" {
		t.Fatalf("sanitizeDetail of whitespace = %q, want empty", got)
	}
	// Every rune here is 3 bytes; a naive byte cut would split one and
	// produce invalid UTF-8.
	multibyte := strings.Repeat("字", 40)
	for _, limit := range []int{1, 2, 3, 4, 7, 11, 100} {
		got := sanitizeDetail(multibyte, limit)
		if len(got) > limit {
			t.Fatalf("sanitizeDetail(limit=%d) = %d bytes", limit, len(got))
		}
		if !utf8.ValidString(got) {
			t.Fatalf("sanitizeDetail(limit=%d) = %q, not valid UTF-8", limit, got)
		}
	}
	if got := sanitizeDetail("anything", 0); got != "" {
		t.Fatalf("sanitizeDetail(limit=0) = %q, want empty", got)
	}
}

// The three sentinels must stay distinguishable from each other. A refactor
// that made any two of them the same value, or that dropped the ErrUnavailable
// wrapping, would silently restore the collapse this ticket removes.
func TestClassificationSentinelsAreDistinctAndWrapErrUnavailable(t *testing.T) {
	sentinels := map[string]error{
		"not sent": ErrCompatibilityNotSent,
		"refused":  ErrCompatibilityRefused,
		"unknown":  ErrCompatibilityUnknown,
	}
	for name, sentinel := range sentinels {
		if !errors.Is(sentinel, ErrUnavailable) {
			t.Fatalf("%s does not wrap ErrUnavailable", name)
		}
		for otherName, other := range sentinels {
			if name == otherName {
				continue
			}
			if errors.Is(sentinel, other) {
				t.Fatalf("%s is indistinguishable from %s", name, otherName)
			}
		}
	}
}

// The allowlist's own guard. This does NOT restate `refusedStatuses` -- it
// sweeps every status this executor can plausibly see and asserts that
// Refused is reachable ONLY for the enumerated codes. A future change that
// widens the retryable set has to change this test deliberately, which is the
// whole point: three review rounds each found something classified
// safe-to-retry that had already executed, and the fix for that class is a
// default, not another patch.
func TestOnlyTheCitedStatusesAreEverRefused(t *testing.T) {
	// Every status in these ranges, not a hand-picked sample -- a sampled
	// sweep is exactly how the previous shape kept missing one.
	var swept int
	for status := 200; status <= 599; status++ {
		swept++
		got := compatibilityStatusSentinel(status)
		_, allowlisted := refusedStatuses[status]
		if allowlisted {
			if !errors.Is(got, ErrCompatibilityRefused) {
				t.Fatalf("status %d is on the allowlist but classified %v", status, got)
			}
			continue
		}
		if errors.Is(got, ErrCompatibilityRefused) {
			t.Fatalf("status %d is NOT on the allowlist but was classified Refused -- "+
				"an uncited status must never be retryable", status)
		}
		if !errors.Is(got, ErrCompatibilityUnknown) {
			t.Fatalf("status %d fell through to %v, want Unknown", status, got)
		}
	}
	if swept != 400 {
		t.Fatalf("swept %d statuses, want the full 200-599 range", swept)
	}
	// The allowlist is exactly the three codes citable in the bridge's source.
	// If this fails, someone added a code -- check they cited a line for it.
	if len(refusedStatuses) != 3 {
		t.Fatalf("allowlist has %d entries, want 3 (401, 409, 422)", len(refusedStatuses))
	}
	for _, cited := range []int{401, 409, 422} {
		if _, ok := refusedStatuses[cited]; !ok {
			t.Fatalf("status %d is cited in the bridge source but missing from the allowlist", cited)
		}
	}
	// 403 specifically: it reads like a decline, but this bridge never emits
	// it, so it must NOT be retryable. This is the trap the allowlist exists
	// to hold shut.
	if _, present := refusedStatuses[403]; present {
		t.Fatal("403 is on the allowlist but appears nowhere in the bridge's source")
	}
}

// A 2xx whose status word is unrecognised means version skew against a bridge
// that RAISES on failure, so it cannot be read as "declined".
func TestUnrecognisedSuccessBodyStatusIsUnknownNotRefused(t *testing.T) {
	for _, body := range []string{
		`{"status":"declined"}`,
		`{"status":"error"}`,
		`{"status":""}`,
		`{}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(body))
		}))
		executor := newTestExecutor(t, server.URL+"/internal/worker/workgraph/v1/execute", &http.Client{Timeout: 5 * time.Second})
		_, err := executor.Execute(t.Context(), *testClaim(time.Second))
		server.Close()
		if err == nil {
			t.Fatalf("body %s: Execute succeeded unexpectedly", body)
		}
		if errors.Is(err, ErrCompatibilityRefused) {
			t.Fatalf("body %s classified Refused; an unrecognised status word cannot prove the bridge declined", body)
		}
		if !errors.Is(err, ErrCompatibilityUnknown) {
			t.Fatalf("body %s = %v, want Unknown", body, err)
		}
	}
}
