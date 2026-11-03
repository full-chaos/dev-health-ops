package sessionissue_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

// With the workload credential the state request carries exactly one Bearer, the workload token, and no byte of a session
// credential; one exchange serves a state read and the issues of the token's lifetime.
func TestWorkloadStateCarriesOnlyTheWorkloadToken(t *testing.T) {
	s := newIssueServer(t)
	token := mintWorkloadToken(t)
	s.exchange = issuedAnswer(token)
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	runIssueCases(t, s, client)
	for i := 0; i < 3; i++ {
		stateRequestOf(t, s, client)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.states) != 3 {
		t.Fatalf("%d state requests, want 3", len(s.states))
	}
	for i, r := range s.states {
		if len(r.Auth) != 1 || r.Auth[0] != "Bearer "+token {
			t.Errorf("state request %d: Authorization is not exactly the workload token", i)
		}
		if strings.Contains(r.Dump, "svc_authsess_") {
			t.Errorf("state request %d holds svc_authsess_ bytes", i)
		}
	}
	if s.exchanges != 1 {
		t.Errorf("%d exchanges for %d issues and 3 state reads, want 1", s.exchanges, len(issueCases()))
	}
}

// The state request equals the session credential's (the golden) except the bearer.
func TestWorkloadStateRequestDiffersFromTheGoldenOnlyInTheBearer(t *testing.T) {
	s := newIssueServer(t)
	s.exchange = issuedAnswer(mintWorkloadToken(t))
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(stateGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var want []goldenRequest
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	got := stateRequestOf(t, s, client)
	got.Headers["Authorization"] = want[0].Headers["Authorization"]
	gotJSON, _ := json.Marshal([]goldenRequest{got})
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("workload state request differs from the golden beyond the bearer\n got %s\nwant %s", gotJSON, wantJSON)
	}
}

// A failed exchange fails the state read as unavailable and sends nothing to the state route.
func TestWorkloadStateExchangeFailureSendsNoRequest(t *testing.T) {
	s := newIssueServer(t)
	s.exchange = func() (int, string) { return http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}` }
	client, err := sessionissue.NewWorkload(s.URL, newWorkloadClient(t, s), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.State(context.Background(), goldenPredecessor)
	if !errors.Is(err, sessionissue.ErrUnavailable) {
		t.Fatalf("error %v, want unavailable", err)
	}
	if err != nil && (strings.Contains(err.Error(), "Bearer") || strings.Contains(err.Error(), "svc_authsess_")) {
		t.Errorf("the error carries a credential: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.states) != 0 {
		t.Fatalf("%d state requests after a failed exchange, want 0", len(s.states))
	}
}

// The workload state read answers a redirect without following it: the second origin sees no request.
func TestWorkloadStateNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	exchange := newIssueServer(t)
	exchange.exchange = issuedAnswer(mintWorkloadToken(t))
	client, err := sessionissue.NewWorkload(mustURL(t, probe.Base.URL), newWorkloadClient(t, exchange), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.State(context.Background(), goldenPredecessor); err == nil {
		t.Fatal("a redirect answer was taken for a state")
	}
	probe.Assert(t)
}
