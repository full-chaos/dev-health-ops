package sessionissue_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const stateGoldenPath = "testdata/outgoing_state.golden.json"

// stateRequestOf is the outgoing state request of the fixed session id, as the server saw it.
func stateRequestOf(t *testing.T, s *issueServer, client *sessionissue.Client) goldenRequest {
	t.Helper()
	if _, err := client.State(context.Background(), goldenPredecessor); err != nil {
		t.Fatalf("state: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	got := s.stateLast
	got.Case = "state"
	return got
}

// The outgoing state request, with the session credential, is pinned by testdata/outgoing_state.golden.json.
func TestOutgoingStateRequestMatchesTheGolden(t *testing.T) {
	s := newIssueServer(t)
	client, err := sessionissue.New(s.URL, goldenToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := renderGolden([]goldenRequest{stateRequestOf(t, s, client)})
	if os.Getenv("SESSIONISSUE_WRITE_GOLDEN") == "1" {
		if err := os.WriteFile(stateGoldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(stateGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("outgoing state request differs from %s\n--- got\n%s\n--- want\n%s", stateGoldenPath, got, want)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.states) != 1 || len(s.states[0].Auth) != 1 || s.states[0].Auth[0] != "Bearer "+goldenToken {
		t.Errorf("the state request does not carry exactly the session credential")
	}
	if s.exchanges != 0 {
		t.Errorf("%d exchanges, want 0", s.exchanges)
	}
}
