package sessionissue

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Under auth-service's eddsa class the issue answer carries the session routes' token; the pair hands it on, and an
// answer without it (hs256_compat) leaves it empty.
func TestIssueHandsOnTheSessionRouteToken(t *testing.T) {
	for answer, want := range map[string]string{
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600,"session_access_token":"s.t.u"}`: "s.t.u",
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`:                                "",
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, answer)
		}))
		client, err := New(baseOf(t, server.URL), testToken(t), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		pair, err := client.Issue(context.Background(), endUserRequest(), Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}})
		server.Close()
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}
		if pair.Access != "a.b.c" || pair.SessionAccess != want {
			t.Fatalf("pair access %q session %q, want session %q", pair.Access, pair.SessionAccess, want)
		}
	}
}
