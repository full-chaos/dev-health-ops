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

// The issue answer's expires_in is the access token's lifetime auth-service chose; the pair hands it on so go-api's
// sign-in answers state it. A value outside 1..3600 (the access-token.v1 ceiling) is not a token pair; an answer without it is go-api's 3600.
func TestIssueHandsOnTheAnswersExpiresIn(t *testing.T) {
	for answer, want := range map[string]int64{
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":600}`:  600,
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3600}`: 3600,
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":0}`:    -1,
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer","expires_in":3601}`: -1,
		`{"access_token":"a.b.c","refresh_token":"dhr1_x","token_type":"bearer"}`:                   3600,
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
		if want < 0 {
			if err == nil {
				t.Errorf("%s: accepted, expires_in %d", answer, pair.ExpiresIn)
			}
			continue
		}
		if err != nil || pair.ExpiresIn != want {
			t.Errorf("%s: expires_in %d, err %v; want %d", answer, pair.ExpiresIn, err, want)
		}
	}
}
