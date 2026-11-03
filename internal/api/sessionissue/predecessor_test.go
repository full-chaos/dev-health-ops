package sessionissue

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A superseding issue (switch-org, CHAOS-12085) sends the caller's session as
// predecessor_session_id; every other issue sends the three-member body it
// always did, so an auth-service that predates the field keeps accepting it.
func TestIssueSendsThePredecessorOnlyWhenOneIsNamed(t *testing.T) {
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		_, _ = io.WriteString(w, `{"access_token":"a","refresh_token":"r","token_type":"bearer"}`)
	}))
	defer server.Close()
	client, err := New(baseOf(t, server.URL), testToken(t), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	principal, org, predecessor := uuid.New(), uuid.New(), uuid.New()
	for _, req := range []Request{
		{PrincipalID: principal, OrganizationID: &org, AMR: []string{"pwd"}, PredecessorSessionID: &predecessor},
		{PrincipalID: principal, OrganizationID: &org, AMR: []string{"pwd"}},
	} {
		if _, err := client.Issue(context.Background(), endUserRequest(), req); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		`{"principal_id":"` + principal.String() + `","organization_id":"` + org.String() + `","amr":["pwd"],"predecessor_session_id":"` + predecessor.String() + `"}`,
		`{"principal_id":"` + principal.String() + `","organization_id":"` + org.String() + `","amr":["pwd"]}`,
	}
	for i := range want {
		if bodies[i] != want[i] {
			t.Errorf("body %d = %s, want %s", i, bodies[i], want[i])
		}
	}
}

// Only a 401 session_revoked to a superseding issue is ErrSessionEnded (the
// caller's own session ended first); every other refusal stays ErrUnavailable.
func TestIssueMapsAnEndedPredecessorToErrSessionEnded(t *testing.T) {
	predecessor := uuid.New()
	refusal := func(status int, body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, body)
		}
	}
	revoked := `{"schema_version":"error.v1","status":401,"reason_code":"session_revoked"}`
	cases := []struct {
		name        string
		answer      http.HandlerFunc
		predecessor *uuid.UUID
		want        error
	}{
		{"ended predecessor", refusal(http.StatusUnauthorized, revoked), &predecessor, ErrSessionEnded},
		{"session_revoked without a predecessor", refusal(http.StatusUnauthorized, revoked), nil, ErrUnavailable},
		{"other 401 reason", refusal(http.StatusUnauthorized, `{"reason_code":"invalid_credentials"}`), &predecessor, ErrUnavailable},
		{"401 without a body", refusal(http.StatusUnauthorized, ``), &predecessor, ErrUnavailable},
		{"403 other principal", refusal(http.StatusForbidden, `{"reason_code":"not_entitled"}`), &predecessor, ErrUnavailable},
		{"503", refusal(http.StatusServiceUnavailable, `{"reason_code":"store_unavailable"}`), &predecessor, ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.answer)
			defer server.Close()
			client, err := New(baseOf(t, server.URL), testToken(t), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Issue(context.Background(), endUserRequest(),
				Request{PrincipalID: uuid.New(), AMR: []string{"pwd"}, PredecessorSessionID: tc.predecessor})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
			if tc.want == ErrSessionEnded && errors.Is(err, ErrUnavailable) {
				t.Fatal("an ended session must not read as unavailable (503)")
			}
		})
	}
	if strings.Contains(ErrSessionEnded.Error(), "unavailable") {
		t.Fatal("ErrSessionEnded must not say unavailable")
	}
}
