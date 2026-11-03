package sessionissue

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func stateClient(t *testing.T, handler http.HandlerFunc, timeout time.Duration) (*Client, string) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	token := testToken(t)
	client, err := New(baseOf(t, server.URL), token, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return client, token
}

func errorDocument(status int, reason string) string {
	return `{"schema_version":"error.v1","status":` + strconv.Itoa(status) + `,"reason_code":"` + reason +
		`","request_id":"req-1","occurred_at":"2026-10-31T12:00:00Z"}`
}

func TestStateSendsTheSessionAndReadsTheState(t *testing.T) {
	sessionID, principal, org := uuid.New(), uuid.New(), uuid.New()
	var gotMethod, gotPath, gotAuth, gotBody, gotType string
	client, _ := stateClient(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotMethod, gotPath, gotAuth, gotBody, gotType = r.Method, r.URL.Path, r.Header.Get("Authorization"), string(raw), r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"principal_id":"`+principal.String()+`","organization_id":"`+org.String()+`","revoked":true,"expired":false,"legacy_token_version":3}`)
	}, time.Second)
	state, err := client.State(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != PathState || gotType != "application/json" || gotBody != `{"session_id":"`+sessionID.String()+`"}` {
		t.Fatalf("request %s %s %q body %s", gotMethod, gotPath, gotType, gotBody)
	}
	if gotAuth != "Bearer "+client.token {
		t.Fatal("the request does not present the session issuer credential")
	}
	if state.PrincipalID != principal || state.OrganizationID == nil || *state.OrganizationID != org || !state.Revoked || state.Expired ||
		state.LegacyTokenVersion == nil || *state.LegacyTokenVersion != 3 {
		t.Fatalf("state %+v", state)
	}
	client, _ = stateClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"principal_id":"`+principal.String()+`","organization_id":null,"revoked":false,"expired":true,"legacy_token_version":null}`)
	}, time.Second)
	state, err = client.State(context.Background(), sessionID)
	if err != nil || state.OrganizationID != nil || state.LegacyTokenVersion != nil || state.Revoked || !state.Expired {
		t.Fatalf("state %+v %v, want no organization, no pin, expired", state, err)
	}
}

func TestStateRefusesAnAnswerThatIsNotExactlyTheStateDocument(t *testing.T) {
	principal, org := uuid.New().String(), uuid.New().String()
	valid := func(replace ...string) string {
		doc := `{"principal_id":"` + principal + `","organization_id":"` + org + `","revoked":false,"expired":false,"legacy_token_version":1}`
		for i := 0; i+1 < len(replace); i += 2 {
			doc = strings.Replace(doc, replace[i], replace[i+1], 1)
		}
		return doc
	}
	for name, answer := range map[string]string{
		"not json":              "session",
		"an array":              `[]`,
		"a member missing":      `{"principal_id":"` + principal + `","organization_id":null,"revoked":false,"expired":false}`,
		"an unknown member":     strings.Replace(valid(), `"revoked"`, `"extra":1,"revoked"`, 1),
		"a repeated member":     strings.Replace(valid(), `"revoked":false`, `"revoked":false,"revoked":true`, 1),
		"upper case principal":  valid(principal, strings.ToUpper(principal)),
		"principal not a uuid":  valid(principal, "prn_x"),
		"principal null":        valid(`"`+principal+`"`, `null`),
		"organization a number": valid(`"`+org+`"`, `7`),
		"revoked a string":      valid(`"revoked":false`, `"revoked":"false"`),
		"expired null":          valid(`"expired":false`, `"expired":null`),
		"pin fractional":        valid(`"legacy_token_version":1`, `"legacy_token_version":1.5`),
		"pin quoted":            valid(`"legacy_token_version":1`, `"legacy_token_version":"1"`),
		"pin out of range":      valid(`"legacy_token_version":1`, `"legacy_token_version":99999999999999999999`),
		"trailing data":         valid() + `{}`,
		"empty":                 ``,
		"over the size bound":   valid(`"revoked":false`, `"revoked":false,"pad":"`+strings.Repeat("a", maxAnswerBytes)+`"`),
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := stateClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, answer) }, time.Second)
			if _, err := client.State(context.Background(), uuid.New()); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want ErrUnavailable", err)
			}
		})
	}
}

// Only a 404 that names session_not_found is "no such session": every other
// answer, a 404 without that reason (an auth-service with no state route)
// included, is unavailable, never a session that does not exist.
func TestStateReadsOnlyANamedNotFoundAsNoSuchSession(t *testing.T) {
	for name, tc := range map[string]struct {
		status   int
		body     string
		notFound bool
	}{
		"404 session_not_found":   {http.StatusNotFound, errorDocument(404, "session_not_found"), true},
		"404 without a body":      {http.StatusNotFound, "", false},
		"404 of a mux":            {http.StatusNotFound, "404 page not found\n", false},
		"404 another reason":      {http.StatusNotFound, errorDocument(404, "organization_not_found"), false},
		"404 status mismatch":     {http.StatusNotFound, errorDocument(400, "session_not_found"), false},
		"401 invalid_credentials": {http.StatusUnauthorized, errorDocument(401, "invalid_credentials"), false},
		"400 malformed":           {http.StatusBadRequest, errorDocument(400, "malformed"), false},
		"500":                     {http.StatusInternalServerError, "<html>", false},
		"503 store_unavailable":   {http.StatusServiceUnavailable, errorDocument(503, "store_unavailable"), false},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := stateClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}, time.Second)
			_, err := client.State(context.Background(), uuid.New())
			if tc.notFound {
				if !errors.Is(err, ErrSessionNotFound) || errors.Is(err, ErrUnavailable) {
					t.Fatalf("err = %v, want ErrSessionNotFound and not ErrUnavailable", err)
				}
				return
			}
			if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrSessionNotFound) {
				t.Fatalf("err = %v, want ErrUnavailable and not ErrSessionNotFound", err)
			}
		})
	}
}

func TestStateFailsClosedOnATimeoutAndARedirect(t *testing.T) {
	slow, _ := stateClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}, 100*time.Millisecond)
	if _, err := slow.State(context.Background(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a slow route: %v, want ErrUnavailable", err)
	}
	redirecting, _ := stateClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://198.51.100.1/elsewhere", http.StatusTemporaryRedirect)
	}, time.Second)
	if _, err := redirecting.State(context.Background(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a redirect: %v, want ErrUnavailable (never followed)", err)
	}
}

// An error document that names a member twice is not the error.v1 document:
// the last value must never decide. A 404 whose reason_code is repeated is
// unavailable, not "no such session", whichever value comes last.
func TestStateRefusesAnErrorDocumentWithARepeatedMember(t *testing.T) {
	for name, body := range map[string]string{
		"reason last":  `{"schema_version":"error.v1","status":404,"reason_code":"organization_not_found","reason_code":"session_not_found"}`,
		"reason first": `{"schema_version":"error.v1","status":404,"reason_code":"session_not_found","reason_code":"organization_not_found"}`,
		"status twice": `{"schema_version":"error.v1","status":500,"status":404,"reason_code":"session_not_found"}`,
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := stateClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, body)
			}, time.Second)
			_, err := client.State(context.Background(), uuid.New())
			if errors.Is(err, ErrSessionNotFound) || !errors.Is(err, ErrUnavailable) {
				t.Fatalf("err = %v, want ErrUnavailable and not ErrSessionNotFound", err)
			}
		})
	}
}
