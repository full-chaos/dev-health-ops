package orgreactivate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const testToken = "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func newClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(base, testToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestPathIsTheOrganizationReactivateRoute(t *testing.T) {
	if Path != "/internal/v1/organizations/reactivate" {
		t.Fatalf("Path = %q", Path)
	}
}

func TestNewRefusesAnythingButAnAuthSessionIssuerCredential(t *testing.T) {
	base, _ := url.Parse("http://auth-service:8095")
	for _, token := range []string{"", "svc_acr_" + strings.Repeat("A", 43), "svc_authsess_short", "Bearer " + testToken, testToken + "x"} {
		_, err := New(base, token, time.Second)
		if err == nil {
			t.Fatalf("New accepted %q", token)
		}
		if token != "" && strings.Contains(err.Error(), token) {
			t.Fatalf("the error carries the token: %v", err)
		}
	}
	if _, err := New(nil, testToken, time.Second); err == nil {
		t.Fatal("New accepted no URL")
	}
}

func TestReactivateSendsTheOrgAndTheActorWithTheServiceCredential(t *testing.T) {
	org, actor := uuid.New(), uuid.New()
	var gotAuth, gotMethod, gotPath, gotType string
	var body map[string]any
	client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotMethod, gotPath, gotType = r.Header.Get("Authorization"), r.Method, r.URL.Path, r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"active","reactivated":true}`))
	})
	if err := client.Reactivate(context.Background(), org, actor); err != nil {
		t.Fatalf("Reactivate = %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != Path || gotAuth != "Bearer "+testToken || gotType != "application/json" {
		t.Fatalf("request = %s %s auth=%v type=%q", gotMethod, gotPath, gotAuth == "Bearer "+testToken, gotType)
	}
	if len(body) != 2 || body["organization_id"] != org.String() || body["actor_principal_id"] != actor.String() {
		t.Fatalf("body = %v, want exactly organization_id and actor_principal_id", body)
	}
}

func TestAnAlreadyActiveOrganizationIsSuccess(t *testing.T) {
	org := uuid.New()
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"active","reactivated":false}`))
	})
	if err := client.Reactivate(context.Background(), org, uuid.New()); err != nil {
		t.Fatalf("a no-op answer = %v, want success", err)
	}
}

// Every non-success is an error, so the PATCH fails closed: a refusal is
// ErrRefused, everything else (5xx, a bad answer, no answer) ErrUnavailable.
func TestEveryFailureIsAnErrorAndNamesItsClass(t *testing.T) {
	org := uuid.New()
	good := `{"organization_id":"` + org.String() + `","status":"active","reactivated":true}`
	for name, tc := range map[string]struct {
		status int
		body   string
		want   error
	}{
		"401":                  {http.StatusUnauthorized, `{}`, ErrRefused},
		"403":                  {http.StatusForbidden, `{}`, ErrRefused},
		"404":                  {http.StatusNotFound, `{}`, ErrRefused},
		"409":                  {http.StatusConflict, `{}`, ErrRefused},
		"400":                  {http.StatusBadRequest, `{}`, ErrRefused},
		"500":                  {http.StatusInternalServerError, `{}`, ErrUnavailable},
		"503":                  {http.StatusServiceUnavailable, `{}`, ErrUnavailable},
		"200, not JSON":        {http.StatusOK, `nope`, ErrUnavailable},
		"200, empty":           {http.StatusOK, ``, ErrUnavailable},
		"200, still suspended": {http.StatusOK, `{"organization_id":"` + org.String() + `","status":"suspended","reactivated":false}`, ErrUnavailable},
		"200, another org":     {http.StatusOK, `{"organization_id":"` + uuid.NewString() + `","status":"active","reactivated":true}`, ErrUnavailable},
		"204":                  {http.StatusNoContent, ``, ErrUnavailable},
		"302":                  {http.StatusFound, good, ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "http://127.0.0.1:1/x")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			err := client.Reactivate(context.Background(), org, uuid.New())
			if !errors.Is(err, tc.want) {
				t.Fatalf("Reactivate = %v, want %v", err, tc.want)
			}
			if tc.want == ErrRefused && errors.Is(err, ErrUnavailable) {
				t.Fatalf("a refusal is not unavailable: %v", err)
			}
		})
	}
}

func TestATimeoutAndAnUnreachableServerAreUnavailable(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	t.Cleanup(slow.Close)
	base, _ := url.Parse(slow.URL)
	client, err := New(base, testToken, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Reactivate(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout = %v, want ErrUnavailable", err)
	}
	closed := httptest.NewServer(http.NotFoundHandler())
	base, _ = url.Parse(closed.URL)
	closed.Close()
	client, _ = New(base, testToken, time.Second)
	if err := client.Reactivate(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unreachable = %v, want ErrUnavailable", err)
	}
}

func TestNoErrorCarriesTheCredentialOrTheAnswerBody(t *testing.T) {
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"detail":"SENTINEL-ANSWER-TEXT"}`))
	})
	err := client.Reactivate(context.Background(), uuid.New(), uuid.New())
	if err == nil || strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "SENTINEL-ANSWER-TEXT") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnAnswerOverTheBoundIsUnavailable(t *testing.T) {
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"organization_id":"` + strings.Repeat("a", 128<<10) + `"}`))
	})
	if err := client.Reactivate(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("an oversized answer = %v, want ErrUnavailable", err)
	}
}

func TestReactivateNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	reached := make(chan struct{}, 4)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached <- struct{}{}
		_, _ = w.Write([]byte(`{"status":"active"}`))
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+Path, http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()
	base, _ := url.Parse(redirecting.URL)
	client, err := New(base, testToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if client.http.CheckRedirect == nil || client.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the client's own CheckRedirect does not answer http.ErrUseLastResponse")
	}
	if err := client.Reactivate(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("redirect = %v, want ErrUnavailable", err)
	}
	if len(reached) != 0 {
		t.Fatalf("the redirect target saw %d request(s)", len(reached))
	}
}

func TestSuspendPathIsTheOrganizationSuspendRoute(t *testing.T) {
	if SuspendPath != "/internal/v1/organizations/suspend" {
		t.Fatalf("SuspendPath = %q", SuspendPath)
	}
	var _ Suspender = (*Client)(nil)
}

func TestSuspendSendsTheOrgAndTheActorWithTheServiceCredential(t *testing.T) {
	org, actor := uuid.New(), uuid.New()
	var gotAuth, gotMethod, gotPath, gotType string
	var body map[string]any
	client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotMethod, gotPath, gotType = r.Header.Get("Authorization"), r.Method, r.URL.Path, r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"suspended","suspended":true}`))
	})
	if err := client.Suspend(context.Background(), org, actor); err != nil {
		t.Fatalf("Suspend = %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != SuspendPath || gotAuth != "Bearer "+testToken || gotType != "application/json" {
		t.Fatalf("request = %s %s auth=%v type=%q", gotMethod, gotPath, gotAuth == "Bearer "+testToken, gotType)
	}
	if len(body) != 2 || body["organization_id"] != org.String() || body["actor_principal_id"] != actor.String() {
		t.Fatalf("body = %v, want exactly organization_id and actor_principal_id", body)
	}
}

func TestAnAlreadySuspendedOrganizationIsSuccess(t *testing.T) {
	org := uuid.New()
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"organization_id":"` + org.String() + `","status":"suspended","suspended":false}`))
	})
	if err := client.Suspend(context.Background(), org, uuid.New()); err != nil {
		t.Fatalf("a no-op answer = %v, want success", err)
	}
}

func TestSuspendFailuresAreErrorsAndNameTheirClass(t *testing.T) {
	org := uuid.New()
	for name, tc := range map[string]struct {
		status int
		body   string
		want   error
	}{
		"401":                {http.StatusUnauthorized, `{}`, ErrRefused},
		"403":                {http.StatusForbidden, `{}`, ErrRefused},
		"404":                {http.StatusNotFound, `{}`, ErrRefused},
		"409":                {http.StatusConflict, `{}`, ErrRefused},
		"500":                {http.StatusInternalServerError, `{}`, ErrUnavailable},
		"503":                {http.StatusServiceUnavailable, `{}`, ErrUnavailable},
		"200, not JSON":      {http.StatusOK, `nope`, ErrUnavailable},
		"200, still active":  {http.StatusOK, `{"organization_id":"` + org.String() + `","status":"active","suspended":false}`, ErrUnavailable},
		"200, another org":   {http.StatusOK, `{"organization_id":"` + uuid.NewString() + `","status":"suspended","suspended":true}`, ErrUnavailable},
		"200, oversize body": {http.StatusOK, `{"organization_id":"` + strings.Repeat("a", 128<<10) + `"}`, ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			err := client.Suspend(context.Background(), org, uuid.New())
			if !errors.Is(err, tc.want) {
				t.Fatalf("Suspend = %v, want %v", err, tc.want)
			}
			if tc.want == ErrRefused && errors.Is(err, ErrUnavailable) {
				t.Fatalf("a refusal is not unavailable: %v", err)
			}
		})
	}
}

func TestSuspendNeverFollowsARedirectToAnotherOrigin(t *testing.T) {
	reached := make(chan struct{}, 4)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached <- struct{}{}
		_, _ = w.Write([]byte(`{"status":"suspended"}`))
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+SuspendPath, http.StatusTemporaryRedirect)
	}))
	defer redirecting.Close()
	base, _ := url.Parse(redirecting.URL)
	client, err := New(base, testToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Suspend(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("redirect = %v, want ErrUnavailable", err)
	}
	if len(reached) != 0 {
		t.Fatalf("the redirect target saw %d request(s)", len(reached))
	}
}

func TestNoSuspendErrorCarriesTheCredentialOrTheAnswerBody(t *testing.T) {
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"detail":"SENTINEL-ANSWER-TEXT"}`))
	})
	err := client.Suspend(context.Background(), uuid.New(), uuid.New())
	if err == nil || strings.Contains(err.Error(), testToken) || strings.Contains(err.Error(), "SENTINEL-ANSWER-TEXT") {
		t.Fatalf("error = %v", err)
	}
}
