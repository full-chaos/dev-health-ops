package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A provider HTTP-status error must not print a path that carries a member
// login (CHAOS-9761); it prints the caller's route template instead.
func TestStatusErrorCarriesRouteTemplateNotRawPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	for name, tc := range map[string]struct {
		path, route string
	}{
		"user":       {"/users/octocat-login", "/users/{login}"},
		"membership": {"/orgs/acme/teams/platform/memberships/octocat-login", "/orgs/{org}/teams/{slug}/memberships/{login}"},
	} {
		t.Run(name, func(t *testing.T) {
			client := transportClient(t, &http.Client{}, server.URL)
			_, err := client.DoRoute(context.Background(), http.MethodGet, tc.route, tc.path+"?token=secret-query", nil)
			var providerErr *ProviderError
			if !errors.As(err, &providerErr) || providerErr.StatusCode != http.StatusInternalServerError {
				t.Fatalf("err = %v, want a 500 *ProviderError", err)
			}
			if strings.Contains(err.Error(), "octocat-login") {
				t.Fatalf("login reached the error: %q", err.Error())
			}
			if !strings.Contains(err.Error(), "route="+tc.route) {
				t.Fatalf("Error() = %q lacks route=%s", err.Error(), tc.route)
			}
		})
	}
}

func TestStatusErrorWithoutRouteHasNoPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client := transportClient(t, &http.Client{}, server.URL)
	_, err := client.Do(context.Background(), http.MethodGet, "/users/octocat-login", nil)
	if err == nil || strings.Contains(err.Error(), "octocat-login") || strings.Contains(err.Error(), "route=") {
		t.Fatalf("Error() = %v, want no path and no route", err)
	}
}

func TestProviderErrorMessagePrintsEachPartOnce(t *testing.T) {
	message := (&ProviderError{Class: ErrorTransient, StatusCode: 500, Route: "/users/{login}", Reason: TransportReasonOther}).Error()
	for _, part := range []string{"status=500", "route=/users/{login}", "reason=other"} {
		if strings.Count(message, part) != 1 {
			t.Fatalf("Error() = %q, want %q exactly once", message, part)
		}
	}
}

func TestRouteTemplateDoesNotOutliveItsCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := transportClient(t, &http.Client{}, server.URL)
	ctx := context.Background()
	_, _ = client.DoRoute(ctx, http.MethodGet, "/users/{login}", "/users/octocat-login", nil)
	_, err := client.Do(ctx, http.MethodGet, "/repos/acme/api", nil)
	if err == nil || strings.Contains(err.Error(), "route=") {
		t.Fatalf("Error() = %v, want no route on a plain Do", err)
	}
}

func TestObjectTooLargeErrorCarriesNoPath(t *testing.T) {
	for _, length := range []int64{123, -1} {
		message := (&ObjectTooLargeError{Path: "/api/v4/groups/secret-group", CapBytes: 10, ContentLength: length}).Error()
		if strings.Contains(message, "secret-group") || strings.Contains(message, "/api/") {
			t.Fatalf("Error() = %q carries the request path", message)
		}
	}
}
