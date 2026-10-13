package restcore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// CHAOS-8034: an operator can configure a provider base URL that holds a credential in its PATH. The base URL is part of
// every request URL, and a 404 puts the request URL in the error text (and the callers log that text). The text must not
// carry the credential. The forms below are the closed set: a protected name as a path segment with the value in the next
// segment, a segment that holds a protected word with the value after it, a path parameter (`;name=value`) on a segment,
// a percent-encoded separator, and the same forms in front of the GitLab API root. A path without them stays byte-identical
// (the frozen venue oracles of the sync admin routes pin that text).

const plantedBasePathMarker = "plantedbasepathmarker8034"

func notFoundText(t *testing.T, basePath, apiPath string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	core := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }}
	_, err := core.Get(context.Background(), server.URL+basePath+apiPath, "GET "+apiPath)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "NotFoundException" {
		t.Fatalf("err = %v, want a NotFoundException", err)
	}
	return apiErr.Message
}

func TestANotFoundErrorCarriesNoCredentialFromTheBaseURLPath(t *testing.T) {
	for _, row := range []struct {
		name, basePath, apiPath string
	}{
		{"protected name then value, GitHub root", "/token/" + plantedBasePathMarker + "/api/v3", "/orgs/acme/repos?per_page=100"},
		{"protected name then value, GitLab root", "/private_token/" + plantedBasePathMarker + "/api/v4", "/projects?page=1"},
		{"api key name then value", "/api_key/" + plantedBasePathMarker, "/api/v3/orgs/acme/repos"},
		{"segment with a protected word", "/token-" + plantedBasePathMarker + "/api/v3", "/orgs/acme/repos"},
		{"segment with a protected word, secret first", "/" + plantedBasePathMarker + "-secret/api/v3", "/orgs/acme/repos"},
		{"path parameter on a segment", "/api/v3;access_token=" + plantedBasePathMarker, "/orgs/acme/repos"},
		{"path parameter on the first segment", "/gitlab;private_token=" + plantedBasePathMarker + "/api/v4", "/projects"},
		{"percent-encoded separator", "/token%2F" + plantedBasePathMarker + "/api/v3", "/orgs/acme/repos"},
		{"encoded path parameter delimiter", "/api/v3%3Baccess_token=" + plantedBasePathMarker, "/orgs/acme/repos"},
		{"encoded path parameter delimiter, GitLab root", "/gitlab%3Bprivate_token=" + plantedBasePathMarker + "/api/v4", "/projects"},
		{"percent-encoded name", "/%74oken/" + plantedBasePathMarker + "/api/v3", "/orgs/acme/repos"},
	} {
		t.Run(row.name, func(t *testing.T) {
			message := notFoundText(t, row.basePath, row.apiPath)
			if strings.Contains(message, plantedBasePathMarker) {
				t.Fatalf("the error text carries the credential of the base URL path: %s", message)
			}
			if !strings.HasPrefix(message, "github resource not found on GET "+row.apiPath+": http://") {
				t.Fatalf("the error text lost its shape: %s", message)
			}
		})
	}
}

func TestANotFoundErrorKeepsAPlainBaseURLPathByteForByte(t *testing.T) {
	for _, row := range []struct {
		name, basePath, apiPath string
	}{
		{"GitHub Enterprise root", "/api/v3", "/orgs/acme/repos?per_page=100"},
		{"GitLab root", "/api/v4", "/projects?page=1&per_page=100"},
		{"GitLab under a sub-path", "/gitlab/api/v4", "/groups/acme/projects"},
		{"no base path", "", "/orgs/acme/repos?per_page=100"},
	} {
		t.Run(row.name, func(t *testing.T) {
			message := notFoundText(t, row.basePath, row.apiPath)
			if !strings.HasSuffix(message, row.basePath+row.apiPath) {
				t.Fatalf("the plain URL changed: %s", message)
			}
		})
	}
}
