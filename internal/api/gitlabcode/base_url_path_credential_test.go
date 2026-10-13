package gitlabcode

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// CHAOS-8034: a credential an operator places in the PATH of the configured instance URL reaches the request URL, and a 404
// puts that URL in the error text. Python's text carries the URL as it is (the frozen GitLab 404 rows print the request URL
// whole); Go's hides the credential. Each row is a NAMED difference with Go's text pinned literally. The frozen golden of
// this package holds no scenario with such a base, so no recorded row moves.

const basePathMarker = "plantedbasepathmarker8034"

func TestBaseURLPathCredentialNamedDifferences(t *testing.T) {
	for _, row := range []struct{ name, base, wantURL string }{
		{"D8034-token-segment", "https://gitlab.test/token/" + basePathMarker, "https://gitlab.test/token/[REDACTED]/api/v4/projects?page=1&per_page=100"},
		{"D8034-private-token-segment", "https://gitlab.test/private_token/" + basePathMarker, "https://gitlab.test/private_token/[REDACTED]/api/v4/projects?page=1&per_page=100"},
		{"D8034-protected-word-segment", "https://gitlab.test/token-" + basePathMarker, "https://gitlab.test/[REDACTED]/api/v4/projects?page=1&per_page=100"},
		{"D8034-path-parameter", "https://gitlab.test/gitlab;private_token=" + basePathMarker, "https://gitlab.test/gitlab;private_token=[REDACTED]/api/v4/projects?page=1&per_page=100"},
		{"D8034-encoded-separator", "https://gitlab.test/token%2F" + basePathMarker, "https://gitlab.test/[REDACTED]/api/v4/projects?page=1&per_page=100"},
	} {
		t.Run(row.name, func(t *testing.T) {
			transport := &scriptedTransport{responses: []scripted{status(404, `{}`)}}
			client := Client{BaseURL: row.base, Token: "tok", HTTP: &http.Client{Transport: transport},
				Sleep: func(context.Context, time.Duration) error { return nil }}
			_, err := client.ListProjects(context.Background(), ListOptions{})
			typed, ok := err.(*Error)
			if !ok {
				t.Fatalf("err = %v, want an *Error", err)
			}
			if typed.Class != "NotFoundException" || !strings.HasSuffix(typed.Message, ": "+row.wantURL) {
				t.Fatalf("Go's text changed: %s: %s\n want the URL %s", typed.Class, typed.Message, row.wantURL)
			}
			if strings.Contains(typed.Message, basePathMarker) {
				t.Fatalf("the error text carries the credential: %s", typed.Message)
			}
			if len(transport.seen) != 1 || !strings.Contains(transport.seen[0][0], basePathMarker) {
				t.Fatalf("the request must still carry the base path as configured (Python's text prints it): %v", transport.seen)
			}
		})
	}
}
