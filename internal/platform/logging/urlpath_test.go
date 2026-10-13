package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// CHAOS-8034: the closed set of forms RedactURLPath hides in the path of a URL, and the paths it must leave byte for byte.

const urlPathMarker = "plantedurlpathmarker8034"

func TestRedactURLPathHidesTheClosedSetOfForms(t *testing.T) {
	for name, row := range map[string]struct{ path, want string }{
		"protected name hides the next segment":    {"/token/" + urlPathMarker + "/api/v3/orgs", "/token/[REDACTED]/api/v3/orgs"},
		"api key name":                             {"/api_key/" + urlPathMarker, "/api_key/[REDACTED]"},
		"private token name":                       {"/gitlab/private_token/" + urlPathMarker + "/api/v4", "/gitlab/private_token/[REDACTED]/api/v4"},
		"segment holding a protected word":         {"/token-" + urlPathMarker + "/api/v3", "/[REDACTED]/api/v3"},
		"segment ending in a protected word":       {"/" + urlPathMarker + "-secret/api/v3", "/[REDACTED]/api/v3"},
		"path parameter":                           {"/api/v3;access_token=" + urlPathMarker + "/orgs", "/api/v3;access_token=[REDACTED]/orgs"},
		"path parameter, encoded name":             {"/api/v3;access%5Ftoken=" + urlPathMarker, "/api/v3;access%5Ftoken=[REDACTED]"},
		"encoded separator after a protected name": {"/token%2F" + urlPathMarker + "/api/v3", "/[REDACTED]/api/v3"},
		"encoded protected name":                   {"/%74oken/" + urlPathMarker + "/api/v3", "/%74oken/[REDACTED]/api/v3"},
		"protected name at the end hides nothing":  {"/api/v3/token", "/api/v3/token"},
		"encoded path parameter delimiter":         {"/api/v3%3Baccess_token=" + urlPathMarker + "/orgs", "/api/[REDACTED]/orgs"},
		"two credentials":                          {"/token/" + urlPathMarker + "/x/password/" + urlPathMarker, "/token/[REDACTED]/x/password/[REDACTED]"},
	} {
		t.Run(name, func(t *testing.T) {
			got := redactURLPath(row.path)
			if got != row.want {
				t.Fatalf("redactURLPath(%q) = %q, want %q", row.path, got, row.want)
			}
			if row.want != row.path && strings.Contains(got, urlPathMarker) {
				t.Fatalf("the marker survived: %q", got)
			}
		})
	}
}

func TestRedactURLPathKeepsEveryOtherPathByteForByte(t *testing.T) {
	for _, same := range []string{
		"", "/", "/api/v3", "/api/v4/projects", "/gitlab/api/v4/groups/acme/projects", "/orgs/acme/repos",
		"/api/v3/orgs/a%20b/repos", "/groups/gitlab-examples%2Fno-such-group/projects", "/api/v3;v=1", "/installation/repositories",
		"/search/repositories", "not a path",
	} {
		if got := redactURLPath(same); got != same {
			t.Fatalf("redactURLPath(%q) = %q, want it unchanged", same, got)
		}
	}
}

func TestRedactURLPathWritesOneDebugLineWithCountsNeverTheText(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	path := "/token/" + urlPathMarker + "/api/v3"
	got := RedactURLPath(context.Background(), logger, path)
	if strings.Contains(got, urlPathMarker) {
		t.Fatalf("the marker survived: %q", got)
	}
	line := out.String()
	if strings.Count(line, "\n") != 1 || !strings.Contains(line, "url path credential redacted") || !strings.Contains(line, "raw_bytes=") {
		t.Fatalf("want one debug line with the byte counts, got %q", line)
	}
	if strings.Contains(line, urlPathMarker) || strings.Contains(line, "/api/v3") {
		t.Fatalf("the debug line carries the text: %q", line)
	}
	out.Reset()
	RedactURLPath(context.Background(), logger, "/api/v3")
	if out.Len() != 0 {
		t.Fatalf("a plain path writes no line, got %q", out.String())
	}
}
