package providersync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// retiredJiraSearchPattern matches a literal reference to Atlassian's removed
// GET /rest/api/3/search endpoint (410 Gone since CHAOS-4585): "search"
// immediately followed by the closing quote of a bare path literal, or by a
// "?" opening a query string. /rest/api/3/search/jql -- the live replacement
// -- is followed by "/" instead, so it never matches.
var retiredJiraSearchPattern = regexp.MustCompile(`/rest/api/3/search(["?])`)

// TestNoGoJiraCallerTargetsTheRetiredSearchEndpoint is CHAOS-4585's
// registry-level regression guard: every Go Jira HTTP request builder in this
// package must target /rest/api/3/search/jql, never bare /rest/api/3/search.
// collectJiraAtlassianIssues (jira_atlassian_route.go) drifted onto the
// retired path even though the sibling collectJiraWorkItemIssues
// (jira_work_items_route.go) already targeted the replacement -- this scans
// every production jira_*.go file so a future caller cannot reintroduce it.
func TestNoGoJiraCallerTargetsTheRetiredSearchEndpoint(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "jira_") ||
			!strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatal(err)
		}
		checked++
		if match := retiredJiraSearchPattern.FindString(string(raw)); match != "" {
			t.Fatalf(
				"%s references the retired Jira endpoint %q -- migrate to "+
					"/rest/api/3/search/jql (CHAOS-4585)", name, match,
			)
		}
	}
	if checked == 0 {
		t.Fatal("no production jira_*.go files found -- guard did not scan anything")
	}
}
