package gitlabcode

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestListGroupProjectsFollowsAnyPageNumber pins that a page number past
// 32 and 64 bits is requested as its exact decimal text, as Python's
// unbounded int is, and not treated as the end of the listing.
func TestListGroupProjectsFollowsAnyPageNumber(t *testing.T) {
	for _, next := range []string{"2147483648", "9223372036854775808", "123456789012345678901234567890"} {
		transport := &scriptedTransport{responses: []scripted{
			ok(`[{"id": 1, "name": "api", "path_with_namespace": "grp/api"}]`, [2]string{"X-Next-Page", next}),
			ok(`[{"id": 2, "name": "target", "path_with_namespace": "grp/target"}]`),
		}}
		client := Client{BaseURL: "http://gitlab.test", Token: "tok", HTTP: &http.Client{Transport: transport},
			Sleep: func(context.Context, time.Duration) error { return nil }}
		projects, err := client.ListGroupProjects(context.Background(), "grp")
		if err != nil {
			t.Fatalf("X-Next-Page %s: %v", next, err)
		}
		if len(projects) != 2 || projects[1].Name != "target" {
			t.Fatalf("X-Next-Page %s: got %d projects, want the page it names listed", next, len(projects))
		}
		if len(transport.seen) != 2 || !strings.Contains(transport.seen[1][0], "page="+next+"&") {
			t.Fatalf("X-Next-Page %s: requests %v", next, transport.seen)
		}
	}
}
