package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// WANT: a classified rate limit on a group /members page in the post-sync (Strict=false) GitLab team catalog ends the
// walk with that error and no catalog rows, so the caller can snooze and read the catalog again: a rate limit says
// nothing about the group's roster. A 500 keeps the skip of that group's memberships; a 200 completes.
func TestGitLabTeamCatalogRateLimitedMembersEndsTheWalk(t *testing.T) {
	for _, cell := range []struct {
		name          string
		status        int
		header        http.Header
		wantLimited   bool
		wantSkipped   int
		wantMembers   int
		wantRowsEmpty bool
	}{
		{"members 429 Retry-After 120", http.StatusTooManyRequests, http.Header{"Retry-After": {"120"}}, true, 0, 0, true},
		{"members 500 keeps the skip", http.StatusInternalServerError, nil, false, 1, 0, false},
		{"members 200 completes", http.StatusOK, nil, false, 0, 1, false},
	} {
		t.Run(cell.name, func(t *testing.T) {
			doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
				header := http.Header{"Content-Type": {"application/json"}}
				switch request.URL.Path {
				case "/api/v4/groups/zz9":
					return githubTestsHTTPResponse(request, header, `{"id":1,"full_path":"zz9","name":"Zz9"}`), nil
				case "/api/v4/groups/zz9/subgroups", "/api/v4/groups/zz9/projects":
					return githubTestsHTTPResponse(request, header, `[]`), nil
				case "/api/v4/groups/zz9/members":
					if cell.status == http.StatusOK {
						return githubTestsHTTPResponse(request, header, `[{"username":"member-a","name":"member-a"}]`), nil
					}
					for key, values := range cell.header {
						header[key] = values
					}
					return &http.Response{StatusCode: cell.status, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"synthetic"}`)), Request: request}, nil
				default:
					t.Fatalf("unexpected request path %q", request.URL.Path)
					return nil, nil
				}
			})
			client, err := providerfoundation.NewHTTPClient(
				"gitlab", "https://gitlab.example.test", fakehttp.Client(doer),
				func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			)
			if err != nil {
				t.Fatal(err)
			}
			ref := TeamCatalogReference{OrgID: "org-zz9", SyncRunID: "run-zz9", Strict: false}
			credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "zz9"}}
			batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(), ref, credential, client,
				TeamCatalogSelections{Teams: true, Members: true}, time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
			limited := isClassifiedRateLimit(err)
			rowsEmpty := len(batch.Rows.Teams) == 0 && len(batch.Rows.Memberships) == 0 && len(batch.Rows.Ownership) == 0
			t.Logf("CELL gitlab members %s: err=%v rate_limited=%v skipped=%d memberships=%d rows_empty=%v",
				cell.name, err, limited, batch.Evidence.SkippedTeamMemberships, len(batch.Rows.Memberships), rowsEmpty)
			if limited != cell.wantLimited || (err != nil) != cell.wantLimited {
				t.Errorf("err=%v rate_limited=%v, want rate_limited=%v", err, limited, cell.wantLimited)
			}
			if batch.Evidence.SkippedTeamMemberships != cell.wantSkipped || len(batch.Rows.Memberships) != cell.wantMembers {
				t.Errorf("skipped=%d memberships=%d, want skipped=%d memberships=%d",
					batch.Evidence.SkippedTeamMemberships, len(batch.Rows.Memberships), cell.wantSkipped, cell.wantMembers)
			}
			if cell.wantRowsEmpty && !rowsEmpty {
				t.Errorf("rows %+v, want none: a rate-limited walk writes no catalog row", batch.Rows)
			}
		})
	}
}
