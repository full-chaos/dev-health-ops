package providersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"context"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// linearProjectTeamsServer is a fake Linear endpoint whose single project owns
// a teams connection of totalPages pages, one team per page. Page 0 is embedded
// in the projects query exactly as the live API returns it.
type linearProjectTeamsServer struct {
	mu            sync.Mutex
	totalPages    int
	followUps     int
	projectsQuery string
	// emptyEmbeddedCursor makes the teams page embedded in the projects query say
	// "next page" with an EMPTY endCursor.
	emptyEmbeddedCursor bool
}

func (server *linearProjectTeamsServer) teamsPage(index int) string {
	cursor := fmt.Sprintf("c%d", index)
	if index == 0 && server.emptyEmbeddedCursor {
		cursor = ""
	}
	return fmt.Sprintf(`{"nodes":[{"id":"team-id-%d","key":"K%d"}],"pageInfo":{"hasNextPage":%t,"endCursor":%q}}`,
		index, index, index < server.totalPages-1, cursor)
}

func (server *linearProjectTeamsServer) Do(request *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	var body struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	var payload string
	switch {
	case strings.Contains(body.Query, "query LinearReferenceCatalogTeams("):
		payload = `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","members":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	case strings.Contains(body.Query, "query LinearWorkItemsCycles("):
		payload = `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	case strings.Contains(body.Query, "query LinearReferenceCatalogProjects("):
		server.projectsQuery = body.Query
		payload = `{"data":{"projects":{"nodes":[{"id":"project-big","name":"Big","status":{"id":"s","name":"Started","type":"started"},"trashed":false,"url":"https://linear.app/p","teams":` +
			server.teamsPage(0) + `}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	case strings.Contains(body.Query, "query LinearReferenceCatalogProjectTeams("):
		server.followUps++
		if body.Variables["projectId"] != "project-big" {
			return nil, fmt.Errorf("unexpected project %v", body.Variables["projectId"])
		}
		index := 0
		if after, ok := body.Variables["after"].(string); ok && after != "" {
			n, convErr := strconv.Atoi(strings.TrimPrefix(after, "c"))
			if convErr != nil {
				return nil, convErr
			}
			index = n + 1
		}
		payload = `{"data":{"project":{"teams":` + server.teamsPage(index) + `}}}`
	default:
		return nil, fmt.Errorf("unexpected query %q", body.Query)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(payload)), Request: request,
	}, nil
}

func collectLinearProjectTeams(t *testing.T, totalPages int, strict bool) (LinearReferenceCatalogBatch, *linearProjectTeamsServer, error) {
	t.Helper()
	return collectLinearProjectTeamsFrom(t, &linearProjectTeamsServer{totalPages: totalPages}, strict)
}

func collectLinearProjectTeamsFrom(t *testing.T, server *linearProjectTeamsServer, strict bool) (LinearReferenceCatalogBatch, *linearProjectTeamsServer, error) {
	t.Helper()
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	ref := teamCatalogRefFromClaim(claim)
	ref.Strict = strict
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10}).CollectReferenceCatalog(
		context.Background(), ref,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(server)),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true},
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	return batch, server, err
}

func projectOwnershipRows(batch LinearReferenceCatalogBatch) int {
	total := 0
	for _, row := range batch.Rows.Ownership {
		if row.ProjectID.String() == "project-big" {
			total++
		}
	}
	return total
}

// A project with 12 teams keeps all 12 ownership rows: the teams connection is
// paged to its end, not cut at the first page.
func TestLinearReferenceCatalogPagesProjectTeamsToTheEnd(t *testing.T) {
	t.Parallel()
	batch, server, err := collectLinearProjectTeams(t, 12, true)
	if err != nil || batch.Failure != nil || !batch.Evidence.ProjectsComplete {
		t.Fatalf("err=%v batch=%+v", err, batch.Failure)
	}
	if got := projectOwnershipRows(batch); got != 12 {
		t.Fatalf("project ownership rows=%d want 12", got)
	}
	// The embedded teams selection must ask for pageInfo, or the provider's
	// "more pages" signal never reaches the collector.
	compact := strings.Join(strings.Fields(server.projectsQuery), " ")
	if !strings.Contains(compact, "teams(first: 50) { nodes { id key } pageInfo { hasNextPage endCursor } }") {
		t.Fatalf("projects query does not page its embedded teams: %s", compact)
	}
	if server.followUps != 11 {
		t.Fatalf("follow-up requests=%d want 11", server.followUps)
	}
}

// Literal numbers: 50 total pages (the embedded one included) pass, 51 fail.
func TestLinearReferenceCatalogProjectTeamsBoundIsExactlyFiftyPages(t *testing.T) {
	t.Parallel()
	batch, server, err := collectLinearProjectTeams(t, 50, true)
	if err != nil || projectOwnershipRows(batch) != 50 || server.followUps != 49 {
		t.Fatalf("50 pages: err=%v rows=%d followUps=%d", err, projectOwnershipRows(batch), server.followUps)
	}
	batch, server, err = collectLinearProjectTeams(t, 51, true)
	if !errors.Is(err, ErrPaginationCapExceeded) || batch.Failure == nil ||
		batch.Failure.Code != LinearReferenceCatalogPaginationCap {
		t.Fatalf("51 pages: err=%v failure=%+v", err, batch.Failure)
	}
	for _, want := range []string{"teams of project project-big", "after 50 pages", "max 50 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	if server.followUps != 49 {
		t.Fatalf("follow-up requests=%d want 49", server.followUps)
	}
}

// Non-strict (post-sync) keeps the other rows, marks projects incomplete and
// writes no ownership row for the project whose teams it could not finish.
func TestLinearReferenceCatalogNonStrictProjectTeamsBoundKeepsOtherRows(t *testing.T) {
	t.Parallel()
	batch, _, err := collectLinearProjectTeams(t, 51, false)
	if err != nil || batch.Failure != nil {
		t.Fatalf("non-strict must not fail: err=%v failure=%+v", err, batch.Failure)
	}
	if batch.Evidence.ProjectsComplete || projectOwnershipRows(batch) != 0 || len(batch.Rows.Teams) != 1 {
		t.Fatalf("projectsComplete=%v ownership=%d teams=%d", batch.Evidence.ProjectsComplete,
			projectOwnershipRows(batch), len(batch.Rows.Teams))
	}
}

// CHAOS-8781 r1 P1-2 (M7) and P2, for a project's teams: an EMBEDDED page that
// said "next page" with an empty cursor fails closed before any follow-up, the
// failure evidence counts the page that WAS read (teams 1 + cycles 1 + projects 1
// + embedded 1), and a strict failure never reports the projects list complete.
// Non-strict keeps the other rows and also reports projects incomplete.
func TestLinearReferenceCatalogProjectTeamsWithAnEmptyEmbeddedCursorFailClosed(t *testing.T) {
	t.Parallel()
	batch, server, err := collectLinearProjectTeamsFrom(t, &linearProjectTeamsServer{totalPages: 5, emptyEmbeddedCursor: true}, true)
	if !errors.Is(err, providerfoundation.ErrPaginationInvalid) || batch.Failure == nil {
		t.Fatalf("err=%v failure=%+v", err, batch.Failure)
	}
	if server.followUps != 0 {
		t.Fatalf("follow-ups=%d want 0", server.followUps)
	}
	if batch.Failure.Pages != 4 || batch.Evidence.Pages != 4 {
		t.Fatalf("failure pages=%d evidence pages=%d want 4", batch.Failure.Pages, batch.Evidence.Pages)
	}
	if batch.Evidence.ProjectsComplete {
		t.Fatal("a strict project-team failure reports the projects list complete")
	}
	lenient, _, err := collectLinearProjectTeamsFrom(t, &linearProjectTeamsServer{totalPages: 5, emptyEmbeddedCursor: true}, false)
	if err != nil || lenient.Failure != nil || lenient.Evidence.ProjectsComplete || len(lenient.Rows.Teams) != 1 {
		t.Fatalf("non-strict: err=%v failure=%+v projectsComplete=%v teams=%d", err, lenient.Failure, lenient.Evidence.ProjectsComplete, len(lenient.Rows.Teams))
	}
}
