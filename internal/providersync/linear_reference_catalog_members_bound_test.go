package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// linearCatalogMembersServer serves one team whose members connection spans
// totalPages pages of one member each; page 0 is embedded in the teams query.
type linearCatalogMembersServer struct {
	mu         sync.Mutex
	totalPages int
	followUps  int
	afters     []string
}

func (server *linearCatalogMembersServer) page(index int) string {
	return fmt.Sprintf(`{"nodes":[{"id":"user-%d","name":"Member %d","email":"m%d@example.com","active":true}],"pageInfo":{"hasNextPage":%t,"endCursor":"m%d"}}`,
		index, index, index, index < server.totalPages-1, index)
}

func (server *linearCatalogMembersServer) Do(request *http.Request) (*http.Response, error) {
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
	case strings.Contains(body.Query, "query LinearReferenceCatalogMembers("):
		server.followUps++
		after, _ := body.Variables["after"].(string)
		server.afters = append(server.afters, after)
		index := 0
		if after != "" {
			index, _ = strconv.Atoi(strings.TrimPrefix(after, "m"))
			index++
		}
		payload = `{"data":{"team":{"members":` + server.page(index) + `}}}`
	case strings.Contains(body.Query, "query LinearReferenceCatalogTeams("):
		payload = `{"data":{"teams":{"nodes":[{"id":"team-raw-1","key":"ENG","name":"Engineering","members":` + server.page(0) +
			`}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	case strings.Contains(body.Query, "query LinearWorkItemsCycles("):
		payload = `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	case strings.Contains(body.Query, "query LinearReferenceCatalogProjects("):
		payload = `{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	default:
		return nil, fmt.Errorf("unexpected query %q", body.Query)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(payload)), Request: request,
	}, nil
}

func collectLinearCatalogMembers(t *testing.T, totalPages int, maxPages int) (LinearReferenceCatalogBatch, *linearCatalogMembersServer, error) {
	t.Helper()
	server := &linearCatalogMembersServer{totalPages: totalPages}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "workspace"
	batch, err := (LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: maxPages}).CollectReferenceCatalog(
		context.Background(), teamCatalogRefFromClaim(claim),
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(server)),
		TeamCatalogSelections{Teams: true, Members: true, Projects: true},
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	return batch, server, err
}

// CHAOS-8781: the member follow-up continues AFTER the page embedded in the teams
// query (a restart would read page 1 twice) and the connection is held to the same
// 50-page hard bound as every nested Linear connection, embedded page included,
// even though the catalog's own page budget (here the default 100) is larger.
// Literal numbers.
func TestLinearReferenceCatalogTeamMembersContinueFromTheEmbeddedPageUnderTheHardBound(t *testing.T) {
	batch, server, err := collectLinearCatalogMembers(t, 50, 100)
	if err != nil || batch.Failure != nil || len(batch.Rows.Members) != 50 || server.followUps != 49 {
		t.Fatalf("50 pages: err=%v members=%d followUps=%d", err, len(batch.Rows.Members), server.followUps)
	}
	if server.afters[0] != "m0" {
		t.Fatalf("first follow-up after=%q want the embedded endCursor m0", server.afters[0])
	}
	// teams 1 + cycles 1 + projects 1 + members 50 (49 follow-ups + the embedded page).
	if batch.Evidence.Pages != 53 {
		t.Fatalf("evidence pages=%d want 53 (the embedded members page counts)", batch.Evidence.Pages)
	}

	batch, server, err = collectLinearCatalogMembers(t, 51, 100)
	if !errors.Is(err, ErrPaginationCapExceeded) || batch.Failure == nil || batch.Failure.Code != LinearReferenceCatalogPaginationCap {
		t.Fatalf("51 pages: err=%v failure=%+v", err, batch.Failure)
	}
	for _, want := range []string{"members of team team-raw-1", "after 50 pages", "max 50 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	if server.followUps != 49 {
		t.Fatalf("follow-ups=%d want 49 (no request past the bound)", server.followUps)
	}
}

// A lower configured page budget still lowers the bound.
func TestLinearReferenceCatalogTeamMembersHonourALowerConfiguredBudget(t *testing.T) {
	_, _, err := collectLinearCatalogMembers(t, 5, 3)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("err=%v want ErrPaginationCapExceeded with MaxPages 3 and 5 pages", err)
	}
}

// CHAOS-8781: the project-team evidence counts the embedded page: 50 total pages
// are 1 teams + 1 cycles + 1 projects + 49 follow-ups + the embedded page = 53;
// and the cap log carries the item count of ALL pages read (embedded included).
func TestLinearReferenceCatalogProjectTeamsEvidenceCountsTheEmbeddedPage(t *testing.T) {
	batch, _, err := collectLinearProjectTeams(t, 50, true)
	if err != nil || batch.Evidence.Pages != 53 {
		t.Fatalf("50 pages: err=%v evidence pages=%d want 53", err, batch.Evidence.Pages)
	}
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	batch, _, err = collectLinearProjectTeams(t, 51, true)
	if !errors.Is(err, ErrPaginationCapExceeded) || batch.Failure == nil || batch.Failure.Pages != 53 {
		t.Fatalf("51 pages: err=%v failure=%+v want failure pages 53", err, batch.Failure)
	}
	for _, want := range []string{"level=ERROR", "field=teams", "pages=50", "items=50", `owner="project project-big"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("cap log lacks %q: %s", want, logs.String())
		}
	}
}
