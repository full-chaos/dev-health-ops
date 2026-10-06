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

// linearNestedServer is a fake Linear GraphQL endpoint. One issue (and, for
// cycles, one team) owns a single nested connection that spans `totalPages`
// pages; every page holds one unique node and is reached through the
// connection's own cursor, exactly as the live API pages it.
type linearNestedServer struct {
	mu         sync.Mutex
	field      string // attachments|history|comments|relations|inverseRelations|cycles
	totalPages int
	nested     int // requests served for the nested connection
}

var linearNestedQueryMarkers = map[string]string{
	"attachments":      "query LinearWorkItemsAttachments(",
	"history":          "query LinearWorkItemsHistory(",
	"comments":         "query LinearWorkItemsComments(",
	"relations":        "query LinearWorkItemsRelations(",
	"inverseRelations": "query LinearWorkItemsInverseRelations(",
	"cycles":           "query LinearWorkItemsCycles(",
}

func linearNestedNode(field string, index int) string {
	switch field {
	case "attachments":
		return fmt.Sprintf(`{"url":"https://github.com/acme/repo/pull/%d","sourceType":"github"}`, index+1)
	case "history":
		return fmt.Sprintf(`{"createdAt":"2026-07-26T10:%02d:00Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"},"actor":null}`, index%60)
	case "comments":
		return fmt.Sprintf(`{"body":"comment %d","createdAt":"2026-07-27T12:00:00Z","user":null}`, index)
	case "relations":
		return fmt.Sprintf(`{"type":"related","issue":{"identifier":"ENG-45"},"relatedIssue":{"identifier":"ENG-%d"}}`, 1000+index)
	case "inverseRelations":
		return fmt.Sprintf(`{"type":"related","issue":{"identifier":"ENG-%d"},"relatedIssue":{"identifier":"ENG-45"}}`, 1000+index)
	default: // cycles
		return fmt.Sprintf(`{"id":"cycle-%d","number":%d,"name":"","startsAt":"2026-07-25T09:00:00Z","endsAt":"2026-08-01T09:00:00Z","completedAt":null,"progress":0,"team":{"id":"team-eng","key":"ENG","name":"Engineering"}}`, index, index+1)
	}
}

func (server *linearNestedServer) page(field string, index int) string {
	more := index < server.totalPages-1
	pageInfo := fmt.Sprintf(`{"hasNextPage":%t,"endCursor":%q}`, more, "c"+strconv.Itoa(index))
	connection := fmt.Sprintf(`{"nodes":[%s],"pageInfo":%s}`, linearNestedNode(field, index), pageInfo)
	if field == "cycles" {
		return `{"data":{"cycles":` + connection + `}}`
	}
	return `{"data":{"issue":{"` + field + `":` + connection + `}}}`
}

func (server *linearNestedServer) issue() string {
	empty := `{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}`
	conns := map[string]string{
		"attachments": empty, "history": `{"nodes":[]}`, "comments": empty,
		"relations": empty, "inverseRelations": empty,
	}
	if server.field != "cycles" {
		conns[server.field] = server.firstEmbedded()
	}
	return `{"data":{"issues":{"nodes":[{"id":"lin-issue-big","identifier":"ENG-45","title":"Big",
		"createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z",
		"state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},
		"history":` + conns["history"] + `,"comments":` + conns["comments"] + `,
		"attachments":` + conns["attachments"] + `,"relations":` + conns["relations"] + `,
		"inverseRelations":` + conns["inverseRelations"] + `}],
		"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

func (server *linearNestedServer) firstEmbedded() string {
	more := server.totalPages > 1
	return fmt.Sprintf(`{"nodes":[%s],"pageInfo":{"hasNextPage":%t,"endCursor":"c0"}}`,
		linearNestedNode(server.field, 0), more)
}

func (server *linearNestedServer) Do(request *http.Request) (*http.Response, error) {
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
	case strings.Contains(body.Query, "query LinearWorkItemsTeam("):
		payload = linearTeamResponse()
	case strings.Contains(body.Query, "query LinearWorkItems("):
		payload = server.issue()
	default:
		marker := linearNestedQueryMarkers[server.field]
		if !strings.Contains(body.Query, marker) {
			return nil, fmt.Errorf("unexpected query %q", body.Query)
		}
		server.nested++
		index := 0
		if after, ok := body.Variables["after"].(string); ok && after != "" {
			n, convErr := strconv.Atoi(strings.TrimPrefix(after, "c"))
			if convErr != nil {
				return nil, convErr
			}
			index = n + 1
		}
		payload = server.page(server.field, index)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(payload)), Request: request,
	}, nil
}

func collectWithLinearNestedServer(t *testing.T, field string, totalPages int) (CompleteRouteBatch, *linearNestedServer, error) {
	t.Helper()
	server := &linearNestedServer{field: field, totalPages: totalPages}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	handler := LinearWorkItemsRouteHandler{FetchCycles: boolPointer(field == "cycles")}
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(server)),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	return batch, server, err
}

// Every nested connection is paged to its own end, never cut at the old
// 500-row (5 page) / 2-comment-page bound.
func TestLinearWorkItemsNestedConnectionsPageToTheEnd(t *testing.T) {
	t.Parallel()
	const totalPages = 12 // more than the old 5-page and 2-page caps
	for _, field := range []string{"attachments", "history", "comments", "relations", "inverseRelations", "cycles"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			batch, server, err := collectWithLinearNestedServer(t, field, totalPages)
			if err != nil {
				t.Fatalf("%s: %v", field, err)
			}
			want := totalPages - 1 // page 0 is embedded in the issue
			if field == "cycles" {
				want = totalPages
			}
			if server.nested != want {
				t.Fatalf("%s nested requests=%d want %d", field, server.nested, want)
			}
			if field == "comments" {
				for _, effect := range batch.Effects {
					if effect.Destination == "work_item_interactions" && len(effect.Rows) != totalPages {
						t.Fatalf("comment rows=%d want %d", len(effect.Rows), totalPages)
					}
				}
			}
		})
	}
}

// Past the hard bound the unit fails loudly: the error names the owner, the
// field and the counts, is logged at ERROR, and still maps to the permanent
// pagination_incomplete class. Not parallel: it swaps the default logger.
func TestLinearWorkItemsNestedHardBoundNamesIssueAndField(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	for _, field := range []string{"attachments", "history", "comments", "relations", "inverseRelations", "cycles"} {
		logs.Reset()
		_, server, err := collectWithLinearNestedServer(t, field, linearNestedHardMaxPages+5)
		if !errors.Is(err, ErrPaginationCapExceeded) {
			t.Fatalf("%s: err=%v want ErrPaginationCapExceeded", field, err)
		}
		owner := "issue lin-issue-big"
		if field == "cycles" {
			owner = "team team-eng"
		}
		for _, want := range []string{field, owner, strconv.Itoa(linearNestedHardMaxPages) + " pages"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%s: error %q lacks %q", field, err, want)
			}
		}
		if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "field="+field) {
			t.Fatalf("%s: no ERROR log naming the field: %s", field, logs.String())
		}
		// The bound counts nested requests (the issue embeds page 0).
		want := linearNestedHardMaxPages
		if server.nested != want {
			t.Fatalf("%s nested requests=%d want %d", field, server.nested, want)
		}
	}
}
