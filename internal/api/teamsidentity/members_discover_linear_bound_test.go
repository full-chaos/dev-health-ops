package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// linearMembersDoer serves one team (key ENG) whose members connection spans
// `totalPages` pages of one member each. Page 0 rides in the teams query, the
// rest answer the TeamMembers query, reached through the connection's cursor.
type linearMembersDoer struct {
	mu         sync.Mutex
	totalPages int
	followUps  int
	afters     []string
}

func (doer *linearMembersDoer) page(index int) string {
	return fmt.Sprintf(`{"nodes":[{"id":"u%d","name":"Member %d","email":"m%d@example.com","active":true}],"pageInfo":{"hasNextPage":%t,"endCursor":"m%d"}}`,
		index, index, index, index < doer.totalPages-1, index)
}

func (doer *linearMembersDoer) Do(request *http.Request) (*http.Response, error) {
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
	doer.mu.Lock()
	defer doer.mu.Unlock()
	var payload string
	switch {
	case strings.Contains(body.Query, "query TeamMembers("):
		doer.followUps++
		after, _ := body.Variables["after"].(string)
		doer.afters = append(doer.afters, after)
		index := 0
		if after != "" {
			index, _ = strconv.Atoi(strings.TrimPrefix(after, "m"))
			index++
		}
		payload = `{"data":{"team":{"members":` + doer.page(index) + `}}}`
	case strings.Contains(body.Query, "query Teams("):
		payload = `{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering","members":` + doer.page(0) +
			`}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	default:
		return nil, fmt.Errorf("unexpected query %q", body.Query)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(payload)), Request: request,
	}, nil
}

// CHAOS-8781: the member follow-up continues AFTER the inline page (it does not
// restart from the first page) and the connection is held to 50 pages in all,
// the inline page included. Literal numbers.
func TestDiscoverMembersLinearContinuesFromTheInlinePageUnderTheHardBound(t *testing.T) {
	for _, test := range []struct {
		pages   int
		wantErr bool
	}{{12, false}, {50, false}, {51, true}} {
		t.Run(fmt.Sprintf("%d pages", test.pages), func(t *testing.T) {
			doer := &linearMembersDoer{totalPages: test.pages}
			withDiscoveryClient(t, doer)
			members, err := discoverMembersLinear(context.Background(), secrets.NewValue("lin_api_test"), "ENG")
			if test.wantErr {
				if err == nil {
					t.Fatal("51 pages accepted")
				}
				for _, want := range []string{"members of team team-eng", "after 50 pages", "max 50 pages"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("error %q lacks %q", err, want)
					}
				}
				if strings.Contains(err.Error(), "lin_api_test") {
					t.Fatalf("error carries the credential: %q", err)
				}
				if doer.followUps != 49 {
					t.Fatalf("follow-ups=%d want 49 (no request past the bound)", doer.followUps)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(members) != test.pages {
				t.Fatalf("members=%d want %d", len(members), test.pages)
			}
			if doer.followUps != test.pages-1 {
				t.Fatalf("follow-ups=%d want %d", doer.followUps, test.pages-1)
			}
			if doer.afters[0] != "m0" {
				t.Fatalf("first follow-up after=%q want the inline endCursor m0 (the walk must not restart)", doer.afters[0])
			}
		})
	}
}
