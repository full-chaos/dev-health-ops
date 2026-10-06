package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// linearFuncDoer answers each Linear GraphQL request from a function of the
// query name and variables, and records every request's variables.
type linearFuncDoer struct {
	mu       sync.Mutex
	answer   func(query string, variables map[string]any) string
	requests []map[string]any
	queries  []string
}

func (doer *linearFuncDoer) Do(request *http.Request) (*http.Response, error) {
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
	doer.requests = append(doer.requests, body.Variables)
	doer.queries = append(doer.queries, body.Query)
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(doer.answer(body.Query, body.Variables))), Request: request,
	}, nil
}

func (doer *linearFuncDoer) count(marker string) int {
	total := 0
	for _, query := range doer.queries {
		if strings.Contains(query, marker) {
			total++
		}
	}
	return total
}

func teamPageInfo(hasNext bool, cursor string) string {
	return fmt.Sprintf(`{"hasNextPage":%t,"endCursor":%q}`, hasNext, cursor)
}

// rootTeamsAnswer serves the workspace teams list: one team per page, none of
// them the team the caller asks for ("ENG" never appears), `pages` pages in all.
// cursorOf picks the endCursor a page advertises.
func rootTeamsAnswer(pages int, cursorOf func(index int) string) func(string, map[string]any) string {
	return func(query string, variables map[string]any) string {
		index := 0
		if after, ok := variables["after"].(string); ok && after != "" {
			fmt.Sscanf(after, "t%d", &index)
			index++
		}
		return fmt.Sprintf(`{"data":{"teams":{"nodes":[{"id":"id%d","key":"T%d","name":"Team %d","members":{"nodes":[],"pageInfo":%s}}],"pageInfo":%s}}}`,
			index, index, index, teamPageInfo(false, ""), teamPageInfo(index < pages-1, cursorOf(index)))
	}
}

func goodCursor(index int) string { return fmt.Sprintf("t%d", index) }

func runRootWalk(t *testing.T, which string, answer func(string, map[string]any) string) (*linearFuncDoer, error) {
	t.Helper()
	doer := &linearFuncDoer{answer: answer}
	withDiscoveryClient(t, doer)
	if which == "members" {
		_, err := discoverMembersLinear(context.Background(), secrets.NewValue("lin_api_test"), "ENG")
		return doer, err
	}
	credential := providerfoundation.NewCredential("linear", memberCredentialID, nil,
		map[string]secrets.Value{"api_key": secrets.NewValue("lin_api_test")})
	_, err := discoverLinear(context.Background(), credential)
	return doer, err
}

// CHAOS-8781 r1 P1-1: the ROOT teams walks (member discovery and team discovery)
// had no page bound. Literal numbers, both walks: 50 pages pass, 51 fail closed
// with no request past the bound.
func TestLinearRootTeamsWalksAreHeldToFiftyPages(t *testing.T) {
	for _, which := range []string{"members", "teams"} {
		t.Run(which, func(t *testing.T) {
			doer, err := runRootWalk(t, which, rootTeamsAnswer(50, goodCursor))
			if err != nil || len(doer.requests) != 50 {
				t.Fatalf("50 pages: err=%v requests=%d", err, len(doer.requests))
			}
			doer, err = runRootWalk(t, which, rootTeamsAnswer(51, goodCursor))
			if err == nil || !strings.Contains(err.Error(), "linear teams still had a next page after 50 pages (max 50 pages)") {
				t.Fatalf("51 pages: err=%v", err)
			}
			if len(doer.requests) != 50 {
				t.Fatalf("requests=%d want 50 (none past the bound)", len(doer.requests))
			}
			if strings.Contains(err.Error(), "lin_api_test") {
				t.Fatalf("error carries the credential: %q", err)
			}
		})
	}
}

// A root walk that said "next page" with no cursor, or with the cursor it just
// sent, fails closed; it never sends a second request that drops `after` or
// repeats it.
func TestLinearRootTeamsWalksFailClosedOnAnAmbiguousCursor(t *testing.T) {
	for _, which := range []string{"members", "teams"} {
		for _, test := range []struct {
			name     string
			cursorOf func(int) string
			want     string
		}{
			{"missing cursor", func(int) string { return "" }, "gave no cursor"},
			{"blank cursor", func(int) string { return "  " }, "gave no cursor"},
			{"cursor repeated", func(int) string { return "same" }, "cursor did not advance"},
		} {
			t.Run(which+"/"+test.name, func(t *testing.T) {
				doer, err := runRootWalk(t, which, rootTeamsAnswer(5, test.cursorOf))
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("err=%v want %q", err, test.want)
				}
				// "same": page 1 sends no cursor, page 2 sends "same", page 3 would
				// repeat it. "missing": no second request at all.
				limit := 2
				if test.name != "cursor repeated" {
					limit = 1
				}
				if len(doer.requests) > limit {
					t.Fatalf("requests=%d want at most %d", len(doer.requests), limit)
				}
			})
		}
	}
}

// memberFollowUpAnswer serves ENG whose inline member page says "next page" with
// embeddedCursor; each follow-up page advertises the cursor followCursor(index).
func memberFollowUpAnswer(embeddedCursor string, followCursor func(index int) string, pages int) func(string, map[string]any) string {
	member := func(index int) string {
		return fmt.Sprintf(`{"id":"u%d","name":"M%d","email":"m%d@example.com","active":true}`, index, index, index)
	}
	return func(query string, variables map[string]any) string {
		if strings.Contains(query, "query TeamMembers(") {
			index := 1
			if after, _ := variables["after"].(string); after != "" {
				fmt.Sscanf(after, "m%d", &index)
				index++
			}
			return fmt.Sprintf(`{"data":{"team":{"members":{"nodes":[%s],"pageInfo":%s}}}}`,
				member(index), teamPageInfo(index < pages-1, followCursor(index)))
		}
		return fmt.Sprintf(`{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Eng","members":{"nodes":[%s],"pageInfo":%s}}],"pageInfo":%s}}}`,
			member(0), teamPageInfo(true, embeddedCursor), teamPageInfo(false, ""))
	}
}

// CHAOS-8781 r1 P1-2 and T5: a member continuation that lacks a cursor (the
// embedded one, or a later one) or repeats the one just sent used to drop `after`
// or repeat the request and return the overlapping members as success. It fails
// closed now: no second request, no duplicate identity.
func TestDiscoverMembersLinearFailsClosedOnAnAmbiguousContinuationCursor(t *testing.T) {
	for _, test := range []struct {
		name           string
		embedded       string
		followCursor   func(int) string
		want           string
		maxMemberCalls int
	}{
		{"embedded cursor empty", "", func(i int) string { return fmt.Sprintf("m%d", i) }, "gave no cursor", 0},
		{"embedded cursor blank", "   ", func(i int) string { return fmt.Sprintf("m%d", i) }, "gave no cursor", 0},
		{"follow-up cursor missing", "m0", func(int) string { return "" }, "gave no cursor", 1},
		{"follow-up cursor unchanged", "m0", func(int) string { return "m0" }, "cursor did not advance", 1},
		{"follow-up cursor seen before", "m0", func(i int) string { return fmt.Sprintf("m%d", i%2) }, "cursor did not advance", 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &linearFuncDoer{answer: memberFollowUpAnswer(test.embedded, test.followCursor, 10)}
			withDiscoveryClient(t, doer)
			members, err := discoverMembersLinear(context.Background(), secrets.NewValue("lin_api_test"), "ENG")
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "members of team team-eng") {
				t.Fatalf("err=%v members=%v want an error naming the team and %q", err, members, test.want)
			}
			if members != nil {
				t.Fatalf("a failed discovery returned members: %v", members)
			}
			if got := doer.count("query TeamMembers("); got > test.maxMemberCalls {
				t.Fatalf("member follow-ups=%d want at most %d (no request that drops or repeats the cursor)", got, test.maxMemberCalls)
			}
		})
	}
}
