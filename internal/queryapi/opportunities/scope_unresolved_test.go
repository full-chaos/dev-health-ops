package opportunities

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

type emptyRows struct{}

func (emptyRows) Next() bool        { return false }
func (emptyRows) Scan(...any) error { return nil }
func (emptyRows) Err() error        { return nil }
func (emptyRows) Close() error      { return nil }

type countingClient struct{ queries atomic.Int32 }

func (c *countingClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries.Add(1)
	return emptyRows{}, nil
}

// An unresolved scope answers no card at all, not the "Maintain steady flow" fallback card (CHAOS-6119, R335).
func TestBuildResponseAnswersNoCardForAnUnresolvedScopeAtEveryLevel(t *testing.T) {
	for level, id := range map[string]string{"repo": "nope", "service": "svc-1", "developer": "nobody@example.com"} {
		client := &countingClient{}
		f := home.Filters{Time: home.TimeFilter{RangeDays: 30, CompareDays: 30}, Scope: home.ScopeFilter{Level: level, IDs: []string{id}}}
		resp, err := BuildResponse(context.Background(), client, "org-a", f, time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatalf("%s: %v", level, err)
		}
		if len(resp.Items) != 0 || resp.Items == nil {
			t.Errorf("%s: %d cards, want an empty non-nil list", level, len(resp.Items))
		}
	}
}

// With nothing named the request still builds: the fallback card stays for an org scope.
func TestBuildResponseKeepsTheFallbackCardForAnOrgScope(t *testing.T) {
	resp, err := BuildResponse(context.Background(), &countingClient{}, "org-a", home.Filters{Time: home.TimeFilter{RangeDays: 30, CompareDays: 30}, Scope: home.ScopeFilter{Level: "org"}}, time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil || len(resp.Items) != 1 || resp.Items[0].ID != "opp-0" {
		t.Fatalf("org scope: %v %+v", err, resp)
	}
}
