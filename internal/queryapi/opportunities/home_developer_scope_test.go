package opportunities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

const oppDevScopeID = "dev-synthetic-a@example.invalid"

type oppDevOneHitRows struct{ done bool }

func (r *oppDevOneHitRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}
func (r *oppDevOneHitRows) Scan(dest ...any) error {
	if s, ok := dest[0].(*string); ok {
		*s = oppDevScopeID
	}
	return nil
}
func (r *oppDevOneHitRows) Err() error   { return nil }
func (r *oppDevOneHitRows) Close() error { return nil }

// oppDevClient records every statement and its bindings; known answers the developer existence read with a hit.
type oppDevClient struct {
	known      bool
	mu         sync.Mutex
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *oppDevClient) Query(_ context.Context, statement string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b)
	if c.known && strings.Contains(statement, "SELECT hit FROM") {
		return &oppDevOneHitRows{}, nil
	}
	return emptyRows{}, nil
}

var oppDevDate = regexp.MustCompile(`\d{4}-\d{2}-\d{2}([T ][0-9:.]+(Z|[+-][0-9:]+)?)?`)

func (c *oppDevClient) digest() (string, int) {
	lines := make([]string, 0, len(c.statements))
	for i, statement := range c.statements {
		bindings := append([]dhclickhouse.Binding(nil), c.bindings[i]...)
		sort.Slice(bindings, func(a, b int) bool { return bindings[a].Name < bindings[b].Name })
		raw, _ := json.Marshal(bindings)
		lines = append(lines, statement+"\n"+oppDevDate.ReplaceAllString(string(raw), "D"))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n--\n")))
	return hex.EncodeToString(sum[:]), len(lines)
}

func oppDevBuild(t *testing.T, c *oppDevClient, f home.Filters) *Response {
	t.Helper()
	f.Time = home.TimeFilter{RangeDays: 30, CompareDays: 30}
	resp, err := BuildResponse(context.Background(), c, "org-a", f, time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// Under a developer scope the Home half of opportunities answers no data: every metric value read carries the no-data filter,
// and the answer has no card (as for an id the organization does not know).
func TestOpportunitiesUnderAKnownDeveloperScopeServeNoOrganizationNumber(t *testing.T) {
	c := &oppDevClient{known: true}
	resp := oppDevBuild(t, c, home.Filters{Scope: home.ScopeFilter{Level: "developer", IDs: []string{oppDevScopeID}}})
	values := 0
	for _, s := range c.statements {
		if strings.Contains(s, "AS value") || strings.HasPrefix(strings.TrimSpace(s), "SELECT day") {
			values++
			if !strings.Contains(s, "1 = 0") {
				t.Errorf("a value read without the no-data filter: %.120s", strings.Join(strings.Fields(s), " "))
			}
		}
	}
	if values == 0 {
		t.Fatal("no value read issued: the cell measures nothing")
	}
	t.Logf("cards=%d", len(resp.Items))
}

// The organization scope reads exactly what main read: the digest is taken at main (5dd4758412f2) from this code.
func TestOpportunitiesOrgScopeReadsAreByteEqualToMain(t *testing.T) {
	c := &oppDevClient{}
	oppDevBuild(t, c, home.DefaultFilters())
	const mainDigest = "4222f5903a63d62d3ec00d0b600e6350395d471f958e87b46f7e511f58ca7a99"
	if digest, reads := c.digest(); digest != mainDigest || reads != 40 {
		t.Fatalf("org scope reads changed: digest %s (%d reads), main %s (40 reads)", digest, reads, mainDigest)
	}
}
