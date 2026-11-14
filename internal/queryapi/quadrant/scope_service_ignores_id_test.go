package quadrant

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type emptyRows struct{}

func (emptyRows) Next() bool        { return false }
func (emptyRows) Scan(...any) error { return nil }
func (emptyRows) Err() error        { return nil }
func (emptyRows) Close() error      { return nil }

type recordingClient struct {
	statements []string
	bindings   []string
}

func (c *recordingClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	for _, b := range bindings {
		c.bindings = append(c.bindings, fmt.Sprint(b.Value))
	}
	return emptyRows{}, nil
}

// On the quadrant scope_type=service is a team-grain selector: the scope id is ignored, so a known id, an unknown id and no id read the same statements (CHAOS-13736, ruled by design).
func TestAServiceScopeOnTheQuadrantIgnoresTheScopeID(t *testing.T) {
	end := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for typ := range QuadrantDefinitions {
		var want []string
		for _, id := range []string{"", "svc-known", "svc-unknown-7d1f"} {
			c := &recordingClient{}
			if _, err := BuildResponse(context.Background(), c, "org-a", Params{Type: typ, ScopeType: "service", ScopeID: id, Bucket: "week", RangeDays: 30, EndDate: &end}); err != nil {
				t.Fatalf("%s id %q: %v", typ, id, err)
			}
			if len(c.statements) == 0 {
				t.Fatalf("%s id %q: no statement recorded", typ, id)
			}
			for _, b := range c.bindings {
				if id != "" && b == id {
					t.Errorf("%s: scope id %q reached a binding", typ, id)
				}
			}
			if id == "" {
				want = c.statements
				continue
			}
			if !reflect.DeepEqual(c.statements, want) {
				t.Errorf("%s id %q: statements differ from the no-id read", typ, id)
			}
		}
	}
}
