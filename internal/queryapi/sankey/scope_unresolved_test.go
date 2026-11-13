package sankey

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type rn13705Rows struct {
	hits  []string
	index int
}

func (r *rn13705Rows) Next() bool { r.index++; return r.index <= len(r.hits) }
func (r *rn13705Rows) Scan(dest ...any) error {
	if p, ok := dest[0].(*string); ok {
		*p = r.hits[r.index-1]
	}
	return nil
}
func (r *rn13705Rows) Err() error   { return nil }
func (r *rn13705Rows) Close() error { return nil }

// rn13705Client records every statement; the existence read answers one hit when knownDeveloper is set.
type rn13705Client struct {
	knownDeveloper bool
	statements     []string
}

func (c *rn13705Client) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	if c.knownDeveloper && strings.Contains(statement, "user_metrics_daily") && strings.Contains(statement, "id_count") {
		return &rn13705Rows{hits: []string{"dev@example.com"}}, nil
	}
	return &rn13705Rows{}, nil
}

func sankeyParams(mode, level string, ids ...string) Params {
	return Params{Mode: mode, ScopeLevel: level, ScopeIDs: ids, StartDay: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndDay: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)}
}

// An unresolved service or developer scope answers an empty graph in every mode and reads no metric (CHAOS-13705, R335).
func TestAnUnresolvedServiceOrDeveloperScopeAnswersAnEmptyGraphInEveryMode(t *testing.T) {
	for _, mode := range []string{"investment", "expense", "state", "hotspot"} {
		for level, id := range map[string]string{"service": "svc-1", "developer": "nobody@example.com"} {
			c := &rn13705Client{}
			resp, err := BuildResponse(context.Background(), c, "org-a", sankeyParams(mode, level, id))
			if err != nil || resp == nil || len(resp.Nodes) != 0 || len(resp.Links) != 0 {
				t.Fatalf("%s %s: %+v err %v, want an empty graph", mode, level, resp, err)
			}
			wantReads := 0
			if level == "developer" {
				wantReads = 1
			}
			if len(c.statements) != wantReads {
				t.Errorf("%s %s: %d reads, want %d (the existence read only)", mode, level, len(c.statements), wantReads)
			}
		}
	}
}

// A known developer builds the graph as before (the reads of the mode run); an org scope names no id and resolves nothing.
func TestAKnownDeveloperAndAnOrgScopeStillBuild(t *testing.T) {
	c := &rn13705Client{knownDeveloper: true}
	if _, err := BuildResponse(context.Background(), c, "org-a", sankeyParams("expense", "developer", "dev@example.com")); err != nil || len(c.statements) < 2 {
		t.Fatalf("known developer: err %v, %d reads, want the mode's reads after the existence read", err, len(c.statements))
	}
	c = &rn13705Client{}
	if _, err := BuildResponse(context.Background(), c, "org-a", sankeyParams("expense", "org")); err != nil || len(c.statements) == 0 {
		t.Fatalf("org scope: err %v, %d reads, want the mode's reads", err, len(c.statements))
	}
}
