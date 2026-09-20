package experiments

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/home"
)

// recordingCH answers every read with no rows and keeps the statements.
type recordingCH struct {
	mu   sync.Mutex
	sqls []string
}

func (c *recordingCH) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	c.sqls = append(c.sqls, statement)
	c.mu.Unlock()
	return noRows{}, nil
}

type noRows struct{}

func (noRows) Next() bool        { return false }
func (noRows) Scan(...any) error { return nil }
func (noRows) Err() error        { return nil }
func (noRows) Close() error      { return nil }

func (c *recordingCH) countWith(fragment string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.sqls {
		if strings.Contains(s, fragment) {
			n++
		}
	}
	return n
}

// A team scope that resolves to nothing narrows the repository-keyed reads to
// the team's owned repositories and answers the steady-flow card; it never
// drops the filter and answers the whole org.
func TestResolve_UnknownTeamNarrowsTheReadsAndNeverWidensToTheOrg(t *testing.T) {
	team := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"deleted-team"}}}
	org := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputOrg}}

	scoped := &recordingCH{}
	got := Resolve(context.Background(), NewBuilder(home.QueryClient(scoped)), "org-1", team, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if !got.DerivedFromOpportunities || len(got.Items) != 1 || got.Items[0].OpportunityID != "opp-0" {
		t.Fatalf("unknown team answer = %+v", got)
	}
	if scoped.countWith("team_repo_ownership") == 0 {
		t.Fatal("a team scope must read through team ownership")
	}

	unscoped := &recordingCH{}
	Resolve(context.Background(), NewBuilder(home.QueryClient(unscoped)), "org-1", org, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if unscoped.countWith("team_repo_ownership") != 0 {
		t.Fatal("the org scope must not read team ownership")
	}
}
