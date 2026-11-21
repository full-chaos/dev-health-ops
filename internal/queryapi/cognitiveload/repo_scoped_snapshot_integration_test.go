//go:build integration

package cognitiveload

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// writeAfterFirstQuery runs after once, after the first statement it passes
// through has returned its rows: a recompute that lands between two reads.
type writeAfterFirstQuery struct {
	inner   QueryClient
	after   func()
	queries int
}

func (c *writeAfterFirstQuery) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	rows, err := c.inner.Query(ctx, statement, bindings)
	c.queries++
	if c.queries == 1 {
		c.after()
	}
	return rows, err
}

func TestRepoScopedTeamMetricsReadsOneSnapshotAcrossAWrite(t *testing.T) {
	env := startTieEnv(t)
	newer := tieVersion{tieTeam, 8, 2, 4, 0.25, 0.5, "2026-09-01 04:00:00"}
	for name, seed := range map[string][]tieVersion{
		"retraction-only":           {tieRetraction(tieAt)},
		"tie-retraction-picked":     {tieMeasured(0, tieAt), tieRetraction(tieAt)},
		"tie-retraction-picked-alt": {tieRetraction(tieAt), tieMeasured(0, tieAt)},
	} {
		t.Run(name, func(t *testing.T) {
			env.seedRepo(t, seed...)
			before := env.readRepo(t)
			env.seedRepo(t, append(append([]tieVersion{}, seed...), newer)...)
			after := env.readRepo(t)
			env.seedRepo(t, seed...)
			c := &writeAfterFirstQuery{inner: env.client, after: func() {
				env.exec(t, strings.Replace(newer.insert(), "'repo-1')", "'"+tieRepo+"')", 1))
			}}
			got, err := fetchRepoScopedTeamMetrics(env.ctx, c, tieOrg, "2026-08-01", "2026-08-31", tieRepo)
			if err != nil {
				t.Fatal(err)
			}
			served := fmt.Sprintf("%+v", got)
			if served != before && served != after {
				t.Errorf("across a write the read serves %s, neither the read before (%s) nor the read after (%s)", served, before, after)
			}
			if c.queries != 1 {
				t.Errorf("the read ran %d statements, want 1", c.queries)
			}
		})
	}
}
