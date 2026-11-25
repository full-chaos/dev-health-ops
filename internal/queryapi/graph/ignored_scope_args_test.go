package graph

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// scopeProbeClient records every statement and its bindings, answers the developer existence read of a scope with
// one hit (so the scope is known, not unresolved), and answers every other read with no rows.
type scopeProbeClient struct {
	mu         sync.Mutex
	statements []string
	bindings   [][]clickhouse.Binding
}

const developerExistenceMarker = "SELECT hit FROM"

func (c *scopeProbeClient) Query(_ context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b)
	if strings.Contains(statement, developerExistenceMarker) {
		return &oneHitRows{}, nil
	}
	return emptyRows{}, nil
}

// readsCarrying counts the reads, other than the developer existence read, whose statement or bindings name want.
func (c *scopeProbeClient) readsCarrying(want string) (carrying, total int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, statement := range c.statements {
		if strings.Contains(statement, developerExistenceMarker) {
			continue
		}
		total++
		if strings.Contains(statement, want) {
			carrying++
			continue
		}
		for _, b := range c.bindings[i] {
			raw, _ := json.Marshal(b.Value)
			if strings.Contains(string(raw), want) {
				carrying++
				break
			}
		}
	}
	return carrying, total
}

func (c *scopeProbeClient) issued(marker string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, statement := range c.statements {
		if strings.Contains(statement, marker) {
			return true
		}
	}
	return false
}

type oneHitRows struct{ done bool }

func (r *oneHitRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}

func (r *oneHitRows) Scan(dest ...any) error {
	if p, ok := dest[0].(*string); ok {
		*p = "dev-synthetic-a@example.invalid"
	}
	return nil
}
func (r *oneHitRows) Err() error   { return nil }
func (r *oneHitRows) Close() error { return nil }

func scopeProbeContext() context.Context {
	return authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-synthetic-1"})
}

// complexityTimeseries takes teamIds; a team scope reaches the read, as repoIds does.
func TestComplexityTimeseriesTeamIdsReachTheRead(t *testing.T) {
	run := func(t *testing.T, repoIDs, teamIDs []string) *scopeProbeClient {
		t.Helper()
		ch := &scopeProbeClient{}
		r := &Resolver{ClickHouse: ch}
		_, err := r.Query().ComplexityTimeseries(scopeProbeContext(), model.ComplexityTimeseriesInput{
			OrgID:       "org-synthetic-1",
			SinceUtc:    time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			UntilUtc:    time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC),
			Granularity: model.TimeGranularityDay,
			Scope:       model.ComplexityScopeRepo,
			RepoIds:     repoIDs,
			TeamIds:     teamIDs,
		})
		if err != nil {
			t.Fatalf("complexityTimeseries: %v", err)
		}
		return ch
	}
	byRepo := run(t, []string{"repo-synthetic-a"}, nil)
	if n, total := byRepo.readsCarrying("repo-synthetic-a"); n == 0 {
		t.Fatalf("control: repoIds [repo-synthetic-a] reaches none of %d reads", total)
	}
	t.Log("control: repoIds [repo-synthetic-a] reaches the read")
	byTeam := run(t, nil, []string{"team-synthetic-a"})
	if n, total := byTeam.readsCarrying("team-synthetic-a"); n == 0 {
		t.Errorf("teamIds [team-synthetic-a] reaches none of %d reads: the team scope serves the org-wide complexity trend", total)
	}
}

// home with a known developer scope narrows its reads to that developer, as a team scope narrows them to the team.
func TestHomeDeveloperScopeReachesTheReads(t *testing.T) {
	run := func(t *testing.T, level model.ScopeLevelInput, id string) *scopeProbeClient {
		t.Helper()
		ch := &scopeProbeClient{}
		r := &Resolver{ClickHouse: ch, Postgres: pgNoRows{&recordingHomePGClient{}}}
		filters := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: level, Ids: []string{id}}}
		if _, err := r.Query().Home(scopeProbeContext(), "org-synthetic-1", filters, nil); err != nil {
			t.Fatalf("home %s: %v", level, err)
		}
		return ch
	}
	byTeam := run(t, model.ScopeLevelInputTeam, "team-synthetic-a")
	if n, total := byTeam.readsCarrying("team-synthetic-a"); n == 0 {
		t.Fatalf("control: scope TEAM [team-synthetic-a] reaches none of %d reads", total)
	} else {
		t.Logf("control: scope TEAM reaches %d of %d reads", n, total)
	}
	const developer = "dev-synthetic-a@example.invalid"
	byDeveloper := run(t, model.ScopeLevelInputDeveloper, developer)
	if !byDeveloper.issued(developerExistenceMarker) {
		t.Fatal("control: the developer existence read was not issued, so the scope was not resolved as known")
	}
	if n, total := byDeveloper.readsCarrying(developer); n == 0 {
		t.Errorf("scope DEVELOPER [%s] (known: the existence read finds it) reaches none of %d data reads: the developer view serves org-wide home data", developer, total)
	}
}
