package aianalytics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// The prod repro of the AI analytics team filter: in prod a team that owns
// repositories through team_repo_ownership and has no repo_patterns (pm read
// 2026-11-13: one such team owns 10 repositories with 898 attribution rows in
// 30 days) got an empty team scope. Its scope is the repositories it owns.
const (
	prodReproTeam = "linear:TEAM"
	prodReproRepo = "dddddddd-0000-0000-0000-000000000001"
)

// prodReproClient answers the team catalogue (the team, no patterns), the
// repository catalogue and the ownership read (the team owns the repository).
type prodReproClient struct{}

func (prodReproClient) Query(_ context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	switch {
	case strings.Contains(st, "{held_repo_ids:Array(String)}"):
		return &scriptedRows{rows: [][]any{{prodReproRepo}}}, nil
	case strings.Contains(st, "AS full_name"):
		return &scriptedRows{rows: [][]any{{prodReproRepo, "acme/owned-no-pattern"}}}, nil
	case strings.Contains(st, "FROM teams"):
		return &scriptedRows{rows: [][]any{{prodReproTeam, "Team", []string{}}}}, nil
	}
	return nil, errors.New("prodReproClient: unscripted statement: " + st)
}

func TestTeamScope_ProdRepro_OwningTeamWithoutPatternsHasItsRepositories(t *testing.T) {
	ids, ok := teamRepoIDs(context.Background(), prodReproClient{}, "org-1", prodReproTeam, "test")
	if !ok {
		t.Fatal("the team scope must resolve")
	}
	if fmt.Sprint(ids) != fmt.Sprint([]string{prodReproRepo}) {
		t.Fatalf("team scope = %v, want the owned repository [%s]", ids, prodReproRepo)
	}
}
