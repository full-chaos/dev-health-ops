package aianalytics

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The team scope of the reviewer concentration is the repositories the team
// owns (teamscope), never user_metrics_daily.team_id, a membership label.

type concentrationClient struct {
	*ownerClient
	concentration []string
	bindings      [][]clickhouse.Binding
}

func (c *concentrationClient) Query(ctx context.Context, st string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	switch {
	case strings.Contains(st, "AS reviews_total"):
		c.concentration = append(c.concentration, st)
		c.bindings = append(c.bindings, b)
		return &scriptedRows{rows: [][]any{{3.0}, {1.0}}}, nil
	case strings.Contains(st, "AS full_name"), strings.Contains(st, "{held_repo_ids:Array(String)}"), strings.Contains(st, "{owner_repo_ids:Array(String)}"):
		return c.ownerClient.Query(ctx, st, b)
	}
	return &scriptedRows{}, nil
}

func reviewLoadForTeam(t *testing.T, team string) (*concentrationClient, *model.AIReviewLoadResult) {
	t.Helper()
	client := &concentrationClient{ownerClient: newOwnerClient()}
	dr := model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}
	got, err := ReviewLoad(context.Background(), client, "org-1", dr, &model.AIScopeInput{TeamID: &team})
	if err != nil {
		t.Fatal(err)
	}
	return client, got
}

func TestReviewerConcentration_TeamScopeIsTheOwnedRepositories(t *testing.T) {
	client, got := reviewLoadForTeam(t, scopedTeamID)
	if len(client.concentration) != 1 {
		t.Fatalf("concentration statements = %d, want 1", len(client.concentration))
	}
	if st := client.concentration[0]; strings.Contains(st, "team_id") {
		t.Fatalf("the concentration read filters on a team_id column:\n%s", st)
	}
	var ids []string
	for _, b := range client.bindings[0] {
		if b.Name == "repo_ids" {
			ids, _ = b.Value.([]string)
		}
	}
	sort.Strings(ids)
	if want := []string{ownedRepo, coOwnedRepo}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("repo_ids = %v, want %v (owned and co-owned only)", ids, want)
	}
	if !got.ReviewerConcentration.DataAvailable || got.ReviewerConcentration.ReviewerCount != 2 {
		t.Fatalf("concentration = %+v, want 2 reviewers available", got.ReviewerConcentration)
	}
}

func TestReviewerConcentration_ATeamThatOwnsNothingReadsNoRows(t *testing.T) {
	client, got := reviewLoadForTeam(t, "team-none")
	if len(client.concentration) != 0 {
		t.Fatalf("a team that owns no repository ran %d concentration reads", len(client.concentration))
	}
	if got.ReviewerConcentration.DataAvailable || got.ReviewerConcentration.ReviewerCount != 0 {
		t.Fatalf("concentration = %+v, want no data", got.ReviewerConcentration)
	}
}

func TestNoQueryAPIReaderFiltersUserMetricsDailyByTeamID(t *testing.T) {
	root := filepath.Join("..")
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || filepath.Base(p) == "generated.go" {
			return err
		}
		raw, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if strings.Contains(string(raw), "umd.team_id") {
			t.Errorf("%s filters user_metrics_daily by team_id (a membership label, not ownership)", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
