package experiments

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

type emptyScanner struct{}

func (emptyScanner) Next() bool        { return false }
func (emptyScanner) Scan(...any) error { return nil }
func (emptyScanner) Err() error        { return nil }
func (emptyScanner) Close() error      { return nil }

type emptyClient struct{}

func (emptyClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyScanner{}, nil
}

// An unresolved scope answers no experiment (CHAOS-6119, R335), while the org scope keeps the suggestion of its fallback card.
func TestResolveAnswersNoExperimentForAnUnresolvedScope(t *testing.T) {
	build := NewBuilder(emptyClient{})
	now := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)
	for _, level := range []model.ScopeLevelInput{model.ScopeLevelInputRepo, model.ScopeLevelInputService, model.ScopeLevelInputDeveloper} {
		in := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: level, Ids: []string{"unknown-id"}}}
		got := Resolve(context.Background(), build, "org-a", in, now)
		if len(got.Items) != 0 {
			t.Errorf("%s: %d experiments, want none", level, len(got.Items))
		}
	}
	if got := Resolve(context.Background(), build, "org-a", nil, now); len(got.Items) == 0 {
		t.Error("org scope: no experiment, want the fallback card's suggestion")
	}
}
