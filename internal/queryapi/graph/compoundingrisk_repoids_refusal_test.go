package graph

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The resolver wraps the Resolve error with %w; the BAD_USER_INPUT refusal of
// a repoIds list over 500 stays a cause-free *gqlerror.Error in that chain, so
// the presenter serves its message and extensions, and ClickHouse is never
// asked.
func TestCompoundingRiskResolverKeepsTheRepoIdsRefusal(t *testing.T) {
	ch := &scopeRecordingClient{}
	r := &Resolver{ClickHouse: ch}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
	repoIDs := make([]string, 501)
	for i := range repoIDs {
		repoIDs[i] = fmt.Sprintf("r-%03d", i)
	}
	got, err := r.Query().CompoundingRisk(ctx, "org-1", &model.CompoundingRiskFilterInput{Breakout: model.CompoundingRiskScopeRepo, RepoIds: repoIDs, TrendDays: 30})
	var gqlErr *gqlerror.Error
	if got != nil || !errors.As(err, &gqlErr) || gqlErr.Err != nil {
		t.Fatalf("result %v err %v; want no result and a cause-free *gqlerror.Error in the chain", got, err)
	}
	presented := graphql.DefaultErrorPresenter(ctx, err)
	want := "compoundingRisk: filter.repoIds holds 501 entries; the limit is 500"
	if presented.Message != want || presented.Extensions["code"] != "BAD_USER_INPUT" || presented.Extensions["field"] != "filter.repoIds" || presented.Extensions["cap"] != 500 {
		t.Errorf("presented %q %v; want %q with code BAD_USER_INPUT, field filter.repoIds, cap 500", presented.Message, presented.Extensions, want)
	}
	if len(ch.calls) != 0 {
		t.Errorf("%d ClickHouse statements; want 0", len(ch.calls))
	}
}
