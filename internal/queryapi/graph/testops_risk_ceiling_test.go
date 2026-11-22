package graph

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CHAOS-13814 and the repository ceiling: testopsRisk stays a narrowable root (N). A caller under a ceiling (an
// allowlist scope) reads as without it when it names no scope, narrows to the repositories of the ceiling when it
// names repoIds, and is refused a team argument before any read.
func riskCall(t *testing.T, scope pepgate.Scope, input model.TestOpsRiskInput) (*statementRecordingClient, error) {
	t.Helper()
	day, err := graphqldate.Parse("2026-01-01")
	if err != nil {
		t.Fatal(err)
	}
	end, err := graphqldate.Parse("2026-01-31")
	if err != nil {
		t.Fatal(err)
	}
	input.StartDate, input.EndDate = day, end
	ch := &statementRecordingClient{}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1", Scope: scope})
	_, callErr := (&Resolver{ClickHouse: ch}).Query().TestopsRisk(ctx, "org-1", input)
	return ch, callErr
}

func repoIDsBound(c *statementRecordingClient, i int) any {
	for _, b := range c.bindings[i] {
		if b.Name == "repo_ids" {
			return b.Value
		}
	}
	return nil
}

func TestTestopsRisk_UnderACeiling(t *testing.T) {
	ceiling := pepgate.Allowlist("org-1", []string{"acme/web", "acme/api"})
	base, err := riskCall(t, pepgate.Legacy("org-1"), model.TestOpsRiskInput{})
	if err != nil || len(base.statements) != 2 {
		t.Fatalf("base: %v, %d statements", err, len(base.statements))
	}

	t.Run("(i) a ceiling and no scope argument reads as without the ceiling", func(t *testing.T) {
		for name, input := range map[string]model.TestOpsRiskInput{
			"absent":     {},
			"empty":      {RepoIds: []string{}, TeamIds: []string{}},
			"blank team": {TeamIds: []string{""}},
		} {
			got, err := riskCall(t, ceiling, input)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if !reflect.DeepEqual(got.statements, base.statements) || !reflect.DeepEqual(got.bindings, base.bindings) {
				t.Errorf("%s: a ceilinged request with no scope argument sent other statements or bindings than main", name)
			}
		}
	})

	t.Run("(ii) a ceiling and a team argument is refused before any read", func(t *testing.T) {
		for name, scope := range map[string]pepgate.Scope{"allowlist": ceiling, "empty allowlist": pepgate.Allowlist("org-1", nil)} {
			got, err := riskCall(t, scope, model.TestOpsRiskInput{TeamIds: []string{"team-1"}, RepoIds: []string{"acme/web"}})
			var gqlErr *gqlerror.Error
			if !asGQLError(err, &gqlErr) || gqlErr.Message != "Not authorized" || gqlErr.Extensions["code"] != CodeForbidden {
				t.Fatalf("%s: err = %#v, want the FORBIDDEN deny", name, err)
			}
			if len(got.statements) != 0 {
				t.Errorf("%s: %d reads ran for a refused request; the ownership tables were reachable", name, len(got.statements))
			}
		}
	})

	t.Run("(iii) repoIds are cut to the ceiling and never widen", func(t *testing.T) {
		inside, err := riskCall(t, ceiling, model.TestOpsRiskInput{RepoIds: []string{"acme/web"}})
		if err != nil {
			t.Fatal(err)
		}
		if got := repoIDsBound(inside, 0); !reflect.DeepEqual(got, []string{"acme/web"}) {
			t.Errorf("inside: repo_ids = %v", got)
		}
		mixed, err := riskCall(t, ceiling, model.TestOpsRiskInput{RepoIds: []string{"acme/lib", "acme/api", "other/x"}})
		if err != nil {
			t.Fatal(err)
		}
		for i := range mixed.statements {
			if got := repoIDsBound(mixed, i); !reflect.DeepEqual(got, []string{"acme/api"}) {
				t.Errorf("mixed statement %d: repo_ids = %v, want only the id of the ceiling", i, got)
			}
		}
		outside, err := riskCall(t, ceiling, model.TestOpsRiskInput{RepoIds: []string{"acme/lib"}})
		if err != nil {
			t.Fatal(err)
		}
		for i, st := range outside.statements {
			if st == base.statements[i] {
				t.Errorf("outside statement %d is the organization statement: the request widened", i)
			}
			if !strings.Contains(st, "0 = 1") || repoIDsBound(outside, i) != nil {
				t.Errorf("outside statement %d does not match nothing", i)
			}
		}
	})

	t.Run("(iv) no ceiling and a team argument is the scoped answer", func(t *testing.T) {
		got, err := riskCall(t, pepgate.Organization("org-1"), model.TestOpsRiskInput{TeamIds: []string{"team-1"}})
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range []int{3, 1} {
			if n := strings.Count(got.statements[i], "owned_repo_id"); n != want {
				t.Errorf("statement %d: %d ownership clauses, want %d", i, n, want)
			}
		}
	})

	t.Run("(v) the census keeps the root narrowable and names the refusal", func(t *testing.T) {
		if rootClasses["Query.testopsRisk"].class != classNarrowable {
			t.Error("Query.testopsRisk is not class N")
		}
		if teamArgumentRefusedUnderCeiling["Query.testopsRisk"] != "refuseTeamsUnderCeiling" {
			t.Error("the census does not name the refusal arm of Query.testopsRisk")
		}
	})
}
