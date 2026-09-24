package busfactor

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The golden holds, for neutral requests (evidence rows of every churn shape,
// author identity fallbacks, several repositories, and the ways a repository
// scope id can be written), the parameters the reference resolver bound and
// the exact answer it built. It is frozen from the reference resolver that
// was deleted; nothing here calls Python.
//
// Named differences, not compared here and pinned by their own tests:
//   - a team scope: the reference selected a team's repositories by the
//     repositories its members had metrics for (user_metrics_daily); this
//     resolver selects them by team ownership (team_repo_ownership).
//   - equal-churn authors and equal-name repositories: this resolver orders
//     them by author key and repository id, the reference kept arrival order
//     (see TestTiebreaks_DoNotDependOnRowOrder).
//   - a repository scope whose id differs from the ids of the returned rows
//     cannot happen: the query itself filters on the scope id.

type busFactorGoldenCase struct {
	Name string `json:"name"`
	Rows []struct {
		Repo  string  `json:"repo"`
		RName *string `json:"rname"`
		Email *string `json:"email"`
		AName *string `json:"aname"`
		Add   int32   `json:"add"`
		Dele  int32   `json:"dele"`
	} `json:"rows"`
	Scope *struct {
		RepoID *string `json:"repo_id"`
		TeamID *string `json:"team_id"`
	} `json:"scope"`
	PyQueries int              `json:"python_queries"`
	PyParams  []map[string]any `json:"python_params"`
	Expected  struct {
		OrgID string `json:"org_id"`
		Scope struct {
			RepoID *string `json:"repo_id"`
			TeamID *string `json:"team_id"`
		} `json:"scope"`
		Value    int `json:"value"`
		Evidence int `json:"evidence_sample_count"`
		Top      []struct {
			Author string  `json:"author"`
			Share  float64 `json:"share"`
		} `json:"top"`
		Repos []struct {
			RepoID   string `json:"repo_id"`
			RepoName string `json:"repo_name"`
			Value    int    `json:"value"`
			Evidence int    `json:"evidence"`
			Top      []struct {
				Author string  `json:"author"`
				Share  float64 `json:"share"`
			} `json:"top"`
		} `json:"repos"`
	} `json:"expected"`
}

func TestBusFactorMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/busfactor_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []busFactorGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 30 {
		t.Fatalf("golden has %d cases", len(cases))
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var rows [][]any
			for _, r := range c.Rows {
				name := r.Repo
				if r.RName != nil {
					name = *r.RName
				}
				var e, a any
				if r.Email != nil {
					e = *r.Email
				}
				if r.AName != nil {
					a = *r.AName
				}
				rows = append(rows, []any{r.Repo, name, e, a, r.Add, r.Dele})
			}
			cl := &recordingClient{rows: rows}
			var scope *model.BusFactorScopeInput
			if c.Scope != nil {
				scope = &model.BusFactorScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
			}
			got, err := Resolve(context.Background(), cl, "org-1", scope, time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if c.PyQueries != 1 || len(c.PyParams) != 1 {
				t.Fatalf("golden records %d queries", c.PyQueries)
			}
			bound := map[string]any{}
			for _, b := range cl.bindings {
				if b.Name == "org_id" || b.Name == "repo_id" {
					bound[b.Name] = b.Value
				}
			}
			if !reflect.DeepEqual(bound, c.PyParams[0]) {
				t.Errorf("params %v want %v", bound, c.PyParams[0])
			}
			e := c.Expected
			if got.OrgID != e.OrgID || got.Value != e.Value || got.EvidenceSampleCount != e.Evidence {
				t.Errorf("head: %d/%d want %d/%d", got.Value, got.EvidenceSampleCount, e.Value, e.Evidence)
			}
			if !reflect.DeepEqual(got.Scope.RepoID, e.Scope.RepoID) || !reflect.DeepEqual(got.Scope.TeamID, e.Scope.TeamID) {
				t.Errorf("scope %v/%v want %v/%v", deref(got.Scope.RepoID), deref(got.Scope.TeamID), deref(e.Scope.RepoID), deref(e.Scope.TeamID))
			}
			if len(got.TopMaintainers) != len(e.Top) {
				t.Fatalf("top %d want %d", len(got.TopMaintainers), len(e.Top))
			}
			for i, m := range got.TopMaintainers {
				if m.Author != e.Top[i].Author || m.SharePercent != e.Top[i].Share {
					t.Errorf("top[%d] %v want %v", i, m, e.Top[i])
				}
			}
			if len(got.Repos) != len(e.Repos) {
				t.Fatalf("repos %d want %d", len(got.Repos), len(e.Repos))
			}
			for i, r := range got.Repos {
				w := e.Repos[i]
				if r.RepoID != w.RepoID || r.RepoName != w.RepoName || r.Value != w.Value || r.EvidenceSampleCount != w.Evidence || len(r.TopMaintainers) != len(w.Top) {
					t.Fatalf("repo[%d] %v want %v", i, r, w)
				}
				for j, m := range r.TopMaintainers {
					if m.Author != w.Top[j].Author || m.SharePercent != w.Top[j].Share {
						t.Errorf("repo[%d].top[%d] %v want %v", i, j, m, w.Top[j])
					}
				}
			}
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
