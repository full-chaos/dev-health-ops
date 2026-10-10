//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// REST Home says why the repositories a request names matched nothing
// (CHAOS-9098), one value for the whole answer: every combination of named
// repositories (held by the team, existing but not held, unknown), with and
// without a team scope, with and without rows in the window. Filters AND and
// never widen (D5844); an id that resolves to nothing is no data (D5841).
// Real handler, real ClickHouse.
func TestRESTHomeNamesWhyAFilterCombinationMatchedNothing(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const org = "filter-empty-reason"
	held, notHeld, bare := uuid.New(), uuid.New(), uuid.New()
	seedTeamScopeRepo(t, conn, org, "acme/held", held)
	seedTeamScopeRepo(t, conn, org, "acme/not-held", notHeld)
	seedTeamScopeRepo(t, conn, org, "acme/bare", bare) // exists, owned by no team, and has no rows
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedTeamScopeOwnership(t, conn, org, "team-one", "acme/held", "exact", "inferred", &held, validFrom, nil, validFrom)
	seedTeamScopeOwnership(t, conn, org, "team-two", "acme/not-held", "exact", "inferred", &notHeld, validFrom, nil, validFrom)
	for r, churn := range map[uuid.UUID]uint32{held: 5, notHeld: 100} {
		if err := conn.Exec(context.Background(),
			`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			r, time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), churn, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
			t.Fatal(err)
		}
	}
	unknown := uuid.New().String()
	reason := func(body string) any {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(context.Background(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		newHomePostHandler(client, nil)(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST /api/v1/home = HTTP %d: %s", rec.Code, rec.Body.String())
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		raw, present := out["filter_empty_reason"]
		if !present {
			t.Fatalf("the answer has no filter_empty_reason key: %s", rec.Body.String())
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	window := `"time":{"range_days":7,"compare_days":7,"end_date":"2026-08-25"}`
	filters := func(scope string, repos ...string) string {
		quoted := make([]string, len(repos))
		for i, r := range repos {
			quoted[i] = `"` + r + `"`
		}
		what := ""
		if len(repos) > 0 {
			what = `"what":{"repos":[` + strings.Join(quoted, ",") + `]},`
		}
		return `{"filters":{` + scope + what + window + `}}`
	}
	team := `"scope":{"level":"team","ids":["team-one"]},`
	cases := []struct {
		name string
		body string
		want any
	}{
		{"no repository, no team", filters(""), nil},
		{"no repository, a team", filters(team), nil},
		{"an empty string names nothing", filters(team, ""), nil},
		{"a held repository", filters(team, held.String()), nil},
		{"a held repository by name", filters(team, "acme/held"), nil},
		{"a repository the team does not hold", filters(team, notHeld.String()), "repository_not_in_team"},
		{"a repository that exists, no rows, no owner", filters(team, bare.String()), "repository_not_in_team"},
		{"several that exist, none held", filters(team, notHeld.String(), bare.String()), "repository_not_in_team"},
		{"an unknown repository id", filters(team, unknown), "repository_not_found"},
		{"an unknown repository name", filters(team, "acme/nothing"), "repository_not_found"},
		{"part held, part not held: the part that matched", filters(team, held.String(), notHeld.String()), nil},
		{"part held, part unknown: the part that matched", filters(team, held.String(), unknown), nil},
		{"one not held, one unknown: nothing matched, one resolved to nothing", filters(team, notHeld.String(), unknown), "repository_not_found"},
		{"no team scope, a repository that exists", filters("", notHeld.String()), nil},
		{"no team scope, an existing repository with no rows", filters("", bare.String()), nil},
		{"no team scope, an unknown repository", filters("", unknown), "repository_not_found"},
		{"a repository-level scope that exists", filters(`"scope":{"level":"repo","ids":["` + held.String() + `"]},`), nil},
		{"a repository-level scope that is unknown", filters(`"scope":{"level":"repo","ids":["` + unknown + `"]},`), "repository_not_found"},
	}
	for _, c := range cases {
		if got := reason(c.body); got != c.want {
			t.Errorf("%s: filter_empty_reason = %v, want %v\n  body %s", c.name, got, c.want, c.body)
		}
	}
}
