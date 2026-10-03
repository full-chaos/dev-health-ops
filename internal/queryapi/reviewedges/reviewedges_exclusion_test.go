package reviewedges

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-7787: bots (login ends in "[bot]") and self pairs are excluded at read time, inside the
// inner WHERE that both the row query and the count query share, before the dedup and the LIMIT.

func TestExclusion_FragmentHasExactlyTheThreeClauses(t *testing.T) {
	f := norm(ExcludedIdentitiesFragment)
	for _, want := range []string{
		"AND NOT endsWith(lowerUTF8(trimBoth(reviewer)), '[bot]')",
		"AND NOT endsWith(lowerUTF8(trimBoth(author)), '[bot]')",
		"AND lowerUTF8(trimBoth(reviewer)) != lowerUTF8(trimBoth(author))",
	} {
		if !strings.Contains(f, want) {
			t.Errorf("fragment lacks %q: %s", want, f)
		}
	}
	if strings.Count(f, "AND ") != 3 {
		t.Errorf("fragment must hold exactly three clauses (no other heuristic): %s", f)
	}
}

func TestExclusion_SitsInTheInnerWhereBeforeFiltersGroupByAndLimit(t *testing.T) {
	for name, scope := range map[string]Scope{
		"no scope":    {},
		"repo + team": {RepoIDs: []string{"repo-a"}, TeamIDs: []string{"team-a"}},
	} {
		t.Run(name, func(t *testing.T) {
			statement, _ := scopedStatement(t, scope, 500)
			s := norm(statement)
			frag := norm(ExcludedIdentitiesFragment)
			if strings.Count(s, frag) != 1 {
				t.Fatalf("the exclusion must appear exactly once in the row statement: %s", s)
			}
			at := strings.Index(s, frag)
			until := strings.Index(s, "AND day <= {until_date:Date}")
			groupBy := strings.Index(s, "GROUP BY")
			limit := strings.Index(s, "LIMIT {limit:UInt64}")
			if !(until < at && at < groupBy && groupBy < limit) {
				t.Errorf("order must be window < exclusion < GROUP BY < LIMIT: until=%d at=%d groupBy=%d limit=%d", until, at, groupBy, limit)
			}
			if r := strings.Index(s, "{repo_ids:Array(String)}"); r >= 0 && r < at {
				t.Errorf("the exclusion must come before the repo filter")
			}
			if m := strings.Index(s, teamscope.Marker); m >= 0 && m < at {
				t.Errorf("the exclusion must come before the team filter")
			}
		})
	}
}

func TestExclusion_TheCountQueryCarriesIt_SoTotalCountMatchesTheEdges(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(1)}}
	resolveWith(t, client, Scope{}, 500)
	if !strings.Contains(norm(client.countStatement), norm(ExcludedIdentitiesFragment)) {
		t.Errorf("the count must exclude the same identities as the rows: %s", norm(client.countStatement))
	}
}

func TestExclusion_AddsNoBindingAndNoArgument(t *testing.T) {
	_, bindings := scopedStatement(t, Scope{}, 500)
	names := map[string]bool{}
	for _, b := range bindings {
		names[b.Name] = true
	}
	for _, want := range []string{"org_id", "since_date", "until_date", "limit"} {
		if !names[want] {
			t.Errorf("missing binding %q", want)
		}
	}
	if len(names) != 4 {
		t.Errorf("the exclusion is a constant: no binding may be added, got %v", names)
	}
}
