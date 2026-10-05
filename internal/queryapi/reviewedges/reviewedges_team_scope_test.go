package reviewedges

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-7785: a team scope on reviewEdges, through the shared teamscope.RepoCondition
// (repository ownership, never person membership), ANDed beside the repo filter and before the
// dedup and the LIMIT.

func scopedStatement(t *testing.T, scope Scope, limit int) (string, []clickhouse.Binding) {
	t.Helper()
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	if _, err := ResolveScoped(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), scope, limit); err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	return client.statement, client.bindings
}

func bindingValues(bindings []clickhouse.Binding) map[string]any {
	out := map[string]any{}
	for _, b := range bindings {
		out[b.Name] = b.Value
	}
	return out
}

func TestResolveScoped_TeamConditionIsInTheInnerWhereBeforeGroupByAndLimit(t *testing.T) {
	statement, bindings := scopedStatement(t, Scope{TeamIDs: []string{"team-a"}}, 500)

	marker := strings.Index(statement, teamscope.Marker)
	groupBy := strings.Index(statement, "GROUP BY repo_id, reviewer, author, day")
	limit := strings.Index(statement, "LIMIT {limit:UInt64}")
	where := strings.Index(statement, "WHERE org_id = {org_id:String}")
	if marker < 0 {
		t.Fatalf("statement does not carry the shared team condition (%q): %s", teamscope.Marker, statement)
	}
	if !(where < marker && marker < groupBy && groupBy < limit) {
		t.Errorf("order must be WHERE < team condition < GROUP BY < LIMIT, got where=%d marker=%d groupBy=%d limit=%d", where, marker, groupBy, limit)
	}
	if !strings.Contains(statement, "toString(repo_id) IN (") {
		t.Errorf("the condition must compare the UUID column as a string: %s", statement)
	}
	values := bindingValues(bindings)
	if !reflect.DeepEqual(values[teamscope.BindingTeamIDs], []string{"team-a"}) {
		t.Errorf("team ids binding = %#v", values[teamscope.BindingTeamIDs])
	}
	if values[teamscope.BindingOrgID] != "org-1" {
		t.Errorf("the team condition must be bound to the authorized org, got %#v", values[teamscope.BindingOrgID])
	}
}

func TestResolveScoped_NeverReadsPersonMembership(t *testing.T) {
	statement, _ := scopedStatement(t, Scope{TeamIDs: []string{"team-a"}}, 500)
	for _, table := range []string{"team_members", "team_membership", "identities", "identity_aliases", "members"} {
		if strings.Contains(statement, table) {
			t.Errorf("a team scope must come from ownership only; statement reads %q: %s", table, statement)
		}
	}
	if !strings.Contains(statement, "team_repo_ownership") {
		t.Errorf("statement does not read team_repo_ownership: %s", statement)
	}
}

func TestResolveScoped_RepoIDsAndTeamIDsBothApply(t *testing.T) {
	statement, bindings := scopedStatement(t, Scope{RepoIDs: []string{"repo-a"}, TeamIDs: []string{"team-a"}}, 500)
	groupBy := strings.Index(statement, "GROUP BY")
	repoClause := strings.Index(statement, "{repo_ids:Array(String)}")
	teamClause := strings.Index(statement, teamscope.Marker)
	if repoClause < 0 || teamClause < 0 || !(repoClause < groupBy && teamClause < groupBy) {
		t.Fatalf("both filters must sit in the inner WHERE: repo=%d team=%d groupBy=%d\n%s", repoClause, teamClause, groupBy, statement)
	}
	between := statement[repoClause:teamClause]
	if !strings.Contains(between, "AND") || strings.Contains(between, " OR repo_id") {
		t.Errorf("the two filters must be ANDed (an intersection), not ORed: %q", between)
	}
	values := bindingValues(bindings)
	if !reflect.DeepEqual(values["repo_ids"], []string{"repo-a"}) {
		t.Errorf("repo_ids binding = %#v", values["repo_ids"])
	}
}

func TestResolveScoped_NoTeamScopeIsByteEqualToResolve(t *testing.T) {
	plain := &fakeClient{response: &fakeRowScanner{rows: nil}}
	if _, err := Resolve(context.Background(), plain, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), []string{"repo-a"}, 500); err != nil {
		t.Fatal(err)
	}
	for name, scope := range map[string]Scope{
		"nil team ids":    {RepoIDs: []string{"repo-a"}},
		"empty team ids":  {RepoIDs: []string{"repo-a"}, TeamIDs: []string{}},
		"blank team ids":  {RepoIDs: []string{"repo-a"}, TeamIDs: []string{"", ""}},
		"asOf alone only": {RepoIDs: []string{"repo-a"}, AsOf: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)},
	} {
		statement, bindings := scopedStatement(t, scope, 500)
		if statement != plain.statement {
			t.Errorf("%s: statement differs from Resolve's:\n got  %s\n want %s", name, statement, plain.statement)
		}
		if !reflect.DeepEqual(bindings, plain.bindings) {
			t.Errorf("%s: bindings %#v, want %#v", name, bindings, plain.bindings)
		}
		if strings.Contains(statement, teamscope.Marker) {
			t.Errorf("%s: a team condition was added without team ids", name)
		}
	}
}

func TestResolveScoped_OwnershipIsReadAtOneBoundInstant(t *testing.T) {
	asOf := time.Date(2026, 8, 15, 12, 0, 0, 0, time.FixedZone("x", 3600))
	_, bindings := scopedStatement(t, Scope{TeamIDs: []string{"team-a"}, AsOf: asOf}, 500)
	got, ok := bindingValues(bindings)[teamscope.BindingAsOf].(time.Time)
	if !ok || !got.Equal(asOf) || got.Location() != time.UTC {
		t.Errorf("as-of binding = %#v, want %v in UTC", got, asOf.UTC())
	}

	before := time.Now().UTC().Add(-time.Second)
	_, bindings = scopedStatement(t, Scope{TeamIDs: []string{"team-a"}}, 500)
	got, ok = bindingValues(bindings)[teamscope.BindingAsOf].(time.Time)
	if !ok || got.Before(before) || got.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("a zero AsOf must mean now, got %v", got)
	}
}
