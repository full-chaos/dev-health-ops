package workitemmetrics

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

// CHAOS-10797: the scope view counts each work item once per work scope. The
// dedupe key is (provider, work_scope_id, id): the newest row of the scope, on a
// tie the lower repository id; no other scope competes. The org view keeps the
// item once (OncePerProviderAndID).

var scopeViewDay = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)

type scopeViewVersion struct {
	id, repo, scope string
	synced          time.Time
}

// scopeViewFixture: "moved" is stored under repository a in scope old (older)
// and under b in scope new (newer); "same-newest" twice in scope kept (c older,
// d newer); "same-tie" twice in scope kept at one last_synced (d, then c).
func scopeViewFixture() []scopeViewVersion {
	older := scopeViewDay.Add(-2 * time.Hour)
	newer := scopeViewDay.Add(-time.Hour)
	return []scopeViewVersion{
		{"moved", "a", "old", older},
		{"moved", "b", "new", newer},
		{"same-newest", "c", "kept", older},
		{"same-newest", "d", "kept", newer},
		{"same-tie", "d", "kept", newer},
		{"same-tie", "c", "kept", newer},
	}
}

// keptAt is the kept rows as "id@scope/repo", sorted.
func keptAt(versions []scopeViewVersion, kept []int) []string {
	out := make([]string, 0, len(kept))
	for _, index := range kept {
		v := versions[index]
		out = append(out, v.id+"@"+v.scope+"/"+v.repo)
	}
	sort.Strings(out)
	return out
}

func TestOncePerProviderScopeAndIDKeepsAnItemOncePerScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		scopes map[string]bool // nil = every scope (the full run)
		scope  []string
		org    []string
	}{
		{name: "listed {old}", scopes: map[string]bool{"old": true},
			scope: []string{"moved@old/a"}, org: []string{"moved@old/a"}},
		{name: "listed {new}", scopes: map[string]bool{"new": true},
			scope: []string{"moved@new/b"}, org: []string{"moved@new/b"}},
		{name: "listed {kept}: same scope twice, newest, tie lower repository id", scopes: map[string]bool{"kept": true},
			scope: []string{"same-newest@kept/d", "same-tie@kept/c"}, org: []string{"same-newest@kept/d", "same-tie@kept/c"}},
		{name: "full run",
			scope: []string{"moved@new/b", "moved@old/a", "same-newest@kept/d", "same-tie@kept/c"},
			org:   []string{"moved@new/b", "same-newest@kept/d", "same-tie@kept/c"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var versions []scopeViewVersion
			for _, v := range scopeViewFixture() {
				if test.scopes == nil || test.scopes[v.scope] {
					versions = append(versions, v)
				}
			}
			items := make([]ItemVersion, len(versions))
			scopes := make([]string, len(versions))
			for index, v := range versions {
				items[index] = ItemVersion{Provider: "github", WorkItemID: v.id, RepoID: v.repo, LastSynced: v.synced}
				scopes[index] = v.scope
			}
			if got := keptAt(versions, scopeViewRule(items, scopes)); !reflect.DeepEqual(got, test.scope) {
				t.Errorf("scope view kept %v, want %v", got, test.scope)
			}
			orgKept, _ := OncePerProviderAndID(items)
			if got := keptAt(versions, orgKept); !reflect.DeepEqual(got, test.org) {
				t.Errorf("org rule kept %v, want %v", got, test.org)
			}
		})
	}
}

// One item in two scopes, each row with its own attribution: the scope set
// resolves each row by (scope, id), so neither scope takes the other's team.
func TestStoredItemResolverKeysTheScopeSetByScopeAndID(t *testing.T) {
	created := scopeViewDay.Add(time.Hour)
	rows := []StoredItem{
		inScopeSet(StoredItem{WorkItemID: "moved", Provider: "github", Status: "todo", ProjectID: "old", CreatedAt: created}, "old"),
		inScopeSet(StoredItem{WorkItemID: "moved", Provider: "github", Status: "todo", ProjectID: "new", CreatedAt: created}, "new"),
	}
	attributions := map[string]PrimaryAttribution{
		scopeAttributionKey("old", "moved"): {TeamID: "team-old", TeamName: "Old"},
		scopeAttributionKey("new", "moved"): {TeamID: "team-new", TeamName: "New"},
	}
	got := map[string]int{}
	for _, row := range ComputeStoredItemsDay(scopeViewDay, rows, nil, attributions).MetricsDaily {
		got[row.WorkScopeID+"/"+row.TeamID] += row.NewItemsCount
	}
	want := map[string]int{"old/team-old": 1, "new/team-new": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("new items by scope/team = %v, want %v", got, want)
	}

	// The org set keeps the key by id alone.
	orgRow := StoredItem{WorkItemID: "moved", Provider: "github", Status: "todo", ProjectID: "new", CreatedAt: created}
	org := ComputeStoredItemsDay(scopeViewDay, []StoredItem{orgRow}, nil, map[string]PrimaryAttribution{
		"moved": {TeamID: "team-new", TeamName: "New"},
	}).MetricsDaily
	if len(org) != 1 || org[0].TeamID != "team-new" {
		t.Fatalf("org set rows = %+v, want one row of team-new", org)
	}
}
