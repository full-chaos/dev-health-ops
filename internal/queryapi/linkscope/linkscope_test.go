package linkscope

import (
	"context"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

const (
	testOrg   = "org-13085"
	testRepoA = "11111111-1111-1111-1111-111111111111"
	testRepoB = "22222222-2222-2222-2222-222222222222"
)

type keyRows struct {
	paths []string
	ids   [][]string
	i     int
}

func (r *keyRows) Next() bool { r.i++; return r.i <= len(r.paths) }
func (r *keyRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.paths[r.i-1]
	*(dest[1].(*[]string)) = r.ids[r.i-1]
	return nil
}
func (r *keyRows) Err() error   { return nil }
func (r *keyRows) Close() error { return nil }

type keyQuerier map[string][]string

func (q keyQuerier) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	out := &keyRows{}
	for _, b := range bindings {
		if b.Name != teamscope.BindingCeilingKeys {
			continue
		}
		for _, key := range b.Value.([]string) {
			out.paths, out.ids = append(out.paths, key), append(out.ids, q[key])
		}
	}
	return out, nil
}

func ceilingOf(t *testing.T, scope pepgate.Scope, matcher map[string][]string) teamscope.Ceiling {
	t.Helper()
	ceiling, err := teamscope.ResolveCeiling(context.Background(), keyQuerier(matcher), scope)
	if err != nil {
		t.Fatal(err)
	}
	return ceiling
}

func TestItemConditionOfAnUnrestrictedCeilingIsEmpty(t *testing.T) {
	for name, ceiling := range map[string]teamscope.Ceiling{
		"zero":         {},
		"no ceiling":   teamscope.NoCeiling(),
		"organization": ceilingOf(t, pepgate.Organization(testOrg), nil),
		"legacy":       ceilingOf(t, pepgate.Legacy(testOrg), nil),
	} {
		if sql, bindings := ItemCondition(ceiling, "wct.work_item_id"); sql != "" || bindings != nil {
			t.Fatalf("%s: ItemCondition = %q, %v, want empty (the statement stays today's)", name, sql, bindings)
		}
	}
}

func TestItemConditionOfARestrictedCeilingWithNoRepoReadsNothing(t *testing.T) {
	for name, scope := range map[string]pepgate.Scope{
		"empty allowlist": pepgate.Allowlist(testOrg, nil),
		"key of no repo":  pepgate.Allowlist(testOrg, []string{"acme/none"}),
	} {
		ceiling := ceilingOf(t, scope, map[string][]string{})
		if !ceiling.Restricted() {
			t.Fatalf("%s: ceiling is not restricted", name)
		}
		if sql, bindings := ItemCondition(ceiling, "wct.work_item_id"); sql != " AND 1 = 0" || bindings != nil {
			t.Fatalf("%s: ItemCondition = %q, %v, want AND 1 = 0 and no binding", name, sql, bindings)
		}
	}
}

func TestItemConditionBindsTheCeilingIdsToTheLinkSubquery(t *testing.T) {
	ceiling := ceilingOf(t, pepgate.Allowlist(testOrg, []string{"acme/a", "acme/b"}),
		map[string][]string{"acme/a": {testRepoA}, "acme/b": {testRepoB}})
	sql, bindings := ItemCondition(ceiling, "wct.work_item_id")
	if want := " AND wct.work_item_id IN " + linkeditems.Subquery; sql != want {
		t.Fatalf("ItemCondition sql = %q, want %q", sql, want)
	}
	if len(bindings) != 1 || bindings[0].Name != "link_repo_ids" || !reflect.DeepEqual(bindings[0].Value, []string{testRepoA, testRepoB}) {
		t.Fatalf("ItemCondition bindings = %+v, want link_repo_ids = [A B]", bindings)
	}
	for _, banned := range []string{"provenance", "work_items", "repo_id =", "JOIN"} {
		if strings.Contains(sql, banned) {
			t.Fatalf("the condition contains %q: all three tiers count, the link is the only key, IN never JOIN:\n%s", banned, sql)
		}
	}
}
