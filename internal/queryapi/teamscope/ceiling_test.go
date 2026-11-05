package teamscope

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

const ceilingTestOrg = "0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10"

// matcherClient answers the one matcher statement with the (path, ids) rows a ClickHouse would, and records every
// statement it gets.
type matcherClient struct {
	rows       map[string][]string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *matcherClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	var keys []string
	for _, b := range bindings {
		if b.Name == BindingCeilingKeys {
			keys, _ = b.Value.([]string)
		}
	}
	out := &matcherRows{}
	for _, key := range keys {
		if ids, ok := c.rows[key]; ok {
			out.paths = append(out.paths, key)
			out.ids = append(out.ids, ids)
		}
	}
	return out, nil
}

type matcherRows struct {
	paths []string
	ids   [][]string
	i     int
}

func (r *matcherRows) Next() bool { r.i++; return r.i <= len(r.paths) }
func (r *matcherRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.paths[r.i-1]
	*(dest[1].(*[]string)) = r.ids[r.i-1]
	return nil
}
func (r *matcherRows) Err() error   { return nil }
func (r *matcherRows) Close() error { return nil }

func resolve(t *testing.T, rows map[string][]string, keys ...string) (Ceiling, *matcherClient) {
	t.Helper()
	client := &matcherClient{rows: rows}
	ceiling, err := ResolveCeiling(context.Background(), client, pepgate.Allowlist(ceilingTestOrg, keys))
	if err != nil {
		t.Fatalf("ResolveCeiling: %v", err)
	}
	return ceiling, client
}

func sortedIDs(c Ceiling) []string {
	ids := c.IDs()
	sort.Strings(ids)
	return ids
}

func TestCeilingUnrestrictedScopesRunNoStatement(t *testing.T) {
	for name, scope := range map[string]pepgate.Scope{
		"legacy": pepgate.Legacy(ceilingTestOrg), "organization": pepgate.Organization(ceilingTestOrg),
	} {
		client := &matcherClient{}
		ceiling, err := ResolveCeiling(context.Background(), client, scope)
		if err != nil || ceiling.Restricted() || len(client.statements) != 0 {
			t.Fatalf("%s: restricted=%t statements=%d err=%v", name, ceiling.Restricted(), len(client.statements), err)
		}
	}
}

func TestCeilingEmptyAllowlistAndNoScopeAreRestrictedWithNoIDsAndNoStatement(t *testing.T) {
	for name, scope := range map[string]pepgate.Scope{"empty allowlist": pepgate.Allowlist(ceilingTestOrg, nil), "no scope": {}} {
		client := &matcherClient{}
		ceiling, err := ResolveCeiling(context.Background(), client, scope)
		if err != nil || !ceiling.Restricted() || len(ceiling.IDs()) != 0 || len(client.statements) != 0 {
			t.Fatalf("%s: restricted=%t ids=%v statements=%d err=%v", name, ceiling.Restricted(), ceiling.IDs(), len(client.statements), err)
		}
	}
}

func TestMatcher_OneStatementOrgScopedAndParameterised(t *testing.T) {
	ceiling, client := resolve(t, map[string][]string{"acme/a": {"id-a"}, "acme/b": {"id-b"}}, "Acme/B", "acme/a", "acme/a")
	if len(client.statements) != 1 {
		t.Fatalf("%d statements, want 1", len(client.statements))
	}
	statement := client.statements[0]
	if !strings.HasPrefix(strings.TrimSpace(statement), "SELECT") {
		t.Fatalf("the statement must start with SELECT:\n%s", statement)
	}
	for _, want := range []string{"{org_id:String}", "{ceiling_keys:Array(String)}", "identity_path", "lower(repo)"} {
		if !strings.Contains(statement, want) {
			t.Fatalf("statement lacks %q:\n%s", want, statement)
		}
	}
	for _, key := range []string{"acme/a", "acme/b", ceilingTestOrg} {
		if strings.Contains(strings.ToLower(statement), key) {
			t.Fatalf("a value is built into the statement text: %q", key)
		}
	}
	got := map[string]any{}
	for _, b := range client.bindings[0] {
		got[b.Name] = b.Value
	}
	if got["org_id"] != ceilingTestOrg || !reflect.DeepEqual(got[BindingCeilingKeys], []string{"acme/a", "acme/b"}) || len(got) != 2 {
		t.Fatalf("bindings = %v, want org_id and the sorted, lower-cased, de-duplicated keys", got)
	}
	if !ceiling.Restricted() || !reflect.DeepEqual(sortedIDs(ceiling), []string{"id-a", "id-b"}) {
		t.Fatalf("ids = %v", ceiling.IDs())
	}
}

func TestMatcher_KeyWithNoRepo_MatchesNothing(t *testing.T) {
	ceiling, _ := resolve(t, map[string][]string{}, "acme/missing")
	if !ceiling.Restricted() || len(ceiling.IDs()) != 0 {
		t.Fatalf("restricted=%t ids=%v, want restricted with no id", ceiling.Restricted(), ceiling.IDs())
	}
}

// Q2: a key that resolves to two repository ids (the same path on two providers, or a name taken by a second repository
// after a rename) matches nothing; the key that resolves to one id still matches.
func TestMatcher_CrossProvider_MatchesNothing(t *testing.T) {
	ceiling, _ := resolve(t, map[string][]string{"a/x": {"gh-id", "gl-id"}, "a/y": {"only-id"}}, "a/x", "a/y")
	if got := sortedIDs(ceiling); !reflect.DeepEqual(got, []string{"only-id"}) {
		t.Fatalf("ids = %v, want only the unambiguous key's id", got)
	}
}

func TestMatcher_RenameThenNameReuse_MatchesNothing(t *testing.T) {
	ceiling, _ := resolve(t, map[string][]string{"a/x": {"renamed-id", "new-id"}}, "a/x")
	if !ceiling.Restricted() || len(ceiling.IDs()) != 0 {
		t.Fatalf("ids = %v, want none", ceiling.IDs())
	}
}

// A repository renamed a/x -> a/y resolves under both its old and its new key to ONE id; the id is held once.
func TestMatcher_Rename_OldAndNewKeyFollowTheSameID(t *testing.T) {
	ceiling, _ := resolve(t, map[string][]string{"a/x": {"id-1"}, "a/y": {"id-1"}}, "a/x", "a/y")
	if got := ceiling.IDs(); !reflect.DeepEqual(got, []string{"id-1"}) {
		t.Fatalf("ids = %v, want [id-1] once", got)
	}
}

func TestRepoTerm(t *testing.T) {
	if sql, b := NoCeiling().RepoTerm("repo_id"); sql != "" || b != nil {
		t.Fatalf("no ceiling: %q %v", sql, b)
	}
	none, _ := resolve(t, nil)
	if sql, b := none.RepoTerm("repo_id"); sql != " AND 1 = 0" || b != nil {
		t.Fatalf("no ids: %q %v", sql, b)
	}
	some, _ := resolve(t, map[string][]string{"a/x": {"id-1"}}, "a/x")
	sql, b := some.RepoTerm("pr.repo_id")
	if sql != " AND pr.repo_id IN {ceiling_ids:Array(String)}" || len(b) != 1 || b[0].Name != BindingCeilingIDs || !reflect.DeepEqual(b[0].Value, []string{"id-1"}) {
		t.Fatalf("with ids: %q %v", sql, b)
	}
}
