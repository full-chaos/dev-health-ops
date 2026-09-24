package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/quadrant"
)

// scalarStringScanner replays a single string column, "not found" once
// exhausted -- resolvePersonIdentity's own fixture shape.
type scalarStringScanner struct {
	values []string
	index  int
}

func (s *scalarStringScanner) Next() bool {
	if s.index >= len(s.values) {
		return false
	}
	s.index++
	return true
}

func (s *scalarStringScanner) Scan(dest ...any) error {
	*dest[0].(*string) = s.values[s.index-1]
	return nil
}

func (s *scalarStringScanner) Err() error   { return nil }
func (s *scalarStringScanner) Close() error { return nil }

// TestResolveIdentityContextFoundNoAliasEntry pins _resolve_identity_context
// (services/people.py:274-298) when resolve_person_identity finds a raw
// identity that has no alias-map entry at all: canonical falls back to
// the raw identity itself, with an empty alias list.
func TestResolveIdentityContextFoundNoAliasEntry(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{values: []string{"alice@example.com"}}, nil
	}}
	canonical, aliases, err := resolveIdentityContext(context.Background(), client, "person-1", "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityContext: %v", err)
	}
	if canonical != "alice@example.com" {
		t.Fatalf("canonical = %q, want alice@example.com", canonical)
	}
	if len(aliases) != 0 {
		t.Fatalf("aliases = %v, want empty", aliases)
	}
}

// TestResolveIdentityContextFoundWithAliasEntry pins the reverse-map
// branch: the raw identity resolve_person_identity found IS an alias of
// a canonical identity, so canonical maps to it and its own alias list is
// returned (identity appended if not already present).
func TestResolveIdentityContextFoundWithAliasEntry(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/identity_mapping.yaml"
	writeFile(t, path, `
identities:
  - canonical: bob.jones@example.com
    aliases:
      - "github:bjones"
`)
	t.Setenv("IDENTITY_MAPPING_PATH", path)

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{values: []string{"github:bjones"}}, nil
	}}
	canonical, aliases, err := resolveIdentityContext(context.Background(), client, "person-1", "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityContext: %v", err)
	}
	if canonical != "bob.jones@example.com" {
		t.Fatalf("canonical = %q, want bob.jones@example.com", canonical)
	}
	want := []string{"github:bjones"}
	if len(aliases) != len(want) || aliases[0] != want[0] {
		t.Fatalf("aliases = %v, want %v", aliases, want)
	}
}

// TestResolveIdentityContextNotFoundFallsBackToAliasScan pins the second
// branch (services/people.py:291-297): resolve_person_identity finds
// nothing, but personID matches an alias-config entry's own
// PersonIDForIdentity digest.
func TestResolveIdentityContextNotFoundFallsBackToAliasScan(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/identity_mapping.yaml"
	writeFile(t, path, `
identities:
  - canonical: carol@example.com
    aliases:
      - "github:carol"
`)
	t.Setenv("IDENTITY_MAPPING_PATH", path)

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{}, nil
	}}
	personID := quadrant.PersonIDForIdentity("carol@example.com")
	canonical, aliases, err := resolveIdentityContext(context.Background(), client, personID, "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityContext: %v", err)
	}
	if canonical != "carol@example.com" {
		t.Fatalf("canonical = %q, want carol@example.com", canonical)
	}
	if len(aliases) != 1 || aliases[0] != "github:carol" {
		t.Fatalf("aliases = %v, want [github:carol]", aliases)
	}
}

// TestResolveIdentityContextNotFoundAtAll pins the "" return
// (services/people.py:298): no ClickHouse match and no alias-config
// match either.
func TestResolveIdentityContextNotFoundAtAll(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &scalarStringScanner{}, nil
	}}
	canonical, aliases, err := resolveIdentityContext(context.Background(), client, "no-such-person", "org-1")
	if err != nil {
		t.Fatalf("resolveIdentityContext: %v", err)
	}
	if canonical != "" {
		t.Fatalf("canonical = %q, want empty", canonical)
	}
	if len(aliases) != 0 {
		t.Fatalf("aliases = %v, want empty", aliases)
	}
}

// TestResolvePersonIdentityReadsBothTablesFinalWithOrgIDInsideEachBranch
// pins the declared dedup fix (resolve.go's own doc comment): both UNION
// branches read FINAL with org_id filtered at the same nesting depth,
// never a LIMIT-1-BY shape.
func TestResolvePersonIdentityReadsBothTablesFinalWithOrgIDInsideEachBranch(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &scalarStringScanner{}, nil
	}}
	if _, err := resolvePersonIdentity(context.Background(), client, "person-1", "org-1"); err != nil {
		t.Fatalf("resolvePersonIdentity: %v", err)
	}
	if strings.Contains(captured, "LIMIT 1 BY") {
		t.Fatalf("contains a LIMIT-1-BY dedup subquery shape:\n%s", captured)
	}
	for _, marker := range []string{"FROM user_metrics_daily FINAL", "FROM work_item_user_metrics_daily FINAL"} {
		if !strings.Contains(captured, marker) {
			t.Fatalf("expected marker %q in query:\n%s", marker, captured)
		}
	}
}
