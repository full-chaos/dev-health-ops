package scoperesolve

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type rowScanner struct {
	rows  []string
	index int
}

func (s *rowScanner) Next() bool { s.index++; return s.index <= len(s.rows) }
func (s *rowScanner) Scan(dest ...any) error {
	*(dest[0].(*string)) = s.rows[s.index-1]
	return nil
}
func (s *rowScanner) Err() error   { return nil }
func (s *rowScanner) Close() error { return nil }

// client answers the existence reads from the ids that exist per organization and kind, and records what it was asked.
type client struct {
	known      map[string]bool // "org/kind/id"
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *client) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	kind := "developer"
	if strings.Contains(statement, "FROM repos FINAL") {
		kind = "repo"
	}
	var org string
	var ids []string
	for _, b := range bindings {
		switch b.Name {
		case "org_id":
			org = b.Value.(string)
		case "scope_ids":
			ids = b.Value.([]string)
		}
	}
	var rows []string
	for _, id := range ids {
		if c.known[org+"/"+kind+"/"+id] {
			rows = append(rows, id)
		}
	}
	return &rowScanner{rows: rows}, nil
}

func newClient() *client {
	return &client{known: map[string]bool{
		"org-a/repo/aaaaaaaa-1111-4111-8111-111111111111": true,
		"org-a/repo/api-server":                           true,
		"org-a/developer/dev@example.com":                 true,
		"org-a/developer/identity-7":                      true,
		"org-b/repo/bbbbbbbb-2222-4222-8222-222222222222": true,
		"org-b/developer/other@example.com":               true,
	}}
}

func unresolved(t *testing.T, c *client, org, level string, ids ...string) bool {
	t.Helper()
	got, err := Unresolved(context.Background(), c, org, level, ids)
	if err != nil {
		t.Fatalf("Unresolved(%s %v): %v", level, ids, err)
	}
	return got
}

func TestUnknownIDIsUnresolvedAtEveryLevel(t *testing.T) {
	for level, id := range map[string]string{"repo": "nope", "service": "svc-1", "developer": "nobody@example.com"} {
		if !unresolved(t, newClient(), "org-a", level, id) {
			t.Errorf("%s %q: resolved, want unresolved", level, id)
		}
	}
}

func TestKnownAndMixedIDsAreResolved(t *testing.T) {
	cases := map[string]struct {
		level string
		ids   []string
	}{
		"repo uuid":           {"repo", []string{"aaaaaaaa-1111-4111-8111-111111111111"}},
		"repo uppercase uuid": {"repo", []string{"AAAAAAAA-1111-4111-8111-111111111111"}},
		"repo name":           {"repo", []string{"api-server"}},
		"repo mixed":          {"repo", []string{"nope", "api-server"}},
		"developer email":     {"developer", []string{"dev@example.com"}},
		"developer identity":  {"developer", []string{"identity-7"}},
		"developer mixed":     {"developer", []string{"nobody@example.com", "dev@example.com"}},
	}
	for name, tc := range cases {
		if unresolved(t, newClient(), "org-a", tc.level, tc.ids...) {
			t.Errorf("%s: unresolved, want resolved", name)
		}
	}
}

func TestAnIDOfAnotherOrganizationIsUnknown(t *testing.T) {
	for level, id := range map[string]string{"repo": "bbbbbbbb-2222-4222-8222-222222222222", "developer": "other@example.com"} {
		c := newClient()
		if !unresolved(t, c, "org-a", level, id) {
			t.Errorf("%s: an id of org-b is known to org-a", level)
		}
		if unresolved(t, c, "org-b", level, id) {
			t.Errorf("%s: the id of org-b is unknown to org-b", level)
		}
	}
}

func TestOneBoundedOrgScopedQueryPerLevelAndNoneWhenNothingToRead(t *testing.T) {
	for level, want := range map[string]int{"repo": 1, "developer": 1, "service": 0, "team": 0, "org": 0} {
		c := newClient()
		unresolved(t, c, "org-a", level, "a", "b", "c", "a", "")
		if len(c.statements) != want {
			t.Errorf("%s: %d queries, want %d", level, len(c.statements), want)
		}
		for i, stmt := range c.statements {
			var count uint64
			for _, b := range c.bindings[i] {
				if b.Name == "id_count" {
					count = b.Value.(uint64)
				}
			}
			if count != 3 || !strings.Contains(stmt, "LIMIT {id_count:UInt64}") {
				t.Errorf("%s: id_count %d (want 3 unique non-empty ids) or no LIMIT:\n%s", level, count, stmt)
			}
			if got, sources := strings.Count(stmt, "org_id = {org_id:String}"), strings.Count(stmt, "FINAL"); got != sources {
				t.Errorf("%s: %d org predicates for %d FINAL sources:\n%s", level, got, sources, stmt)
			}
			if strings.Contains(stmt, "day >=") || strings.Contains(stmt, "{start_day") {
				t.Errorf("%s: the existence read has a time window:\n%s", level, stmt)
			}
		}
	}
	c := newClient()
	if unresolved(t, c, "org-a", "repo") || unresolved(t, c, "org-a", "service", "", "") || len(c.statements) != 0 {
		t.Error("a request naming no id is unresolved or read something")
	}
}
