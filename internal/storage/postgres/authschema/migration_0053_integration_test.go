//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMigration0053GrantRepositoryKeyNested: a repository grant key is "*" or a
// lowercase path of 2-20 segments in at most 255 bytes, and the constraint is
// validated.
func TestMigration0053GrantRepositoryKeyNested(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	m20Seed(t, ctx, env)
	exec := func(sql string) error { return guardedExec(ctx, env.runtime, sql) }
	deep := func(n int) string { return strings.TrimSuffix(strings.Repeat("a/", n), "/") }
	seg := strings.Repeat("a", 100)

	for _, key := range []string{"*", "acme/widgets", "acme/.github", "grp/sub/web", "a/b/c/d/e", deep(20), seg + "/" + seg + "/" + strings.Repeat("a", 53)} {
		if err := exec(m20GrantHead + `'repository', '` + key + `')`); err != nil {
			t.Fatalf("repository grant %q: %v", key, err)
		}
	}
	for _, key := range []string{"Acme/widgets", "acme", "a/", "/a", "a//b", "a/b/", "a./b", "a/b-/c", "a/b/c.", "a/b/c_", "a/.b/c", "a/b/..", "**", "a/b*", deep(21), seg + "/" + seg + "/" + strings.Repeat("a", 54)} {
		m10Fail(t, exec(m20GrantHead+`'repository', '`+key+`')`), "repository grant key "+key)
	}
	var validated bool
	if err := env.admin.QueryRow(ctx, `SELECT convalidated FROM pg_constraint WHERE conname = 'resource_grants_repository_key_canonical' AND connamespace = 'auth'::regnamespace`).Scan(&validated); err != nil || !validated {
		t.Fatalf("resource_grants_repository_key_canonical validated = %v (%v)", validated, err)
	}
}
