//go:build integration

package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// `dho admin users delete` on a real PostgreSQL with the full migration chain, driven through the command tree the binary
// runs (so a missing verb is a failing exit, not a compile error): the guard, the delete, what goes with the user, what stays.

func executeAdmin(t *testing.T, db *database, args ...string) (int, string, string) {
	t.Helper()
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: append([]string{"admin"}, args...), Lookup: lookup, Stdout: &stdout, Stderr: &stderr,
	})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String(), stderr.String()
}

func (db *database) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestUsersDeleteGuardsThenDeletesAUserAndWhatBelongsToIt(t *testing.T) {
	db := startDatabase(t)
	// the migrated database is not empty: every count below is relative to what the migrations seed
	users := func() int { return db.count(t, `SELECT count(*) FROM users`) }
	memberships := func() int { return db.count(t, `SELECT count(*) FROM memberships`) }
	tokens := func() int { return db.count(t, `SELECT count(*) FROM refresh_tokens`) }
	orgs := func() int { return db.count(t, `SELECT count(*) FROM organizations`) }
	seedUsers, seedMemberships, seedTokens, seedOrgs := users(), memberships(), tokens(), orgs()
	for _, args := range [][]string{
		{"users", "create", "--email", "keep@example.com", "--password", "password1", "--username", "keep"},
		{"users", "create", "--email", "throwaway@example.com", "--password", "password1", "--username", "throwaway", "--superuser"},
		{"users", "create", "--email", "second@example.com", "--password", "password1", "--username", "second"},
		{"users", "create", "--email", "third@example.com", "--password", "password1", "--username", "third"},
		{"orgs", "create", "--name", "Acme", "--owner-email", "throwaway@example.com"},
		{"users", "update", "--email", "keep@example.com", "--org", "acme", "--role", "member"},
		{"users", "update", "--email", "throwaway@example.com", "--no-superuser", "--no-active"},
	} {
		if code, out := goVerb(t, db, args); code != 0 {
			t.Fatalf("setup %v: exit %d %s", args, code, out)
		}
	}
	db.seedRefreshToken(t, "throwaway@example.com")
	if users() != seedUsers+4 || memberships() != seedMemberships+2 || tokens() != seedTokens+1 {
		t.Fatalf("the setup left %d users, %d memberships, %d refresh tokens on a seed of %d, %d, %d; want +4, +2, +1",
			users(), memberships(), tokens(), seedUsers, seedMemberships, seedTokens)
	}

	// the guard: no --yes reports and deletes nothing, and says so with a failing exit
	code, out, _ := executeAdmin(t, db, "users", "delete", "--email", "throwaway@example.com")
	if code != 1 || !strings.Contains(out, "Would delete user: throwaway@example.com") || !strings.Contains(out, "memberships: 1") || !strings.Contains(out, "Nothing was deleted") {
		t.Fatalf("without --yes: exit %d, stdout %q", code, out)
	}
	if users() != seedUsers+4 || memberships() != seedMemberships+2 {
		t.Fatal("a delete without --yes changed the database")
	}

	// the delete: by email, case-insensitively; the user, its membership and its refresh token go, the others stay
	code, out, logs := executeAdmin(t, db, "users", "delete", "--email", "THROWAWAY@example.com", "--yes")
	if code != 0 || !strings.HasPrefix(out, "Deleted user: throwaway@example.com (id: ") {
		t.Fatalf("delete by email: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if users() != seedUsers+3 || db.count(t, `SELECT count(*) FROM users WHERE email = 'throwaway@example.com'`) != 0 {
		t.Fatal("the user is still there")
	}
	if memberships() != seedMemberships+1 || tokens() != seedTokens {
		t.Fatal("what belongs to the user was not removed with it")
	}
	if db.count(t, `SELECT count(*) FROM users WHERE email = 'keep@example.com'`) != 1 || orgs() != seedOrgs+1 {
		t.Fatal("a delete took something that is not the user's")
	}
	// the log line names the user by id, the facts of the delete, and carries no email, password or DSN
	if !strings.Contains(logs, "admin: user deleted by the operator CLI") || !strings.Contains(logs, "user_id=") ||
		!strings.Contains(logs, "memberships_removed=1") || strings.Contains(logs, "@example.com") || strings.Contains(logs, "password") {
		t.Fatalf("the log line is %q", logs)
	}

	// by id and by username
	var id string
	if err := db.conn.QueryRow(context.Background(), `SELECT id::text FROM users WHERE email = 'second@example.com'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if code, out, logs := executeAdmin(t, db, "users", "delete", "--id", id, "--yes"); code != 0 || !strings.HasPrefix(out, "Deleted user: second@example.com") {
		t.Fatalf("delete by id: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if code, out, logs := executeAdmin(t, db, "users", "delete", "--username", "third", "--yes"); code != 0 || !strings.HasPrefix(out, "Deleted user: third@example.com") {
		t.Fatalf("delete by username: exit %d, stdout %q, stderr %q", code, out, logs)
	}
	if users() != seedUsers+1 {
		t.Fatalf("%d users left, want %d", users(), seedUsers+1)
	}

	// refusals: an unknown user (and the same user again), no identifier, a malformed id; each deletes nothing
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--email", "throwaway@example.com", "--yes"}, "Error: User not found."},
		{[]string{"--yes"}, "Error: Identify the user with --id, --email, or --username."},
		{[]string{"--id", "not-a-uuid", "--yes"}, "Error: badly formed hexadecimal UUID string"},
	} {
		code, out, _ := executeAdmin(t, db, append([]string{"users", "delete"}, tc.args...)...)
		if code != 1 || !strings.Contains(out, tc.want) {
			t.Errorf("%v: exit %d, stdout %q, want %q", tc.args, code, out, tc.want)
		}
	}
	if users() != seedUsers+1 {
		t.Fatal("a refused delete changed the database")
	}
}
