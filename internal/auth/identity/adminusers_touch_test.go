package identity

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

const touchStatement = "UPDATE public.users SET updated_at = now() WHERE id = $1"

var publicUsersWrite = regexp.MustCompile(`(?i)\b(UPDATE|INSERT\s+INTO|DELETE\s+FROM|TRUNCATE)\s+(public\.)?users\b`)

func funcBody(t *testing.T, path, signature string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	start := strings.Index(src, signature)
	if start < 0 {
		t.Fatalf("%s has no %s", path, signature)
	}
	body := src[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	return body
}

// Every admin PATCH takes one lock order in AdminUpdateUser's apply: the actor rule (the platform role write lock),
// the auth principal, the public.users row, then the write (the touch when no field changes, else the field
// statements) and last the platform role step, whose own principal locks come after the public.users row. The route
// has no other path: it never calls the platform role store's own Grant or Revoke for an update.
func TestAdminUpdateTakesOneLockOrder(t *testing.T) {
	body := funcBody(t, "adminusers.go", "func (s *Store) AdminUpdateUser(")
	steps := []string{
		"req.Authorize(ctx, tx)",
		"s.lockUser(ctx, tx, req.Principal)",
		"SELECT id FROM public.users WHERE id = $1 FOR UPDATE",
		"if decided.Empty() {",
		"touchPublicUser(ctx, tx, req.Principal)",
		"applyAdminUpdate(ctx, tx,",
		"req.Superuser(ctx, tx)",
	}
	at := -1
	for _, step := range steps {
		i := strings.Index(body, step)
		if i < 0 {
			t.Fatalf("AdminUpdateUser has no %q", step)
		}
		if i < at {
			t.Errorf("AdminUpdateUser: %q comes before the step ahead of it; want the order %q", step, steps)
		}
		at = i
	}
	guard := regexp.MustCompile(`if decided\.Empty\(\) \{\s+if applied, err = touchPublicUser\(ctx, tx, req\.Principal\); err != nil \{\s+return err\s+\}\s+\} else if applied, err = applyAdminUpdate\(`)
	if !guard.MatchString(body) {
		t.Error("AdminUpdateUser: the touch must run only on an empty field decision, and the field statements only on a non-empty one")
	}
	route := funcBody(t, filepath.Join("..", "authapi", "provisioning_users.go"), "func (h provisioningHandlers) usersUpdate(")
	for _, direct := range []string{"PlatformRoles.Grant(", "PlatformRoles.Revoke("} {
		if strings.Contains(route, direct) {
			t.Errorf("usersUpdate calls %s: a platform role change goes through AdminUpdateUser's apply, in its lock order", direct)
		}
	}
	if n := strings.Count(route, "h.UsersAdmin.AdminUpdateUser("); n != 1 {
		t.Errorf("usersUpdate calls AdminUpdateUser %d times, want once (every PATCH, no-op and role-only included)", n)
	}
}

// The touch is the one statement that bumps public.users.updated_at alone, in the one func; the platform role
// store writes no public.users row, so a role change has no other updated_at writer.
func TestTheTouchIsTheOnlyUpdatedAtWriterOfARoleChange(t *testing.T) {
	raw, err := os.ReadFile("adminusers.go")
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), touchStatement); n != 1 {
		t.Errorf("adminusers.go has %d touch statements, want 1", n)
	}
	if body := funcBody(t, "adminusers.go", "func touchPublicUser("); !strings.Contains(body, touchStatement) {
		t.Error("the touch statement is not in touchPublicUser")
	}
	files, err := filepath.Glob(filepath.Join("..", "platformrole", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if publicUsersWrite.Match(src) {
			t.Errorf("%s writes public.users: the platform role store writes no public.users row", f)
		}
	}
}

// A PATCH with no field change bumps updated_at: with a platform role change it is one Commit off the stream whose
// own row is auth.user.touched (never auth.user.updated with an empty changed_fields); alone it is one
// CommitPublicProjection with auth.user.touched.
func TestAdminUpdateWithNoFieldChangeTakesTheTouch(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	cur := AdminUpdateCurrent{Email: "same@example.test"}
	for _, c := range []struct {
		name string
		req  AdminUpdateRequest
		want commitPick
	}{
		{"platform role only", AdminUpdateRequest{Superuser: none},
			commitPick{kind: "commit", eventType: "auth.user.touched", offStream: audit.OffStreamPublicProfile}},
		{"no change", AdminUpdateRequest{}, commitPick{kind: "public_projection", eventType: "auth.user.touched"}},
	} {
		picks := recordCommits(t)
		c.req.Principal, c.req.Actor, c.req.Authorize = uuid.New(), uuid.New(), none
		if err := store.AdminUpdateUser(context.Background(), c.req, cur); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(*picks) != 1 || (*picks)[0] != c.want {
			t.Errorf("%s: commits %+v, want one %+v", c.name, *picks, c.want)
		}
	}
}
