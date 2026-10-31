package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

func allBut(action string) *rule.Platform {
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != action {
			rest = append(rest, a)
		}
	}
	p := rule.PlatformOf(rest...)
	return &p
}

func only(action string) *rule.Platform {
	p := rule.PlatformOf(action)
	return &p
}

type actionCase struct {
	name      string
	superuser bool
	platform  *rule.Platform
	want      bool
}

func actionCases(action string) []actionCase {
	empty := rule.PlatformOf()
	return []actionCase{
		{"flag only, as before the change", true, nil, true},
		{"only the mapped action", false, only(action), true},
		{"every other action", true, allBut(action), false},
		{"an empty set beside the flag", true, &empty, false},
		{"no flag, no set", false, nil, false},
	}
}

// platformDB answers no membership and fails every other read, so a request
// that reaches a list read answers 500 and one the guard refuses answers its
// own status.
type platformDB struct{ pgDB }

type noRow struct{}

func (noRow) Scan(dest ...any) error {
	for _, d := range dest {
		switch p := d.(type) {
		case *bool:
			*p = false
		case *string:
			return pgx.ErrNoRows
		}
	}
	return nil
}

func (platformDB) QueryRow(context.Context, string, ...any) pgx.Row { return noRow{} }
func (platformDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("reached the store")
}
func (platformDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("reached the store")
}

func newPlatformHandlers() *handlers {
	return &handlers{store: pgStore{Pool: platformDB{}}, logger: slog.New(slog.DiscardHandler)}
}

// Mix 3: ensureUserInScope. A user outside the caller's org is in scope only
// for a caller holding admin.users.
func TestPlatformAction_UserInScopeNeedsAdminUsers(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser, Platform: c.platform}
		if got := newPlatformHandlers().ensureUserInScope(context.Background(), w, user, uuid.New(), uuid.New()); got != c.want {
			t.Errorf("%s: in scope = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// Mix 3: ensureOrgAdminAccess. An org the caller is no owner or admin of
// passes only for a caller holding admin.orgs.
func TestPlatformAction_OrgAdminAccessNeedsAdminOrgs(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminOrgs) {
		w := httptest.NewRecorder()
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser, Platform: c.platform}
		if got := newPlatformHandlers().ensureOrgAdminAccess(context.Background(), w, user, uuid.New()); got != c.want {
			t.Errorf("%s: access = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// Mix 3: orgIDForNonSuperuser. A caller with no org claim is served without an
// org context only when they hold admin.orgs.
func TestPlatformAction_NoOrgContextNeedsAdminOrgs(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminOrgs) {
		w := httptest.NewRecorder()
		user := &policy.User{IsSuperuser: c.superuser, Platform: c.platform}
		if _, got := orgIDForNonSuperuser(w, user); got != c.want {
			t.Errorf("%s: served = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// Mix 8: createUserOrg. A create with no X-Org-Id and no org claim is a
// platform create only for a caller holding admin.users.
func TestPlatformAction_PlatformCreateNeedsAdminUsers(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/users", nil)
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser, Platform: c.platform}
		org, got := newPlatformHandlers().createUserOrg(context.Background(), w, r, user)
		if got != c.want || (got && org != uuid.Nil) {
			t.Errorf("%s: org %s ok %v (status %d), want ok %v", c.name, org, got, w.Code, c.want)
		}
	}
}

// Mix 8: listUsers. The all-users list is served only to a caller holding
// admin.users (the fake store answers it 500); any other caller with no org
// claim is refused 403 before the store.
func TestPlatformAction_ListAllUsersNeedsAdminUsers(t *testing.T) {
	for _, c := range actionCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/users", nil)
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser, Platform: c.platform}
		newPlatformHandlers().listUsers(w, r.WithContext(policy.WithUser(r.Context(), user)))
		want := http.StatusForbidden
		if c.want {
			want = http.StatusInternalServerError
		}
		if w.Code != want {
			t.Errorf("%s: status %d, want %d", c.name, w.Code, want)
		}
	}
}
