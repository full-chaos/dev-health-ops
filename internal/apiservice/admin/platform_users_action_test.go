package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const (
	paOrgRefusal       = "Organization context required"
	paSuperuserRefusal = "Superuser access required"
)

// paDB answers every users read with one row whose is_superuser is
// targetSuper, and records each write of the guarded transaction.
type paDB struct {
	pgDB
	targetSuper bool
	member      bool
	writes      []string
}

func (d *paDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return paRow{super: d.targetSuper, member: d.member}
}
func (d *paDB) Begin(context.Context) (pgx.Tx, error) { return &paTx{db: d}, nil }

type paTx struct {
	pgx.Tx
	db *paDB
}

func (x *paTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return paRow{super: x.db.targetSuper, member: x.db.member}
}
func (x *paTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &paRows{super: x.db.targetSuper, left: 1}, nil
}
func (x *paTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	x.db.writes = append(x.db.writes, sql)
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (x *paTx) Commit(context.Context) error   { return nil }
func (x *paTx) Rollback(context.Context) error { return nil }

type paRows struct {
	pgx.Rows
	super bool
	left  int
}

func (r *paRows) Next() bool { r.left--; return r.left >= 0 }
func (r *paRows) Scan(dest ...any) error {
	*(dest[0].(*bool)) = r.super
	return nil
}
func (r *paRows) Close()     {}
func (r *paRows) Err() error { return nil }

// paRow scans a users row in fullUser's column order, or the acting user's row (id, password_hash NULL).
type paRow struct{ super, member bool }

func (r paRow) Scan(dest ...any) error {
	if len(dest) == 1 {
		*(dest[0].(*bool)) = r.member
		return nil
	}
	if len(dest) == 2 {
		*(dest[0].(*uuid.UUID)) = uuid.New()
		return nil
	}
	values := []any{uuid.New(), "target@example.com", nil, nil, nil, nil, nil, nil, true, true, r.super, 0, nil, time.Now(), time.Now()}
	for i, d := range dest {
		if values[i] == nil {
			continue
		}
		target := reflect.ValueOf(d).Elem()
		target.Set(reflect.ValueOf(values[i]))
	}
	return nil
}

func paHandlers(db *paDB) *handlers {
	return &handlers{store: pgStore{Pool: db}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func paSet(actions ...string) *rule.Platform {
	set := rule.PlatformOf(actions...)
	return &set
}

type paCaller struct {
	name string
	user policy.User
}

func paUser(superuser bool, platform *rule.Platform, role, org string) policy.User {
	return policy.User{UserID: uuid.NewString(), ID: uuid.New(), OrgID: org, Role: role, IsSuperuser: superuser, Platform: platform}
}

type paRoute struct {
	name string
	call func(h *handlers, w http.ResponseWriter, r *http.Request)
	body string
}

var paRoutes = []paRoute{
	{"getUser", func(h *handlers, w http.ResponseWriter, r *http.Request) { h.getUser(w, r) }, ""},
	{"updateUser", func(h *handlers, w http.ResponseWriter, r *http.Request) { h.updateUser(w, r) }, `{"full_name":"New Name"}`},
	{"setUserPassword", func(h *handlers, w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), setPasswordInputKey{}, setPasswordInput{AdminPassword: "admin-password-1", Password: "a-new-Password-123!"}))
		h.setUserPassword(w, r)
	}, ""},
	{"deleteUser", func(h *handlers, w http.ResponseWriter, r *http.Request) { h.deleteUser(w, r) }, ""},
}

func paServe(t *testing.T, h *handlers, route paRoute, user policy.User) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/users/"+uuid.NewString(), strings.NewReader(route.body))
	r.SetPathValue("user_id", uuid.NewString())
	ctx := policy.WithUser(r.Context(), &user)
	if route.body != "" {
		r.Header.Set("Content-Type", "application/json")
		body, _, _, err := pybody.Read(r)
		if err != nil {
			t.Fatal(err)
		}
		ctx = policy.WithBody(ctx, body)
	}
	w := httptest.NewRecorder()
	route.call(h, w, r.WithContext(ctx))
	return w
}

// The four per-user routes admit a caller whose live row carries admin.users
// with no organization, as their route guard does: no organization is asked.
func TestAdminUsersActionHolderNeedsNoOrganization(t *testing.T) {
	for _, route := range paRoutes {
		for _, c := range []struct {
			name    string
			user    policy.User
			refused bool
		}{
			{"admin.users only, no org", paUser(false, paSet(rule.ActionAdminUsers), "member", ""), false},
			{"platform_admin set, no org", paUser(false, paSet(rule.PlatformActions...), "member", ""), false},
			{"the flag, no org", paUser(true, nil, "member", ""), false},
			{"admin.orgs only, no org: the route's action decides", paUser(false, paSet(rule.ActionAdminOrgs), "member", ""), true},
			{"an org admin with no org claim", paUser(false, nil, "admin", ""), true},
			{"an org admin with an org claim", paUser(false, nil, "admin", uuid.NewString()), false},
		} {
			t.Run(route.name+"/"+c.name, func(t *testing.T) {
				w := paServe(t, paHandlers(&paDB{}), route, c.user)
				if got := strings.Contains(w.Body.String(), paOrgRefusal); got != c.refused {
					t.Fatalf("status %d body %q: organization refusal = %v, want %v", w.Code, w.Body.String(), got, c.refused)
				}
			})
		}
	}
}

// A caller without the flag never writes a superuser target, whatever its
// platform set: no row changes, and the same caller writes a plain user.
func TestAdminUsersActionHolderCannotWriteASuperuserTarget(t *testing.T) {
	for _, route := range paRoutes[1:] {
		for _, c := range []struct {
			name    string
			user    policy.User
			super   bool
			refused bool
		}{
			{"admin.users only, a superuser target", paUser(false, paSet(rule.ActionAdminUsers), "member", ""), true, true},
			{"platform_admin set without the flag, a superuser target", paUser(false, paSet(rule.PlatformActions...), "member", ""), true, true},
			{"admin.users only, a plain target", paUser(false, paSet(rule.ActionAdminUsers), "member", ""), false, false},
			{"an org admin, a superuser target", paUser(false, nil, "admin", uuid.NewString()), true, true},
			{"the flag, a superuser target", paUser(true, nil, "member", ""), true, false},
		} {
			t.Run(route.name+"/"+c.name, func(t *testing.T) {
				db := &paDB{targetSuper: c.super, member: true}
				w := paServe(t, paHandlers(db), route, c.user)
				refused := w.Code == http.StatusForbidden && strings.Contains(w.Body.String(), paSuperuserRefusal)
				if refused != c.refused || strings.Contains(w.Body.String(), paOrgRefusal) {
					t.Fatalf("status %d body %q: superuser refusal = %v, want %v, and no organization refusal", w.Code, w.Body.String(), refused, c.refused)
				}
				if c.refused && len(db.writes) != 0 {
					t.Fatalf("a refused write ran statements: %q", db.writes)
				}
			})
		}
	}
}

// Create with is_superuser is refused for a caller without the flag.
func TestAdminUsersActionHolderCannotCreateASuperuser(t *testing.T) {
	for _, c := range []struct {
		name    string
		user    policy.User
		refused bool
	}{
		{"admin.users only", paUser(false, paSet(rule.ActionAdminUsers), "member", ""), true},
		{"the flag", paUser(true, nil, "member", ""), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db := &paDB{}
			r := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(`{"email":"new@example.com","is_superuser":true}`))
			r.Header.Set("Content-Type", "application/json")
			body, _, _, err := pybody.Read(r)
			if err != nil {
				t.Fatal(err)
			}
			user := c.user
			r = r.WithContext(policy.WithBody(policy.WithUser(r.Context(), &user), body))
			w := httptest.NewRecorder()
			paHandlers(db).createUser(w, r)
			refused := w.Code == http.StatusForbidden && strings.Contains(w.Body.String(), paSuperuserRefusal)
			if refused != c.refused {
				t.Fatalf("status %d body %q: refusal = %v, want %v", w.Code, w.Body.String(), refused, c.refused)
			}
			if c.refused && len(db.writes) != 0 {
				t.Fatalf("a refused create ran statements: %q", db.writes)
			}
		})
	}
}

// The organization-membership writes keep the flag check: a caller with
// admin.users and no flag never writes a superuser through them.
func TestMembershipWritesStillNeedTheFlagForASuperuser(t *testing.T) {
	for _, route := range []string{"add_member", "update_member_role", "remove_member", "transfer_ownership"} {
		for _, c := range []struct {
			name    string
			user    policy.User
			refused bool
		}{
			{"admin.users only", paUser(false, paSet(rule.ActionAdminUsers), "owner", uuid.NewString()), true},
			{"the flag", paUser(true, nil, "owner", uuid.NewString()), false},
		} {
			t.Run(route+"/"+c.name, func(t *testing.T) {
				h := paHandlers(&paDB{targetSuper: true})
				w := httptest.NewRecorder()
				user := c.user
				h.superuserGuardedWrite(context.Background(), w, &user, nil, route, func(g *superuserTx) func() {
					if g.refuse(uuid.New()) {
						return nil
					}
					return func() { w.WriteHeader(http.StatusOK) }
				})
				if refused := w.Code == http.StatusForbidden; refused != c.refused {
					t.Fatalf("status %d: refused = %v, want %v", w.Code, refused, c.refused)
				}
			})
		}
	}
}
