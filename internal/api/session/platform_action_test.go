package session

import (
	"context"
	"errors"
	"reflect"
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

func without(list []string, drop ...string) []string {
	var out []string
	for _, p := range list {
		keep := true
		for _, d := range drop {
			keep = keep && p != d
		}
		if keep {
			out = append(out, p)
		}
	}
	return out
}

// Mix 2: the permission list sent to web. A platform principal gets the
// organization permissions and admin:orgs by admin.orgs and admin:users by
// admin.users, on top of the role's own; the flag alone is platform_admin, so
// its list is the whole list as before.
func TestPlatformAction_PermissionListFollowsTheActions(t *testing.T) {
	viewer := rolePermissions("viewer")
	cases := []struct {
		name string
		user *policy.User
		want []string
	}{
		{"flag only, as before the change", &policy.User{Role: "viewer", IsSuperuser: true}, standardPermissions},
		{"every action", &policy.User{Role: "viewer", IsSuperuser: true, Platform: allBut("")}, standardPermissions},
		{"admin.orgs alone", &policy.User{Role: "viewer", IsSuperuser: true, Platform: only(rule.ActionAdminOrgs)}, without(standardPermissions, "admin:users")},
		{"every action but admin.orgs", &policy.User{Role: "viewer", IsSuperuser: true, Platform: allBut(rule.ActionAdminOrgs)}, append(append([]string{}, viewer...), "admin:users")},
		{"admin.users alone", &policy.User{Role: "viewer", IsSuperuser: true, Platform: only(rule.ActionAdminUsers)}, append(append([]string{}, viewer...), "admin:users")},
		{"no flag, no set", &policy.User{Role: "viewer"}, viewer},
	}
	for _, c := range cases {
		got := userPermissions(nil, c.user)
		if !reflect.DeepEqual(got, orderedPermissions(c.want)) {
			t.Errorf("%s: %d permissions %v, want %d %v", c.name, len(got), got, len(c.want), c.want)
		}
	}
}

func orderedPermissions(held []string) []string {
	set := map[string]bool{}
	for _, p := range held {
		set[p] = true
	}
	return inOrder(set)
}

// fakeRows is one result set; Scan assigns each non-nil value to its
// destination of the same type.
type fakeRows struct {
	pgx.Rows
	rows [][]any
	at   int
}

func (f *fakeRows) Next() bool { f.at++; return f.at <= len(f.rows) }
func (f *fakeRows) Close()     {}
func (f *fakeRows) Err() error { return nil }
func (f *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		if v := f.rows[f.at-1][i]; v != nil {
			reflect.ValueOf(d).Elem().Set(reflect.ValueOf(v))
		}
	}
	return nil
}

// fakeTx answers the user read with one row, every membership read with none
// and stops at the first write: reaching Exec means the refresh was refused
// as a non-member and the family revoked.
type fakeTx struct {
	pgx.Tx
	user  []any
	execs int
}

func (f *fakeTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if len(sql) > 0 && containsFrom(sql, "FROM users") {
		return &fakeRows{rows: [][]any{f.user}}, nil
	}
	return &fakeRows{}, nil
}

func (f *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.execs++
	return pgconn.CommandTag{}, errors.New("stop at the first write")
}

func containsFrom(sql, part string) bool {
	for i := 0; i+len(part) <= len(sql); i++ {
		if sql[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

func userValues(id uuid.UUID, superuser bool) []any {
	return []any{id, "u@example.com", nil, nil, nil, nil, nil, true, true, superuser, int64(0)}
}
