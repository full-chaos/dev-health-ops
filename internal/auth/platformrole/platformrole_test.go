package platformrole

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func lookupOf(value string, set bool) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if name == OwnedEnv {
			return value, set
		}
		return "", false
	}
}

func TestPlatformRolesOwnedFromLookup(t *testing.T) {
	for _, tc := range []struct {
		value   string
		set     bool
		on, bad bool
	}{
		{"", false, false, false},
		{"", true, false, false},
		{"false", true, false, false},
		{" TRUE ", true, true, false},
		{"true", true, true, false},
		{"yes", true, false, true},
		{"1", true, false, true},
	} {
		on, err := OwnedFromLookup(lookupOf(tc.value, tc.set))
		if on != tc.on || (err != nil) != tc.bad {
			t.Errorf("%q (set %v): on=%v err=%v, want on=%v bad=%v", tc.value, tc.set, on, err, tc.on, tc.bad)
		}
	}
}

// The switch parses exactly like AUTH_USERS_OWNED.
func TestPlatformRolesOwnedParsesLikeUsersOwned(t *testing.T) {
	for _, value := range []string{"", "false", "true", "TRUE", " true ", "False", "yes", "0", "on"} {
		on, err := OwnedFromLookup(lookupOf(value, true))
		users, uerr := authconfig.UsersOwnedFromLookup(func(name string) (string, bool) {
			return value, name == authconfig.EnvUsersOwned
		})
		if on != users || (err == nil) != (uerr == nil) {
			t.Errorf("%q: platform roles on=%v err=%v, users on=%v err=%v", value, on, err, users, uerr)
		}
	}
}

func TestIsPlatformRoleIsTheCatalogPlatformScope(t *testing.T) {
	for _, role := range policycatalog.Default().Roles() {
		if got, want := IsPlatformRole(role.ID), role.Scope == policycatalog.ScopePlatform; got != want {
			t.Errorf("IsPlatformRole(%s) = %v, want %v", role.ID, got, want)
		}
	}
	for _, role := range []string{"", "superuser", "Platform_Admin", "platform_admin "} {
		if IsPlatformRole(role) {
			t.Errorf("IsPlatformRole(%q) = true", role)
		}
	}
}

func TestOrganizationOwnerHoldsNoPlatformAction(t *testing.T) {
	for _, role := range []string{"owner", "admin", "member", "viewer"} {
		a, ignored := authorityOf([]string{role})
		if a.Any() || ignored != 1 {
			t.Errorf("stored %s: authority %v ignored %d, want none and 1", role, a.Roles(), ignored)
		}
		for _, action := range policycatalog.Default().Actions() {
			if a.Allows(action.ID) {
				t.Errorf("stored %s allows %s", role, action.ID)
			}
		}
	}
}

func TestPlatformAdminHoldsNoOrganizationAction(t *testing.T) {
	a, ignored := authorityOf([]string{PlatformAdmin})
	if !a.Any() || ignored != 0 {
		t.Fatalf("platform_admin: authority %v ignored %d", a.Roles(), ignored)
	}
	catalog := policycatalog.Default()
	role, err := catalog.Role(PlatformAdmin)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range catalog.Actions() {
		want := action.Scope == policycatalog.ScopePlatform && catalog.Allows(role.ID, action.ID)
		if got := a.Allows(action.ID); got != want {
			t.Errorf("platform_admin Allows(%s) = %v, want %v", action.ID, got, want)
		}
		if action.Scope == policycatalog.ScopeOrganization && a.Allows(action.ID) {
			t.Errorf("platform_admin allows the organization action %s", action.ID)
		}
	}
	if !a.Allows(GrantAction) {
		t.Errorf("platform_admin does not allow %s", GrantAction)
	}
}

func TestUnknownStoredRoleGrantsNothing(t *testing.T) {
	a, ignored := authorityOf([]string{"superuser", "platform.root", ""})
	if a.Any() || ignored != 3 || a.Allows(GrantAction) {
		t.Fatalf("authority %v ignored %d", a.Roles(), ignored)
	}
}

func TestZeroAuthorityHoldsNothing(t *testing.T) {
	var a Authority
	if a.Any() || len(a.Roles()) != 0 {
		t.Fatal("the zero Authority holds a role")
	}
	for _, action := range policycatalog.Default().Actions() {
		if a.Allows(action.ID) {
			t.Errorf("the zero Authority allows %s", action.ID)
		}
	}
	if a.Allows("admin.unknown") {
		t.Error("an unknown action is allowed")
	}
}

func TestGrantAndRevokeRefuseANonPlatformRoleBeforeAnyRead(t *testing.T) {
	s := &Store{}
	for _, role := range []string{"owner", "admin", "member", "viewer", "superuser", ""} {
		if _, err := s.Grant(context.Background(), GrantInput{Actor: uuid.New(), Principal: uuid.New(), Role: role}); !errors.Is(err, ErrNotPlatformRole) {
			t.Errorf("Grant %q: %v, want ErrNotPlatformRole", role, err)
		}
		if err := s.Revoke(context.Background(), RevokeInput{Actor: uuid.New(), Principal: uuid.New(), Role: role}); !errors.Is(err, ErrNotPlatformRole) {
			t.Errorf("Revoke %q: %v, want ErrNotPlatformRole", role, err)
		}
	}
}

func TestNewRefusesAnIncompleteConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("no pool accepted")
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Schema: schema}); err == nil {
		t.Error("no pool with a schema accepted")
	}
}

func TestWriteErrKeepsEachRefusalAndHidesStoreFailures(t *testing.T) {
	s := &Store{logger: discardLogger()}
	for _, refusal := range refusals {
		wrapped := errors.Join(errors.New("audit: applying the state mutation"), refusal)
		if got := s.writeErr(context.Background(), "op", wrapped); got != refusal {
			t.Errorf("writeErr(%v) = %v, want the refusal itself", refusal, got)
		}
	}
	if got := s.writeErr(context.Background(), "op", errors.New("connection reset")); !errors.Is(got, ErrStore) {
		t.Errorf("a store failure = %v, want ErrStore", got)
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// The runtime role holds no UPDATE on platform_role_assignments (a change is a
// revoke then a grant), and PostgreSQL requires UPDATE for any row-locking
// clause. So every locking clause of the store names only principal rows; the
// store serializes its writes with its advisory write lock instead.
func TestStoreLocksNoAssignmentRow(t *testing.T) {
	src, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	clauses := regexp.MustCompile(`FOR\s+(?:NO\s+KEY\s+UPDATE|KEY\s+SHARE|UPDATE|SHARE)\b(\s+OF\s+[a-z_]+(?:\s*,\s*[a-z_]+)*)?`).FindAllStringSubmatch(string(src), -1)
	if len(clauses) == 0 {
		t.Fatal("no locking clause found; the principal rows of a grant and a revoke must be locked")
	}
	for _, c := range clauses {
		if c[1] == "" {
			t.Errorf("%q names no table: say which rows it locks", c[0])
			continue
		}
		for _, name := range strings.Split(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(c[1]), "OF")), ",") {
			if name = strings.TrimSpace(name); name != "p" && name != "principals" {
				t.Errorf("%q locks %s; only principal rows may be locked", c[0], name)
			}
		}
	}
	if !strings.Contains(string(src), "pg_advisory_xact_lock(hashtextextended($1, 0))") {
		t.Error("the store takes no advisory write lock")
	}
	// The guard statements run after the lock is taken, so they read the clock
	// of their own statement: now() is the transaction start, before the wait,
	// and clock_timestamp() can change inside one statement.
	for _, clock := range []string{"now()", "clock_timestamp()", "current_timestamp", "transaction_timestamp()"} {
		if strings.Contains(strings.ToLower(string(src)), clock) {
			t.Errorf("store.go reads %s; the guards read statement_timestamp()", clock)
		}
	}
	if strings.Count(string(src), "statement_timestamp()") < 2 {
		t.Error("the active rule and the expiry check do not both read statement_timestamp()")
	}
	if writeLock != "auth.platform_role_assignments:write" {
		t.Errorf("write lock name = %q: every writer of the table must take the same lock", writeLock)
	}
}
