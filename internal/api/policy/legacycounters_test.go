package policy

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

type legacyRecorder struct {
	mu   sync.Mutex
	hits map[string]int
}

func recordLegacy(t *testing.T) *legacyRecorder {
	t.Helper()
	rec := &legacyRecorder{hits: map[string]int{}}
	previous := legacyPathAdd
	legacyPathAdd = func(_ context.Context, path, kind string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.hits[path+"/"+kind]++
	}
	t.Cleanup(func() { legacyPathAdd = previous })
	return rec
}

func (r *legacyRecorder) get(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits[key]
}

func (r *legacyRecorder) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.hits))
	for k := range r.hits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func bearer(t *testing.T, mutate func(jwt.MapClaims)) map[string]string {
	return map[string]string{"Authorization": "Bearer " + sign(t, claims(mutate))}
}

func TestGuardLevelCounterCountsEachLevelOnce(t *testing.T) {
	adminRole := func(c jwt.MapClaims) { c["role"] = "admin" }
	cases := []struct {
		level   Authz
		headers map[string]string
		store   *fakeStore
	}{
		{Public, nil, newStore()},
		{Optional, nil, newStore()},
		{Authenticated, bearer(t, nil), newStore()},
		{Admin, bearer(t, adminRole), newStore()},
		{AdminOrg, bearer(t, adminRole), newStore()},
		{Superuser, bearer(t, nil), func() *fakeStore {
			s := newStore()
			s.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			return s
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.level.String(), func(t *testing.T) {
			rec := recordLegacy(t)
			got := serve(NewGuard(authenticator(t, tc.store), quiet()).Wrap(tc.level, echo), http.MethodGet, "/api/v1/orgs/me", tc.headers)
			if got.status != 200 {
				t.Fatalf("status %d %s", got.status, got.body)
			}
			if n := rec.get(legacyPathGuardLevel + "/" + tc.level.String()); n != 1 {
				t.Fatalf("guard_level/%s = %d, want 1 (%v)", tc.level, n, rec.keys())
			}
		})
	}
}

func TestGuardLevelCounterSkipsRefusedCaller(t *testing.T) {
	rec := recordLegacy(t)
	got := serve(NewGuard(authenticator(t, newStore()), quiet()).Wrap(Admin, echo), http.MethodGet, "/api/v1/orgs/me", bearer(t, nil))
	if got.status != http.StatusForbidden {
		t.Fatalf("status %d", got.status)
	}
	if n := rec.get(legacyPathGuardLevel + "/admin"); n != 0 {
		t.Fatalf("guard_level/admin = %d, want 0", n)
	}
}

func TestRoleClaimCounterSplitsRoleAndSuperuser(t *testing.T) {
	super := newStore()
	super.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	cases := map[string]struct {
		store  *fakeStore
		mutate func(jwt.MapClaims)
		want   string
		status int
	}{
		"role":      {newStore(), func(c jwt.MapClaims) { c["role"] = "owner" }, roleClaimByRole, 200},
		"superuser": {super, nil, roleClaimBySuperuser, 200},
		"denied":    {newStore(), nil, roleClaimDenied, 403},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := recordLegacy(t)
			got := serve(NewGuard(authenticator(t, tc.store), quiet()).Wrap(Admin, echo), http.MethodGet, "/api/v1/orgs/me", bearer(t, tc.mutate))
			if got.status != tc.status {
				t.Fatalf("status %d, want %d", got.status, tc.status)
			}
			if n := rec.get(legacyPathRoleClaim + "/" + tc.want); n != 1 {
				t.Fatalf("role_claim/%s = %d (%v)", tc.want, n, rec.keys())
			}
		})
	}
}

func TestDirectReadCounterCountsEachStoreCall(t *testing.T) {
	rec := recordLegacy(t)
	store := countStore(newStore())
	ctx := context.Background()
	_, _, _ = store.UserState(ctx, userID)
	_, _, _ = store.Membership(ctx, userID, userID)
	_, _ = store.ActiveImpersonation(ctx, userID)
	_, _ = store.ActiveImpersonation(ctx, userID)
	want := map[string]int{"users": 1, "memberships": 1, "impersonation_sessions": 2}
	for kind, n := range want {
		if got := rec.get(legacyPathDirectRead + "/" + kind); got != n {
			t.Errorf("direct_read/%s = %d, want %d", kind, got, n)
		}
	}
}

func TestDirectReadCounterKeepsSessionStore(t *testing.T) {
	if _, ok := countStore(newStore()).(SessionStore); ok {
		t.Fatal("a Store without SessionState became a SessionStore")
	}
	if _, ok := countStore(newSessionStore()).(SessionStore); !ok {
		t.Fatal("a SessionStore lost SessionState")
	}
}

func TestLegacyCounterLabelsAreClosedSet(t *testing.T) {
	rec := recordLegacy(t)
	for _, level := range []Authz{Public, Optional, Authenticated, Admin, AdminOrg, Superuser} {
		serve(NewGuard(authenticator(t, newStore()), quiet()).Wrap(level, echo), http.MethodGet, "/x/"+"secret-path", bearer(t, nil))
	}
	allowed := map[string]bool{}
	for _, k := range []string{"public", "optional", "authenticated", "admin", "admin_org", "superuser"} {
		allowed[legacyPathGuardLevel+"/"+k] = true
	}
	for _, k := range []string{roleClaimByRole, roleClaimBySuperuser, roleClaimDenied} {
		allowed[legacyPathRoleClaim+"/"+k] = true
	}
	for _, k := range []string{directReadUsers, directReadMemberships, directReadImpersonation} {
		allowed[legacyPathDirectRead+"/"+k] = true
	}
	for _, key := range rec.keys() {
		if !allowed[key] || strings.Contains(key, "secret") || strings.Contains(key, userID.String()) {
			t.Errorf("label pair %q outside the closed set", key)
		}
	}
}

func TestCounterFailureNeverChangesTheDecision(t *testing.T) {
	adminRole := func(c jwt.MapClaims) { c["role"] = "admin" }
	run := func() []int {
		var out []int
		for _, level := range []Authz{Public, Authenticated, Admin} {
			out = append(out, serve(NewGuard(authenticator(t, newStore()), quiet()).Wrap(level, echo), http.MethodGet, "/api/v1/orgs/me", bearer(t, adminRole)).status)
		}
		out = append(out, serve(NewGuard(authenticator(t, newStore()), quiet()).Wrap(Admin, echo), http.MethodGet, "/api/v1/orgs/me", bearer(t, nil)).status)
		return out
	}
	baseline := run()
	previous := legacyPathAdd
	t.Cleanup(func() { legacyPathAdd = previous })
	for name, adder := range map[string]legacyAdder{
		"panic": func(context.Context, string, string) { panic("meter down") },
		"nil":   nil,
	} {
		legacyPathAdd = adder
		got := run()
		for i := range baseline {
			if got[i] != baseline[i] {
				t.Fatalf("%s counter changed decision %d: %d != %d", name, i, got[i], baseline[i])
			}
		}
	}
}
