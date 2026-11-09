package workflows_test

import (
	"strings"
	"testing"
)

// CHAOS-13107 (goapi-leg-local-2): compose.yml has auth-service own users (AUTH_USERS_OWNED defaults true), and its
// users_owned readiness check refuses while public.users holds a user the auth schema does not: a fresh install has
// one, the migrations' baseline service placeholder (go-api-prove@service.dev-health.invalid). So the users are
// copied before auth-service serves: the auth-users-backfill one-shot, the venues' seed tool, runs
// `dho auth users-backfill -insert-only` with users owned (CHAOS-13356: -insert-only is the one form the verb runs while
// users are owned), on auth-service's own login and schema, after the auth schema and its grants, and auth-service waits
// for it.
func TestComposeCopiesTheUsersBeforeAuthServiceServes(t *testing.T) {
	c := readComposeFile(t, "compose.yml")
	backfill, ok := c.Services["auth-users-backfill"]
	if !ok {
		t.Fatal("compose.yml has no service auth-users-backfill: auth-service owns users and never becomes ready on a fresh install")
	}
	// acp ruling on #5795: insert-only. auth-service owns users, so an existing auth user is never set back to a
	// stale public.users value; only the missing ones are created.
	if got := strings.Join(backfill.Command, " "); got != "auth users-backfill -insert-only" {
		t.Errorf("auth-users-backfill command = %q, want \"auth users-backfill -insert-only\"", got)
	}
	if owned, _ := envString(t, backfill, "AUTH_USERS_OWNED"); owned != "${AUTH_USERS_OWNED:-true}" {
		t.Errorf("auth-users-backfill AUTH_USERS_OWNED = %q, want the default-on pass-through ${AUTH_USERS_OWNED:-true}: the seed runs with users owned, as auth-service does", owned)
	}
	authService := c.Services["auth-service"]
	for _, name := range []string{"AUTH_DATABASE_URI", "AUTH_DATABASE_SCHEMA"} {
		got, _ := envString(t, backfill, name)
		want, _ := envString(t, authService, name)
		if got == "" || got != want {
			t.Errorf("auth-users-backfill %s = %q, want auth-service's %q", name, got, want)
		}
	}
	for _, dep := range []string{"auth-migrate", "auth-grants"} {
		if got := backfill.DependsOn[dep].Condition; got != "service_completed_successfully" {
			t.Errorf("auth-users-backfill depends_on %s = %q, want service_completed_successfully", dep, got)
		}
	}
	if got := authService.DependsOn["auth-users-backfill"].Condition; got != "service_completed_successfully" {
		t.Errorf("auth-service depends_on auth-users-backfill = %q, want service_completed_successfully: it serves before the users are copied", got)
	}
	if !runsWherever(backfill.Profiles, authService.Profiles) {
		t.Errorf("auth-users-backfill profiles = %v: it must start in every profile auth-service starts in", backfill.Profiles)
	}
}
