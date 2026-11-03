package devhealth_test

import (
	"strings"
	"testing"
)

// authService.migrate.ownerRole pins the owner of the auth schema and of every object in it for the two auth-migrate
// Jobs (AUTH_SCHEMA_OWNER_ROLE). Empty keeps the pin off at every phase and renders nothing.

func ownerPinJobs(t *testing.T, args ...string) map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	jobs := map[string]any{}
	for _, name := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		job := authDoc(docs, "Job", name)
		if job == nil {
			t.Fatalf("no %s Job", name)
		}
		jobs[name] = at(job, "spec", "template", "spec", "containers", 0)
	}
	return jobs
}

func TestAuthMigrateJobsRenderTheSchemaOwnerPin(t *testing.T) {
	args := append(append([]string{}, authBase...), "--set", "authService.migrate.ownerRole=devhealth_auth_migrator")
	for name, container := range ownerPinJobs(t, args...) {
		got, ok := envValue(container, "AUTH_SCHEMA_OWNER_ROLE")
		if !ok || got != "devhealth_auth_migrator" {
			t.Errorf("%s: AUTH_SCHEMA_OWNER_ROLE = %q (present %v), want devhealth_auth_migrator", name, got, ok)
		}
	}
}

func TestAuthMigrateJobsRenderNoPinWhenTheOwnerRoleIsEmpty(t *testing.T) {
	for name, container := range ownerPinJobs(t, authBase...) {
		if _, ok := envValue(container, "AUTH_SCHEMA_OWNER_ROLE"); ok {
			t.Errorf("%s: AUTH_SCHEMA_OWNER_ROLE is rendered while authService.migrate.ownerRole is empty", name)
		}
	}
}

func TestAuthOwnerRoleRefusals(t *testing.T) {
	owner := []string{"--set", "authService.migrate.ownerRole=pinned_owner"}
	cases := map[string][]string{
		"the runtime role":  {"--set", "authService.migrate.ownerRole=devhealth_auth"},
		"a session reader":  {"--set-json", `authService.migrate.sessionReaderRoles=["pinned_owner"]`},
		"the go-api reader": {"--set", "authService.migrate.sessionReaders.goApi=pinned_owner"},
		"the query reader":  {"--set", "authService.migrate.sessionReaders.queryApi=pinned_owner"},
		"the push writer":   {"--set", "authService.migrate.pushLifecycle.writerRole=pinned_owner", "--set-json", `authService.migrate.pushLifecycle.callerRoles=["api"]`},
		"a push caller":     {"--set", "authService.migrate.pushLifecycle.writerRole=push_writer", "--set-json", `authService.migrate.pushLifecycle.callerRoles=["pinned_owner"]`},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			args := append(append([]string{}, authBase...), extra...)
			if name != "the runtime role" {
				args = append(args, owner...)
			}
			_, out, err := renderAuth(t, args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, "authService.migrate.ownerRole") {
				t.Fatalf("the refusal does not name authService.migrate.ownerRole: %.400s", out)
			}
		})
	}
}

func TestAuthOwnerRoleEmptyIsAllowedAtEveryPhaseThatRenders(t *testing.T) {
	for _, phase := range []string{"1", "2", "3", "4"} {
		args := append([]string{}, authBase...)
		args = append(args, "--set", "authService.phase="+phase, "--set", "authService.migrate.ownerRole=")
		if phase != "1" {
			args = append(args, "--set", "authService.migrate.sessionReaders.goApi=go_api_login", "--set", "authService.migrate.sessionReaders.queryApi=query_api_login")
		}
		if _, out, err := renderAuth(t, args...); err != nil && strings.Contains(out, "authService.migrate.ownerRole") {
			t.Errorf("phase %s: an empty ownerRole is refused: %.300s", phase, out)
		}
	}
}
