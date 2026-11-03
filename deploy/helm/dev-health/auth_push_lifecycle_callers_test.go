package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-12828: the External Push lifecycle writer is executable by exactly the
// logins in authService.migrate.pushLifecycle.callerRoles (go-api's
// devhealth_api and the worker domain login devhealth_domain). Both auth Jobs
// carry the same two settings.
var pushLifecycleHookArgs = []string{"--set", "migrations.hook.enabled=true"}

func pushLifecycleRender(t *testing.T, extra ...string) ([]map[string]any, string, error) {
	t.Helper()
	args := append(append(append([]string{}, authBase...), pushLifecycleHookArgs...), extra...)
	return renderAuth(t, args...)
}

func TestPushLifecycleTwoCallersRenderIntoBothAuthJobs(t *testing.T) {
	docs, out, err := pushLifecycleRender(t,
		"--set", "authService.migrate.pushLifecycle.writerRole=devhealth_auth_push_writer",
		"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","devhealth_domain"]`)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		doc := authDoc(docs, "Job", job)
		if doc == nil {
			t.Fatalf("job %s not rendered", job)
		}
		if v, _ := containerEnvValue(t, doc, "AUTH_PUSH_LIFECYCLE_WRITER_ROLE"); v != "devhealth_auth_push_writer" {
			t.Errorf("%s AUTH_PUSH_LIFECYCLE_WRITER_ROLE = %q", job, v)
		}
		if v, _ := containerEnvValue(t, doc, "AUTH_PUSH_LIFECYCLE_CALLER_ROLES"); v != "devhealth_api,devhealth_domain" {
			t.Errorf("%s AUTH_PUSH_LIFECYCLE_CALLER_ROLES = %q, want devhealth_api,devhealth_domain", job, v)
		}
	}
}

func TestPushLifecycleDefaultRendersNeitherSetting(t *testing.T) {
	docs, out, err := pushLifecycleRender(t)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		for _, name := range []string{"AUTH_PUSH_LIFECYCLE_WRITER_ROLE", "AUTH_PUSH_LIFECYCLE_CALLER_ROLES"} {
			if v, set := containerEnvValue(t, authDoc(docs, "Job", job), name); set {
				t.Errorf("%s sets %s=%q by default", job, name, v)
			}
		}
	}
}

// A half-set pair used to render silently: callers without a writer role were
// dropped, so the grant never happened. Each refusal names the key.
func TestPushLifecycleRefusals(t *testing.T) {
	const writer = "devhealth_auth_push_writer"
	cases := map[string][]string{
		"callers without a writer": {"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api"]`},
		"a writer without callers": {"--set", "authService.migrate.pushLifecycle.writerRole=" + writer},
		"the writer as a caller": {"--set", "authService.migrate.pushLifecycle.writerRole=" + writer,
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","` + writer + `"]`},
		"the runtime login as a caller": {"--set", "authService.migrate.pushLifecycle.writerRole=" + writer,
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_auth"]`},
		"the runtime login as the writer": {"--set", "authService.migrate.pushLifecycle.writerRole=devhealth_auth",
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api"]`},
		"a repeated caller": {"--set", "authService.migrate.pushLifecycle.writerRole=" + writer,
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","devhealth_api"]`},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := pushLifecycleRender(t, extra...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, "authService.migrate.pushLifecycle") {
				t.Fatalf("the refusal does not name authService.migrate.pushLifecycle: %.400s", out)
			}
		})
	}
}
