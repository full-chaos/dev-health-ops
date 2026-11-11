package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-13057: the repository identity append function is owned by its own
// NOLOGIN writer and executable by exactly one login, the worker domain login
// of the provider sync. Both auth Jobs carry the same two settings.
func TestRepoIdentityWriterRendersIntoBothAuthJobs(t *testing.T) {
	t.Parallel()

	docs, out, err := pushLifecycleRender(t,
		"--set", "authService.migrate.repoIdentity.writerRole=devhealth_auth_repo_identity_writer",
		"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		doc := authDoc(docs, "Job", job)
		if doc == nil {
			t.Fatalf("job %s not rendered", job)
		}
		if v, _ := containerEnvValue(t, doc, "AUTH_REPO_IDENTITY_WRITER_ROLE"); v != "devhealth_auth_repo_identity_writer" {
			t.Errorf("%s AUTH_REPO_IDENTITY_WRITER_ROLE = %q", job, v)
		}
		if v, _ := containerEnvValue(t, doc, "AUTH_REPO_IDENTITY_CALLER_ROLES"); v != "devhealth_domain" {
			t.Errorf("%s AUTH_REPO_IDENTITY_CALLER_ROLES = %q, want devhealth_domain", job, v)
		}
	}
}

func TestRepoIdentityDefaultRendersNeitherSetting(t *testing.T) {
	t.Parallel()

	docs, out, err := pushLifecycleRender(t)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, job := range []string{"a-dev-health-auth-migrate", "a-dev-health-auth-grants"} {
		for _, name := range []string{"AUTH_REPO_IDENTITY_WRITER_ROLE", "AUTH_REPO_IDENTITY_CALLER_ROLES"} {
			if v, set := containerEnvValue(t, authDoc(docs, "Job", job), name); set {
				t.Errorf("%s sets %s=%q by default", job, name, v)
			}
		}
	}
}

// Each refusal names the key, at phase 0 (no auth Job renders) and at phase 1.
func TestRepoIdentityRefusalsAtEveryPhase(t *testing.T) {
	t.Parallel()

	const writer = "devhealth_auth_repo_identity_writer"
	set := func(callers string) []string {
		return []string{"--set", "authService.migrate.repoIdentity.writerRole=" + writer,
			"--set-json", "authService.migrate.repoIdentity.callerRoles=" + callers}
	}
	cases := map[string][]string{
		"callers without a writer":      {"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`},
		"a writer without callers":      {"--set", "authService.migrate.repoIdentity.writerRole=" + writer},
		"two callers (go-api included)": set(`["devhealth_domain","devhealth_api"]`),
		"the writer as the caller":      set(`["` + writer + `"]`),
		"the runtime login as caller":   set(`["devhealth_auth"]`),
		"a blank caller":                set(`[""]`),
		"an invalid caller name":        set(`["bad;name"]`),
		"the runtime login as the writer": {"--set", "authService.migrate.repoIdentity.writerRole=devhealth_auth",
			"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`},
		"the push writer as the caller": {"--set", "authService.migrate.repoIdentity.writerRole=" + writer,
			"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_auth_push_writer"]`,
			"--set", "authService.migrate.pushLifecycle.writerRole=devhealth_auth_push_writer",
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","devhealth_domain"]`},
		"a push caller as the writer": {"--set", "authService.migrate.repoIdentity.writerRole=devhealth_api",
			"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`,
			"--set", "authService.migrate.pushLifecycle.writerRole=devhealth_auth_push_writer",
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","devhealth_domain"]`},
		"the push writer as the writer": {"--set", "authService.migrate.repoIdentity.writerRole=devhealth_auth_push_writer",
			"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`,
			"--set", "authService.migrate.pushLifecycle.writerRole=devhealth_auth_push_writer",
			"--set-json", `authService.migrate.pushLifecycle.callerRoles=["devhealth_api","devhealth_domain"]`},
	}
	for name, extra := range cases {
		for phase, base := range map[string][]string{"phase 0": nil, "phase 1": authBase} {
			t.Run(name+" at "+phase, func(t *testing.T) {
				t.Parallel()

				args := append(append([]string{}, base...), extra...)
				_, out, err := renderAuthRaw(t, args...)
				if err == nil {
					t.Fatal("the render succeeded; it must refuse")
				}
				if !strings.Contains(out, "authService.migrate.repoIdentity") {
					t.Fatalf("the refusal does not name authService.migrate.repoIdentity: %.400s", out)
				}
			})
		}
	}
}

func TestRepoIdentityValidPairRendersAtPhaseZero(t *testing.T) {
	t.Parallel()

	if _, out, err := renderAuthRaw(t,
		"--set", "authService.migrate.repoIdentity.writerRole=devhealth_auth_repo_identity_writer",
		"--set-json", `authService.migrate.repoIdentity.callerRoles=["devhealth_domain"]`); err != nil {
		t.Fatalf("a valid pair must render at phase 0: %v\n%.400s", err, out)
	}
}
