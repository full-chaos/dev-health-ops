package main

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-13107: compose.yml forwards production's auth set (sessions, local login, credentials, social), and auth-service
// serves the credentials routes only while it owns users, so every service that reads AUTH_USERS_OWNED defaults it on,
// as production's phase 4: one pass-through, one default, for all of them. A proof that keeps users unowned pins its own
// value in its override (ci/auth_webproof_compose.override.yml, ci/auth_corpus_external.acp.override.yml).
func TestComposeUsersOwnedDefaultsOnForEveryReader(t *testing.T) {
	var file composeKeyFile
	if err := yaml.Unmarshal(readCompose(t, "compose.yml"), &file); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go-api", "auth-service", "auth-migrate", "auth-grants", "auth-fence-init", "auth-workload-seed", "auth-workload-bootstrap"} {
		got, ok := file.Services[name].Environment["AUTH_USERS_OWNED"]
		if !ok {
			t.Errorf("%s: AUTH_USERS_OWNED is not passed through", name)
			continue
		}
		if got != "${AUTH_USERS_OWNED:-true}" {
			t.Errorf("%s: AUTH_USERS_OWNED = %q, want the default-on pass-through ${AUTH_USERS_OWNED:-true}", name, got)
		}
	}
}
