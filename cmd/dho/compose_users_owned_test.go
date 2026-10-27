package main

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestComposeGoAPIUsersOwnedDefaultsOff(t *testing.T) {
	var file composeKeyFile
	if err := yaml.Unmarshal(readCompose(t, "compose.yml"), &file); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go-api", "auth-service", "auth-migrate", "auth-grants"} {
		got, ok := file.Services[name].Environment["AUTH_USERS_OWNED"]
		if !ok {
			t.Errorf("%s: AUTH_USERS_OWNED is not passed through", name)
			continue
		}
		if got != "${AUTH_USERS_OWNED:-false}" {
			t.Errorf("%s: AUTH_USERS_OWNED = %q, want the default-off pass-through ${AUTH_USERS_OWNED:-false}", name, got)
		}
	}
}
