package main

import (
	"io"
	"strings"
	"testing"
)

func ownerMoveLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

func TestParseOwnerMoveArgs(t *testing.T) {
	env := ownerMoveLookup(map[string]string{"AUTH_RUNTIME_DATABASE_ROLE": "devhealth_auth"})
	o, err := parseOwnerMoveArgs([]string{"-from", "devhealth", "-to", "devhealth_auth_migrator"}, env, io.Discard)
	if err != nil {
		t.Fatalf("a valid dry run: %v", err)
	}
	if o.From != "devhealth" || o.To != "devhealth_auth_migrator" || o.RuntimeRole != "devhealth_auth" || o.Schema != "auth" || o.Apply || o.GrantsOnly || o.Restore {
		t.Errorf("options = %+v, want a dry run on schema auth with the runtime role from the environment", o)
	}
	o, err = parseOwnerMoveArgs([]string{"-from", "a", "-to", "b", "-apply", "-restore", "-runtime-role", "rt", "-schema", "authx"}, env, io.Discard)
	if err != nil || !o.Apply || !o.Restore || o.RuntimeRole != "rt" || o.Schema != "authx" {
		t.Errorf("flags not read: %+v %v", o, err)
	}
	o, err = parseOwnerMoveArgs([]string{"-from", "a", "-to", "b", "-grants-only"}, env, io.Discard)
	if err != nil || !o.GrantsOnly {
		t.Errorf("-grants-only not read: %+v %v", o, err)
	}
	for name, args := range map[string][]string{
		"no from":             {"-to", "b"},
		"no to":               {"-from", "a"},
		"same role":           {"-from", "a", "-to", "a"},
		"positional argument": {"-from", "a", "-to", "b", "extra"},
		"unknown flag":        {"-from", "a", "-to", "b", "-force"},
		"restore and grants":  {"-from", "a", "-to", "b", "-restore", "-grants-only"},
		"bad role":            {"-from", "a;b", "-to", "b"},
	} {
		if _, err := parseOwnerMoveArgs(args, env, io.Discard); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := parseOwnerMoveArgs([]string{"-from", "a", "-to", "b"}, ownerMoveLookup(nil), io.Discard); err == nil {
		t.Error("no runtime role in the flags or the environment: accepted")
	}
}

func TestOwnerMoveRefusesABadCommandLineBeforeTheDatabase(t *testing.T) {
	lookup := ownerMoveLookup(map[string]string{"AUTH_RUNTIME_DATABASE_ROLE": "rt"})
	var stderr strings.Builder
	if code := execute(t.Context(), []string{"owner-move", "-to", "b"}, lookup, io.Discard, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "usage: auth-migrate owner-move") {
		t.Errorf("stderr = %q, want the usage line", stderr.String())
	}
}

func TestOwnerMoveNeedsTheMigrationEndpoint(t *testing.T) {
	lookup := ownerMoveLookup(map[string]string{"AUTH_RUNTIME_DATABASE_ROLE": "rt"})
	var stderr strings.Builder
	if code := execute(t.Context(), []string{"owner-move", "-from", "a", "-to", "b"}, lookup, io.Discard, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "AUTH_MIGRATION_DATABASE_URI") {
		t.Errorf("stderr = %q, want the missing endpoint named", stderr.String())
	}
}

func TestSchemaOwnerRoleFromTheEnvironment(t *testing.T) {
	if role, err := schemaOwnerRole(ownerMoveLookup(nil)); err != nil || role != "" {
		t.Errorf("unset = %q %v, want the pin off", role, err)
	}
	if role, err := schemaOwnerRole(ownerMoveLookup(map[string]string{"AUTH_SCHEMA_OWNER_ROLE": " auth_migrator "})); err != nil || role != "auth_migrator" {
		t.Errorf("set = %q %v", role, err)
	}
	if _, err := schemaOwnerRole(ownerMoveLookup(map[string]string{"AUTH_SCHEMA_OWNER_ROLE": "bad;role"})); err == nil {
		t.Error("an unusable role name is accepted")
	}
}
