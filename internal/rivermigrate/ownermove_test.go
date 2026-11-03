package rivermigrate

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
)

func TestOwnerMoveVerbIsRegisteredUnderMigrate(t *testing.T) {
	t.Parallel()
	found := false
	for _, child := range Command().Children {
		if child.Name == "owner-move" {
			found = child.Run != nil
		}
	}
	if !found {
		t.Fatal("`dho migrate owner-move` is not registered")
	}
}

func TestParseOwnerMoveArgs(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	for name, c := range map[string]struct {
		args []string
		mode ownermove.Mode
	}{
		"dry run": {[]string{"-schemas", "river", "-from", "devhealth", "-to", "devhealth_migrator"}, ownermove.DryRun},
		"plan":    {[]string{"-schemas", "public", "-database-owner", "-from", "devhealth", "-to", "devhealth_migrator", "-plan"}, ownermove.Plan},
		"apply":   {[]string{"-schemas", "public, river", "-database-owner", "-from", "devhealth", "-to", "devhealth_migrator", "-apply"}, ownermove.Apply},
		"restore": {[]string{"-schemas", "river", "-from", "devhealth_migrator", "-to", "devhealth", "-restore", "-apply"}, ownermove.Apply},
	} {
		options, err := parseOwnerMoveArgs(c.args, &stderr)
		if err != nil || options.Mode != c.mode {
			t.Errorf("%s: %+v %v", name, options, err)
		}
	}
	options, _ := parseOwnerMoveArgs([]string{"-schemas", "public, river", "-database-owner", "-from", "a_old", "-to", "a_new", "-restore"}, &stderr)
	if strings.Join(options.Schemas, ",") != "public,river" || !options.DatabaseOwner || !options.Restore || options.From != "a_old" || options.To != "a_new" {
		t.Fatalf("parsed %+v", options)
	}
	for name, args := range map[string][]string{
		"plan and apply":  {"-schemas", "river", "-from", "a", "-to", "b", "-plan", "-apply"},
		"no schema":       {"-from", "a", "-to", "b"},
		"positional":      {"-schemas", "river", "-from", "a", "-to", "b", "extra"},
		"a bad role name": {"-schemas", "river", "-from", "a", "-to", "B-ad"},
	} {
		if _, err := parseOwnerMoveArgs(args, &stderr); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestOwnerMoveRefusesABadCommandLineBeforeItConnects(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := ExecuteOwnerMove(context.Background(), []string{"-plan", "-apply", "-schemas", "river", "-from", "a", "-to", "b"}, envLookup(nil), &stdout, &stderr); code != 2 {
		t.Fatalf("exit %d, want 2: %s", code, stderr.String())
	}
	if code := ExecuteOwnerMove(context.Background(), []string{"-h"}, envLookup(nil), &stdout, &stderr); code != 0 {
		t.Fatalf("help exit %d", code)
	}
}

func TestOwnerMigratorRoleIsReadFromTheEnvironment(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"RIVER_DOMAIN_DATABASE_ROLE": "d", "RIVER_DOMAIN_DATABASE_PASSWORD": "x",
		"RIVER_QUEUE_DATABASE_ROLE": "q", "RIVER_QUEUE_DATABASE_PASSWORD": "x",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "x",
	}
	var stderr bytes.Buffer
	off, ok := rolesOptionsFromEnvironment(envLookup(base), &stderr)
	if !ok || off.Migrator.Name != "" {
		t.Fatalf("unset = off: ok=%v %+v", ok, off.Migrator)
	}
	on := map[string]string{"OPS_MIGRATOR_DATABASE_ROLE": "devhealth_migrator", "OPS_MIGRATOR_DATABASE_PASSWORD": "pw-mig"}
	for k, v := range base {
		on[k] = v
	}
	options, ok := rolesOptionsFromEnvironment(envLookup(on), &stderr)
	if !ok || options.Migrator.Name != "devhealth_migrator" || options.Migrator.Password != "pw-mig" {
		t.Fatalf("migrator not read: ok=%v %+v", ok, options.Migrator)
	}
	if roleNames(options)["migrator"] != "devhealth_migrator" || roleLabels(options)[len(roleLabels(options))-1] != "migrator" {
		t.Fatalf("names %v labels %v", roleNames(options), roleLabels(options))
	}
	delete(on, "OPS_MIGRATOR_DATABASE_PASSWORD")
	stderr.Reset()
	if _, ok := rolesOptionsFromEnvironment(envLookup(on), &stderr); ok || !strings.Contains(stderr.String(), "OPS_MIGRATOR_DATABASE_PASSWORD") {
		t.Fatalf("a named migrator role without its password: ok=%v %s", ok, stderr.String())
	}
}

func TestAuthSchemaOf(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]string{"": "auth", "  ": "auth", "auth2": "auth2", "Bad-Name": ""} {
		if got := authSchemaOf(envLookup(map[string]string{"AUTH_DATABASE_SCHEMA": value})); got != want {
			t.Errorf("AUTH_DATABASE_SCHEMA=%q -> %q, want %q", value, got, want)
		}
	}
}
