package devhealth_test

import (
	"strings"
	"testing"
)

var platformRolesReaders = []struct{ kind, name string }{
	{"Deployment", "a-dev-health-auth-service"},
	{"Deployment", "a-dev-health-go-api"},
	{"Deployment", "a-dev-health-query-api"},
	{"Job", "a-dev-health-auth-migrate"},
	{"Job", "a-dev-health-auth-grants"},
}

const platformRolesEnv = "AUTH_PLATFORM_ROLES_OWNED"

// Default and false render the switch nowhere, at phase 4 stage owned too.
func TestPlatformRolesOwnedFalseRendersNothing(t *testing.T) {
	for _, extra := range [][]string{nil, {"--set", "authService.platformRolesOwned=false"}} {
		docs, out, err := renderAuth(t, phaseArgs(4, extra...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.400s", err, out)
		}
		for _, target := range platformRolesReaders {
			doc := authDoc(docs, target.kind, target.name)
			if doc == nil {
				t.Fatalf("no %s %s in the render", target.kind, target.name)
			}
			if value, set := containerEnvValue(t, doc, platformRolesEnv); set {
				t.Errorf("%s sets %s=%q; want it unset", target.name, platformRolesEnv, value)
			}
		}
	}
}

// True renders AUTH_PLATFORM_ROLES_OWNED=true on all six readers.
func TestPlatformRolesOwnedTrueRendersOnEverySite(t *testing.T) {
	args := phaseArgs(4, "--set", "authService.platformRolesOwned=true")
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, target := range platformRolesReaders {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			t.Fatalf("no %s %s in the render", target.kind, target.name)
		}
		if value, set := containerEnvValue(t, doc, platformRolesEnv); !set || value != "true" {
			t.Errorf("%s %s=%q set=%v; want true", target.name, platformRolesEnv, value, set)
		}
	}
	_, job := renderBackfill(t, platformRolesBackfillArgs("authService.platformRolesOwned=true"))
	if value, set := containerEnvValue(t, job, platformRolesEnv); !set || value != "true" {
		t.Errorf("auth-backfill Job %s=%q set=%v; want true", platformRolesEnv, value, set)
	}
}

// True needs users owned: below phase 4 and at stage goApi the render fails.
func TestPlatformRolesOwnedTrueNeedsUsersOwned(t *testing.T) {
	for name, args := range map[string][]string{
		"phase 3":       phaseArgs(3, "--set", "authService.platformRolesOwned=true"),
		"phase 4 goApi": phaseArgs(4, "--set", "authService.usersOwnedStage=goApi", "--set", "authService.platformRolesOwned=true"),
		"go-api only":   {"--set", "goApi.enabled=true", "--set", "authService.platformRolesOwned=true"},
		"not a boolean": phaseArgs(4, "--set", "authService.platformRolesOwned=yes"),
	} {
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "platformRolesOwned") {
			t.Errorf("%s: rendered: err=%v out=%.300s", name, err, out)
		}
	}
}

// The switch has one source: no extraEnv can set it.
func TestPlatformRolesOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	for _, key := range []string{"authService.extraEnv", "goApi.extraEnv", "queryApi.extraEnv", "authService.backfill.extraEnv"} {
		entries := `[{"name":"AUTH_PLATFORM_ROLES_OWNED","value":"true"}]`
		if key == "queryApi.extraEnv" {
			entries = `[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"},{"name":"AUTH_PLATFORM_ROLES_OWNED","value":"true"}]`
		}
		args := phaseArgs(4, "--set-json", key+"="+entries)
		if strings.HasPrefix(key, "authService.backfill") {
			args = append(platformRolesBackfillArgs(), "--set-json", key+"="+entries)
		}
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, key+" must not set AUTH_PLATFORM_ROLES_OWNED") && !strings.Contains(out, "AUTH_PLATFORM_ROLES_OWNED") {
			t.Errorf("%s: extraEnv rendered: err=%v out=%.300s", key, err, out)
		}
	}
}

func platformRolesBackfillArgs(sets ...string) []string {
	args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...), append([]string{"authService.backfill.verb=users-reconcile",
		"authService.phase=4", "authService.usersOwnedStage=owned", "goApi.enabled=true"}, sets...)...)
	return append(append(append(args, sessionReaders...), sessionSuccessor...), authTrusted...)
}
