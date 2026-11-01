package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
)

func platformRoleManifestFile(t *testing.T) string {
	t.Helper()
	m := restorefence.Manifest{Version: restorefence.ManifestVersion, SourceTimeline: 2, Floors: []restorefence.Floor{}, Sections: []restorefence.Section{}}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	raw, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "m.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// D6593: the status verb reads only: it never reaches Apply, Export or Init, and it prints the stored platform role lines.
func TestExecStatusReadsOnlyAndPrintsThePlatformRoleLines(t *testing.T) {
	store := &stubStore{timeline: 1, actions: []restorefence.OperatorAction{
		{Kind: restorefence.ActionRegrantPlatformRole, Source: "auth.platform_role_assignments", IDs: []string{"g1"}},
		{Kind: restorefence.ActionNoActivePlatformAdmin, Source: "auth.platform_role_assignments", IDs: []string{}},
	}}
	rc, out, _ := runExec(t, store, nil, "status")
	if rc != 0 {
		t.Fatalf("rc %d, stdout %q", rc, out)
	}
	if want := []string{"role", "timeline", "check", "actions"}; !reflect.DeepEqual(store.calls, want) {
		t.Errorf("status calls %v, want only reads %v", store.calls, want)
	}
	for _, want := range []string{
		"operator-action regrant_platform_role source=auth.platform_role_assignments count=1 ids=g1",
		"operator-action no_active_platform_admin source=auth.platform_role_assignments count=0 ids=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status lacks %q: %q", want, out)
		}
	}
}

func TestExecApplyPassesTheStrictPlatformRolesSwitch(t *testing.T) {
	path := platformRoleManifestFile(t)
	for env, want := range map[string]bool{"": false, "false": false, "true": true, " TRUE ": true} {
		store := &stubStore{timeline: 2}
		extra := map[string]string{}
		if env != "" {
			extra["AUTH_PLATFORM_ROLES_OWNED"] = env
		}
		if rc, _, errOut := runExecEnv(t, store, nil, extra, "apply", "-manifest", path); rc != 0 || store.applyOpts.PlatformRolesOwned != want {
			t.Errorf("AUTH_PLATFORM_ROLES_OWNED=%q: rc %d owned %v (stderr %q), want %v", env, rc, store.applyOpts.PlatformRolesOwned, errOut, want)
		}
	}
	store := &stubStore{timeline: 2}
	rc, _, errOut := runExecEnv(t, store, nil, map[string]string{"AUTH_PLATFORM_ROLES_OWNED": "yes"}, "apply", "-manifest", path)
	if rc != 1 || !strings.Contains(errOut, "AUTH_PLATFORM_ROLES_OWNED must be true or false") {
		t.Errorf("a bad value: rc %d stderr %q, want a configuration error", rc, errOut)
	}
	for _, c := range store.calls {
		if c == "apply" {
			t.Error("a bad switch reached Apply")
		}
	}
}
