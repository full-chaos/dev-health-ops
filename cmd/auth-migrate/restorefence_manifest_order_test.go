package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/keyrotation"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// An apply manifest is read and parsed before the migration store is opened: a manifest this binary refuses (an
// older version, or an unreadable file) costs no database connection and no role query.
func TestApplyRefusesABadManifestBeforeOpeningTheStore(t *testing.T) {
	cells := map[string]func(dir string) string{}
	for v := 1; v < restorefence.ManifestVersion; v++ {
		v := v
		cells[fmt.Sprintf("version %d", v)] = func(dir string) string {
			m := restorefence.Manifest{Version: v, SourceTimeline: 1}
			if err := m.Seal(); err != nil {
				t.Fatal(err)
			}
			raw, err := m.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "m.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}
	}
	cells["missing file"] = func(dir string) string { return filepath.Join(dir, "nope.json") }
	for name, write := range cells {
		t.Run(name, func(t *testing.T) {
			opened := 0
			prev := openRestoreFenceStore
			t.Cleanup(func() { openRestoreFenceStore = prev })
			openRestoreFenceStore = func(context.Context, string, io.Writer) (restoreFenceStore, error) {
				opened++
				return &stubStore{}, nil
			}
			path := write(t.TempDir())
			rc, _, errOut := runExecNoOpener(t, "apply", "-manifest", path)
			if rc != 1 || opened != 0 {
				t.Errorf("rc %d, store opened %d time(s), stderr %q: want rc 1 and no open", rc, opened, errOut)
			}
			if !strings.Contains(errOut, "export it again with this auth-migrate binary") && !strings.Contains(errOut, "could not read the manifest file") {
				t.Errorf("stderr %q names neither refusal", errOut)
			}
		})
	}
}

// runExecNoOpener runs the command with the opener the caller installed.
func runExecNoOpener(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	env := map[string]string{
		envMigrationURI: "host=db port=5432 user=postgres password='" + execTestPassword + "' dbname=postgres sslmode=disable",
		envRuntimeRole:  "devhealth_auth",
	}
	lookup := secrets.LookupEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	var out, errb bytes.Buffer
	rc := executeRestoreFence(context.Background(), args, lookup, &out, &errb)
	return rc, out.String(), errb.String()
}

func writeCurrentManifest(t *testing.T) (string, restorefence.Manifest) {
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
	return path, m
}

// A current manifest still reaches the store: it is opened once, the role is checked, and Apply gets the parsed
// manifest.
func TestApplyOfACurrentManifestOpensTheStoreOnceAndPassesIt(t *testing.T) {
	path, m := writeCurrentManifest(t)
	store := &stubStore{timeline: 2}
	opened := 0
	prev := openRestoreFenceStore
	t.Cleanup(func() { openRestoreFenceStore = prev })
	openRestoreFenceStore = func(context.Context, string, io.Writer) (restoreFenceStore, error) {
		opened++
		return store, nil
	}
	rc, out, errOut := runExecNoOpener(t, "apply", "-manifest", path)
	if rc != 0 || opened != 1 || store.applied == nil || store.applied.SHA256 != m.SHA256 {
		t.Fatalf("rc %d, opened %d, applied %+v, stdout %q, stderr %q", rc, opened, store.applied, out, errOut)
	}
	if strings.Join(store.calls, ",") != "role,apply" {
		t.Errorf("store calls %v: want the role check first, then apply", store.calls)
	}
}

// Parsing the manifest first does not skip the runtime-role guard: a good manifest run as the runtime role is
// refused, and Apply is never called.
func TestApplyOfAGoodManifestAsTheRuntimeRoleIsStillRefused(t *testing.T) {
	path, _ := writeCurrentManifest(t)
	store := &stubStore{roleErr: keyrotation.ErrRuntimeRole}
	rc, out, errOut := runExec(t, store, nil, "apply", "-manifest", path, "-apply")
	if rc != 1 || !strings.Contains(errOut, "connects as the runtime role") || store.applied != nil {
		t.Fatalf("rc %d, applied %+v, stderr %q", rc, store.applied, errOut)
	}
	if strings.Join(store.calls, ",") != "role" {
		t.Errorf("store calls %v: want only the role check", store.calls)
	}
	noSecret(t, out, errOut)
}
