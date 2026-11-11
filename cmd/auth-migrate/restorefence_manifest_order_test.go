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
