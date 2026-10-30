package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
)

const sourceTestPassword = "shadow-pw-do-not-print"

const sourceTestDSN = "host=shadow port=5432 user=postgres password='" + sourceTestPassword + "' dbname=postgres sslmode=disable"

type stubSource struct{}

func (stubSource) Revocations(context.Context, []string) (int64, string, error) { return 0, "[]", nil }

type sourceOpen struct {
	uris   []string
	closed int
}

func swapSourceOpener(t *testing.T, err error) *sourceOpen {
	t.Helper()
	got := &sourceOpen{}
	prev := openRevocationSource
	t.Cleanup(func() { openRevocationSource = prev })
	openRevocationSource = func(_ context.Context, uri string, _ io.Writer) (restorefence.RevocationSource, func(), error) {
		got.uris = append(got.uris, uri)
		if err != nil {
			return nil, nil, err
		}
		return stubSource{}, func() { got.closed++ }, nil
	}
	return got
}

func sourceManifestFile(t *testing.T) string {
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

func noSourceSecret(t *testing.T, outputs ...string) {
	t.Helper()
	for _, o := range outputs {
		if strings.Contains(o, sourceTestPassword) || strings.Contains(o, "host=shadow") {
			t.Errorf("the revocation source DSN reached the output: %q", o)
		}
	}
}

func TestExecApplyWithoutTheSourceEnvIsTodaysApply(t *testing.T) {
	opened := swapSourceOpener(t, nil)
	store := &stubStore{timeline: 2}
	rc, out, errOut := runExec(t, store, nil, "apply", "-manifest", sourceManifestFile(t), "-apply")
	if rc != 0 || store.applyOpts.Source != nil || len(opened.uris) != 0 {
		t.Fatalf("rc %d, source %v, opened %v, stderr %q", rc, store.applyOpts.Source, opened.uris, errOut)
	}
	noSecret(t, out, errOut)
}

func TestExecApplyReadsTheSourceFromTheEnvOnly(t *testing.T) {
	opened := swapSourceOpener(t, nil)
	store := &stubStore{timeline: 2}
	rc, out, errOut := runExecEnv(t, store, nil, map[string]string{envRevocationSource: sourceTestDSN}, "apply", "-manifest", sourceManifestFile(t), "-apply")
	if rc != 0 || store.applyOpts.Source == nil {
		t.Fatalf("rc %d, source %v, stderr %q", rc, store.applyOpts.Source, errOut)
	}
	if len(opened.uris) != 1 || opened.uris[0] != sourceTestDSN || opened.closed != 1 {
		t.Errorf("the source opener got %d uri(s) and closed %d times; want the env DSN once, closed once", len(opened.uris), opened.closed)
	}
	noSourceSecret(t, out, errOut)
	noSecret(t, out, errOut)
}

func TestExecSourceFileVariantIsRead(t *testing.T) {
	opened := swapSourceOpener(t, nil)
	path := filepath.Join(t.TempDir(), "source.dsn")
	if err := os.WriteFile(path, []byte(sourceTestDSN+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := &stubStore{timeline: 2}
	rc, out, errOut := runExecEnv(t, store, nil, map[string]string{envRevocationSource + "_FILE": path}, "apply", "-manifest", sourceManifestFile(t))
	if rc != 0 || store.applyOpts.Source == nil || len(opened.uris) != 1 || opened.uris[0] != sourceTestDSN {
		t.Fatalf("rc %d, source %v, opened %d, stderr %q", rc, store.applyOpts.Source, len(opened.uris), errOut)
	}
	noSourceSecret(t, out, errOut)
}

func TestExecAnUnavailableSourceHidesTheDSNAndAppliesNothing(t *testing.T) {
	swapSourceOpener(t, errors.New("dial "+sourceTestDSN))
	store := &stubStore{timeline: 2}
	rc, out, errOut := runExecEnv(t, store, nil, map[string]string{envRevocationSource: sourceTestDSN}, "apply", "-manifest", sourceManifestFile(t), "-apply")
	if rc != 1 || !strings.Contains(errOut, "revocation source endpoint unavailable") {
		t.Fatalf("rc %d, stderr %q", rc, errOut)
	}
	for _, c := range store.calls {
		if c == "apply" {
			t.Error("the apply ran although the source is unavailable")
		}
	}
	noSourceSecret(t, out, errOut)
}

func TestExecTheSourceNeedsAManifest(t *testing.T) {
	opened := swapSourceOpener(t, nil)
	store := &stubStore{}
	rc, out, errOut := runExecEnv(t, store, nil, map[string]string{envRevocationSource: sourceTestDSN}, "apply", "-blind-margin", "1000", "-apply")
	if rc == 0 || !strings.Contains(errOut, envRevocationSource) || !strings.Contains(errOut, "-manifest") {
		t.Fatalf("rc %d, stderr %q", rc, errOut)
	}
	if len(opened.uris) != 0 || len(store.calls) != 0 {
		t.Errorf("a blind margin with the source set reached the database: opened %d, calls %v", len(opened.uris), store.calls)
	}
	noSourceSecret(t, out, errOut)
}

func TestExecOtherVerbsNeverOpenTheSource(t *testing.T) {
	opened := swapSourceOpener(t, nil)
	for _, args := range [][]string{{"status"}, {"init"}} {
		runExecEnv(t, &stubStore{timeline: 1}, nil, map[string]string{envRevocationSource: sourceTestDSN}, args...)
	}
	if len(opened.uris) != 0 {
		t.Errorf("status/init opened the revocation source %d times", len(opened.uris))
	}
}

func TestTheRevocationSourceIsNeverAFlag(t *testing.T) {
	path := sourceManifestFile(t)
	for _, flagName := range []string{"-source", "-source-uri", "-revocation-source"} {
		rc, out, errOut := runExec(t, &stubStore{}, nil, "apply", "-manifest", path, flagName, sourceTestDSN)
		if rc != 2 {
			t.Errorf("apply %s DSN: rc %d, want 2 (the source DSN never travels on argv)", flagName, rc)
		}
		noSourceSecret(t, out, errOut)
	}
}

func TestTheDefaultRevocationSourceOpenerIsTheRealOne(t *testing.T) {
	if reflect.ValueOf(openRevocationSource).Pointer() != reflect.ValueOf(openPGRevocationSource).Pointer() {
		t.Fatal("openRevocationSource is not the real PostgreSQL opener: a test seam was left swapped")
	}
}
