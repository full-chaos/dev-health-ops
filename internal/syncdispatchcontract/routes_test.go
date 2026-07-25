package syncdispatchcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckedInArtifactIsFrozenAndLookupIsImmutable(t *testing.T) {
	t.Parallel()
	registry, err := Load(filepath.Join("..", "..", "contracts", "sync-dispatch", "v1"))
	if err != nil {
		t.Fatal(err)
	}

	for _, kind := range []string{
		KindDispatchSyncRun,
		KindFinalizeSyncRun,
		KindPostSync,
		KindReferenceDiscovery,
	} {
		descriptor, ok := registry.Lookup(kind)
		if !ok {
			t.Fatalf("%s route is missing", kind)
		}
		if descriptor.Route != RouteCelery || descriptor.RollbackRoute != RouteCelery {
			t.Fatalf("%s descriptor is not currently Celery-only: %#v", kind, descriptor)
		}
	}
	descriptor, ok := registry.Lookup(KindPostSync)
	if !ok || descriptor.Delivery != DeliveryAtLeastOnce {
		t.Fatalf("post_sync descriptor = %#v", descriptor)
	}
	descriptor.Route = RouteRiver
	again, ok := registry.Lookup(KindPostSync)
	if !ok || again.Route != RouteCelery {
		t.Fatalf("registry was mutated through lookup: %#v", again)
	}
	if _, ok := registry.Lookup("not-a-frozen-kind"); ok {
		t.Fatal("unknown route kind was found")
	}
}

func TestLoadRejectsInvalidArtifacts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		contents string
	}{
		{name: "malformed", contents: `{"schema_version":`},
		{name: "unknown field", contents: strings.Replace(canonicalArtifact, `"schema_version": 1,`, `"schema_version": 1, "unexpected": true,`, 1)},
		{name: "duplicate JSON key", contents: strings.Replace(canonicalArtifact, `"schema_version": 1,`, `"schema_version": 1, "schema_version": 1,`, 1)},
		{name: "out of order", contents: outOfOrderArtifact},
		{name: "missing coverage", contents: strings.Replace(canonicalArtifact, `"reference_discovery"`, `"unfrozen_kind"`, 1)},
		{name: "wrong delivery", contents: strings.Replace(canonicalArtifact, `"kind": "post_sync", "delivery": "at_least_once"`, `"kind": "post_sync", "delivery": "at_most_once_mark_before"`, 1)},
		{name: "unknown route", contents: strings.Replace(canonicalArtifact, `"route": "celery"`, `"route": "sqs"`, 1)},
		{name: "celery with river rollback", contents: strings.Replace(canonicalArtifact, `"rollback_route": "celery"`, `"rollback_route": "river"`, 1)},
		{name: "river with river rollback", contents: strings.ReplaceAll(canonicalArtifact, `"celery"`, `"river"`)},
		{name: "trailing data", contents: canonicalArtifact + "\n{}"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			root := writeArtifact(t, test.contents)
			if _, err := Load(root); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadAcceptsSelectableRiverRoutes(t *testing.T) {
	t.Parallel()
	root := writeArtifact(t, strings.Replace(canonicalArtifact, `"route": "celery"`, `"route": "river"`, 1))
	registry, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := registry.Lookup(KindDispatchSyncRun)
	if !ok || descriptor.Route != RouteRiver || descriptor.RollbackRoute != RouteCelery {
		t.Fatalf("dispatch_sync_run descriptor = %#v", descriptor)
	}
}

func TestLoadRejectsUnsafeOrInvalidPaths(t *testing.T) {
	t.Parallel()
	if _, err := Load(""); err == nil {
		t.Fatal("empty root was accepted")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing root was accepted")
	}

	fileRoot := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(fileRoot, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(fileRoot); err == nil {
		t.Fatal("file root was accepted")
	}

	t.Run("artifact symbolic link", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "target.json")
		if err := os.WriteFile(target, []byte(canonicalArtifact), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(root, Filename)); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(root); err == nil {
			t.Fatal("symbolic-link artifact was accepted")
		}
	})

	t.Run("root symbolic link", func(t *testing.T) {
		actual := writeArtifact(t, canonicalArtifact)
		linked := filepath.Join(t.TempDir(), "linked-root")
		if err := os.Symlink(actual, linked); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(linked); err == nil {
			t.Fatal("symbolic-link root was accepted")
		}
	})

	t.Run("artifact directory", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, Filename), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(root); err == nil {
			t.Fatal("directory artifact was accepted")
		}
	})
}

func TestLoadRejectsOversizedArtifact(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, Filename), []byte(strings.Repeat(" ", maxArtifactBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root); err == nil {
		t.Fatal("oversized artifact was accepted")
	}
}

func writeArtifact(t *testing.T, contents string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, Filename), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

const canonicalArtifact = `{
  "schema_version": 1,
  "routes": [
    {"kind": "dispatch_sync_run", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "finalize_sync_run", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "post_sync", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "reference_discovery", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"}
  ]
}`

const outOfOrderArtifact = `{
  "schema_version": 1,
  "routes": [
    {"kind": "finalize_sync_run", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "dispatch_sync_run", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "post_sync", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"},
    {"kind": "reference_discovery", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"}
  ]
}`
