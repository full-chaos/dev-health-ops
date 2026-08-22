package syncdispatchcontract

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		if descriptor.Route != RouteRiver || descriptor.RollbackRoute != RouteCelery {
			t.Fatalf("%s descriptor is not River with Celery rollback: %#v", kind, descriptor)
		}
	}
	descriptor, ok := registry.Lookup(KindPostSync)
	if !ok || descriptor.Delivery != DeliveryAtLeastOnce {
		t.Fatalf("post_sync descriptor = %#v", descriptor)
	}
	descriptor.Route = RouteCelery
	again, ok := registry.Lookup(KindPostSync)
	if !ok || again.Route != RouteRiver {
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

// TestRiverQueueIsPinnedToItsWireValue pins the queue NAME, not just its use.
//
// RiverQueue is a wire value: it is written into river_job.queue by the
// reconciler and read back by every worker's startup contract-version check.
// Renaming the constant would keep this repository internally consistent and
// still be a production incident -- rows already sitting in the old queue
// would be invisible to the new readers, and during a rolling deploy the two
// binaries would disagree about which queue the sync-dispatch plane occupies.
//
// Changing this string therefore requires a queue migration, not an edit. The
// literal is repeated here deliberately: a test that read the constant would
// agree with any rename and prove nothing.
func TestRiverQueueIsPinnedToItsWireValue(t *testing.T) {
	t.Parallel()
	if RiverQueue != "sync" {
		t.Fatalf("RiverQueue = %q, want \"sync\": this is a persisted wire value; "+
			"changing it strands in-flight river_job rows and splits old and new "+
			"binaries during a rolling deploy", RiverQueue)
	}
}

// TestDispatchStaleAgeReadsTheSameEnvVarAsPython pins DispatchStaleAge's
// contract against SYNC_UNIT_DISPATCH_STALE_SECONDS: unset falls back to the
// checked-in 900-second default (CHAOS-3929's negative control -- the
// callers relying on this default must not silently regress to zero), a
// valid override is honored so an operator's SYNC_UNIT_DISPATCH_STALE_SECONDS
// change actually reaches Go, and Python's _env_int fallback-on-error /
// clamp-on-negative behavior (sync/budget_guard.py._env_int) is mirrored
// exactly rather than approximately.
func TestDispatchStaleAgeReadsTheSameEnvVarAsPython(t *testing.T) {
	cases := []struct {
		name string
		env  string
		set  bool
		want time.Duration
	}{
		{name: "unset falls back to checked-in default", set: false, want: 900 * time.Second},
		{name: "empty falls back to checked-in default", env: "", set: true, want: 900 * time.Second},
		{name: "valid override is honored", env: "60", set: true, want: 60 * time.Second},
		{name: "zero is honored, not treated as unset", env: "0", set: true, want: 0},
		{name: "unparseable falls back to default, mirrors Python's except ValueError", env: "not-a-number", set: true, want: 900 * time.Second},
		{name: "negative clamps to zero, mirrors Python's max(0, value)", env: "-30", set: true, want: 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			original, wasSet := os.LookupEnv(dispatchStaleSecondsEnv)
			t.Cleanup(func() {
				if wasSet {
					os.Setenv(dispatchStaleSecondsEnv, original)
				} else {
					os.Unsetenv(dispatchStaleSecondsEnv)
				}
			})
			if test.set {
				os.Setenv(dispatchStaleSecondsEnv, test.env)
			} else {
				os.Unsetenv(dispatchStaleSecondsEnv)
			}
			if got := DispatchStaleAge(); got != test.want {
				t.Fatalf("DispatchStaleAge() = %v, want %v", got, test.want)
			}
		})
	}
}
