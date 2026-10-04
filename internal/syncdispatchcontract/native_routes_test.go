package syncdispatchcontract

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	syncdispatchv1 "github.com/full-chaos/dev-health-ops/contracts/sync-dispatch/v1"
)

// TestNativeRiverRouteKindsMatchesTheCheckedInPolicy pins the embedded policy
// to the checked-in file, and the derived set to the registry Load builds from
// that file: every kind Load reads as route river with rollback none, in
// artifact order, and nothing else.
func TestNativeRiverRouteKindsMatchesTheCheckedInPolicy(t *testing.T) {
	root := filepath.Join("..", "..", "contracts", "sync-dispatch", "v1")
	onDisk, err := os.ReadFile(filepath.Join(root, Filename))
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(syncdispatchv1.TransportRoutes) {
		t.Fatal("embedded transport-routes.json differs from the checked-in file")
	}
	registry, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, kind := range Kinds() {
		descriptor, known := registry.Lookup(kind)
		if !known {
			t.Fatalf("the checked-in policy has no route for %s", kind)
		}
		if descriptor.Route == RouteRiver && descriptor.RollbackRoute == RouteNone {
			want = append(want, kind)
		}
	}
	got, err := NativeRiverRouteKinds(syncdispatchv1.TransportRoutes)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("NativeRiverRouteKinds = %v, want %v", got, want)
	}
}

// TestNativeRiverRouteKindsInputDomain runs the bytes a binary could embed:
// each case either yields exactly the kinds on river with no rollback route
// or refuses the bytes, by the checks Load applies to the file.
func TestNativeRiverRouteKindsInputDomain(t *testing.T) {
	retire := func(artifact string, kinds ...string) string {
		for _, kind := range kinds {
			old := `"kind": "` + kind + `", "delivery": "at_least_once", "route": "celery", "rollback_route": "celery"`
			if !strings.Contains(artifact, old) {
				t.Fatalf("the canonical artifact has no celery row for %s", kind)
			}
			artifact = strings.Replace(artifact, old,
				`"kind": "`+kind+`", "delivery": "at_least_once", "route": "river", "rollback_route": "none"`, 1)
		}
		return artifact
	}
	riverWithRollback := strings.Replace(canonicalArtifact,
		`"kind": "post_sync", "delivery": "at_least_once", "route": "celery"`,
		`"kind": "post_sync", "delivery": "at_least_once", "route": "river"`, 1)
	for _, tc := range []struct {
		name    string
		data    string
		want    []string
		wantErr bool
	}{
		{"every kind on celery", canonicalArtifact, []string{}, false},
		{"river that still names a rollback route", riverWithRollback, []string{}, false},
		{"two retired", retire(canonicalArtifact, KindDispatchSyncRun, KindPostSync), []string{KindDispatchSyncRun, KindPostSync}, false},
		{"all retired", retire(canonicalArtifact, KindDispatchSyncRun, KindFinalizeSyncRun, KindPostSync, KindReferenceDiscovery),
			[]string{KindDispatchSyncRun, KindFinalizeSyncRun, KindPostSync, KindReferenceDiscovery}, false},
		{"empty", "", nil, true},
		{"null", "null", nil, true},
		{"malformed", `{"schema_version":`, nil, true},
		{"unknown field", strings.Replace(canonicalArtifact, `"schema_version": 1,`, `"schema_version": 1, "unexpected": true,`, 1), nil, true},
		{"out of order", outOfOrderArtifact, nil, true},
		{"missing coverage", strings.Replace(canonicalArtifact, `"reference_discovery"`, `"unfrozen_kind"`, 1), nil, true},
		{"celery route with retired rollback", strings.Replace(canonicalArtifact, `"rollback_route": "celery"`, `"rollback_route": "none"`, 1), nil, true},
		{"trailing data", canonicalArtifact + "\n{}", nil, true},
		{"oversized", canonicalArtifact + strings.Repeat(" ", maxArtifactBytes), nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NativeRiverRouteKinds([]byte(tc.data))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NativeRiverRouteKinds accepted the bytes and returned %v", got)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("kinds=%v err=%v, want %v", got, err, tc.want)
			}
		})
	}
}
