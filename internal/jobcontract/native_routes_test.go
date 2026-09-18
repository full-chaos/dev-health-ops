package jobcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
)

const checkedInContractRoot = "../../contracts/jobs/v1"

// TestNativeRiverRouteKindsMatchesTheCheckedInPolicy pins the embedded policy
// to the checked-in file and the derived set to the validated policy: every
// kind LoadMigrationState reads as route river with rollback none, in file
// order, and nothing else.
func TestNativeRiverRouteKindsMatchesTheCheckedInPolicy(t *testing.T) {
	onDisk, err := os.ReadFile(filepath.Join(checkedInContractRoot, migrationFilename))
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(jobsv1.MigrationState) {
		t.Fatal("embedded migration-state.json differs from the checked-in file")
	}
	registry, err := LoadRegistry(checkedInContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	state, err := LoadMigrationState(checkedInContractRoot, registry)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, job := range state.Jobs {
		if job.Route == "river" && job.RollbackRoute == "none" {
			want = append(want, job.Kind)
		}
	}
	got, err := NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil {
		t.Fatal(err)
	}
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatalf("NativeRiverRouteKinds = %v, want %v", got, want)
	}
}

func migrationJobJSON(kind, route, rollback string) map[string]any {
	return map[string]any{
		"kind": kind, "state": "celery_removed", "producer_version": 1,
		"consumer_versions": []int{1}, "required_queues": []string{"heartbeat"},
		"route": route, "rollback_route": rollback, "evidence": []string{"contract_schema"},
	}
}

func encodeState(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestNativeRiverRouteKindsInputDomain runs the parser's whole input domain:
// each cell either yields exactly the river/none kinds or refuses the bytes.
func TestNativeRiverRouteKindsInputDomain(t *testing.T) {
	valid := func(jobs ...map[string]any) []byte {
		return encodeState(t, map[string]any{"schema_version": 1, "jobs": jobs})
	}
	for _, tc := range []struct {
		name    string
		data    []byte
		want    []string
		wantErr string
	}{
		{"canonical", valid(
			migrationJobJSON("a.one", "river", "none"),
			migrationJobJSON("b.two", "river", "celery"),
			migrationJobJSON("c.three", "celery", "celery"),
			migrationJobJSON("d.four", "river", "none"),
		), []string{"a.one", "d.four"}, ""},
		{"none native", valid(migrationJobJSON("a.one", "celery", "celery")), []string{}, ""},
		{"absent", nil, nil, "empty"},
		{"null", []byte("null"), nil, "schema_version"},
		{"empty object", []byte("{}"), nil, "schema_version"},
		{"wrong container", []byte("[]"), nil, "decode"},
		{"trailing bytes", append(valid(migrationJobJSON("a.one", "river", "none")), []byte(" {}")...), nil, "decode"},
		{"unknown field", encodeState(t, map[string]any{"schema_version": 1, "jobs": []any{}, "extra": 1}), nil, "decode"},
		{"schema version zero", encodeState(t, map[string]any{"schema_version": 0, "jobs": []any{migrationJobJSON("a.one", "river", "none")}}), nil, "schema_version"},
		{"schema version two", encodeState(t, map[string]any{"schema_version": 2, "jobs": []any{migrationJobJSON("a.one", "river", "none")}}), nil, "schema_version"},
		{"schema version string", []byte(`{"schema_version":"1","jobs":[]}`), nil, "decode"},
		{"jobs empty", valid(), nil, "no jobs"},
		{"jobs null", []byte(`{"schema_version":1,"jobs":null}`), nil, "no jobs"},
		{"jobs wrong container", []byte(`{"schema_version":1,"jobs":{}}`), nil, "decode"},
		{"kind empty", valid(migrationJobJSON("", "river", "none")), nil, "invalid"},
		{"kind malformed", valid(migrationJobJSON("NotAKind", "river", "none")), nil, "invalid"},
		{"kind at bound", valid(migrationJobJSON("a."+strings.Repeat("b", 94), "river", "none")), []string{"a." + strings.Repeat("b", 94)}, ""},
		{"kind past bound", valid(migrationJobJSON("a."+strings.Repeat("b", 95), "river", "none")), nil, "invalid"},
		{"duplicate kind", valid(migrationJobJSON("a.one", "river", "none"), migrationJobJSON("a.one", "river", "none")), nil, "sorted"},
		{"unsorted kinds", valid(migrationJobJSON("b.two", "river", "none"), migrationJobJSON("a.one", "river", "none")), nil, "sorted"},
		{"route out of vocabulary", valid(migrationJobJSON("a.one", "rivers", "none")), nil, "routing"},
		{"rollback out of vocabulary", valid(migrationJobJSON("a.one", "river", "nothing")), nil, "routing"},
		{"route wrong type", []byte(`{"schema_version":1,"jobs":[{"kind":"a.one","state":"celery_removed","producer_version":1,"consumer_versions":[1],"required_queues":["q"],"route":1,"rollback_route":"none","evidence":[]}]}`), nil, "decode"},
		{"duplicate key", []byte(`{"schema_version":1,"schema_version":1,"jobs":[]}`), nil, "decode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NativeRiverRouteKinds(tc.data)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v, want one mentioning %q (got kinds %v)", err, tc.wantErr, got)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("kinds=%v err=%v, want %v", got, err, tc.want)
			}
		})
	}
}

// TestRegistryGoOnlyMarker runs the marker's input domain through the real
// loader: only the literal true is a Go-only kind, absent is shared, and every
// other spelling refuses the registry.
func TestRegistryGoOnlyMarker(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    GoOnlyFlag
		wantErr bool
	}{
		{"absent", "", false, false},
		{"true", "true", true, false},
		{"false", "false", false, true},
		{"null", "null", false, true},
		{"zero", "0", false, true},
		{"one", "1", false, true},
		{"string", `"true"`, false, true},
		{"empty container", "{}", false, true},
		{"array", "[true]", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			copyTree(t, checkedInContractRoot, root)
			path := filepath.Join(root, registryFilename)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.value != "" {
				marker := `"kind": "system.heartbeat",`
				if !strings.Contains(string(data), marker) {
					t.Fatalf("registry has no %s", marker)
				}
				data = []byte(strings.Replace(string(data), marker,
					marker+` "go_only": `+tc.value+`,`, 1))
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			registry, err := LoadRegistry(root)
			if tc.wantErr {
				if err == nil {
					t.Fatal("registry with a non-true go_only loaded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, job := range registry.Jobs {
				if job.Kind == "system.heartbeat" && job.GoOnly != tc.want {
					t.Fatalf("go_only=%v want %v", job.GoOnly, tc.want)
				}
			}
		})
	}
}
