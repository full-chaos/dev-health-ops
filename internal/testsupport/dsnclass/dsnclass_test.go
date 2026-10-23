package dsnclass

import (
	"reflect"
	"testing"
)

func TestIsDSN(t *testing.T) {
	for name, want := range map[string]bool{
		"POSTGRES_URI":                   true,
		"WORKER_DATABASE_URI":            true,
		"WORKER_DATABASE_URI_FILE":       true,
		"COORDINATOR_DATABASE_URI":       true,
		"REPORTS_POSTGRES_URI":           true,
		"CLICKHOUSE_URI":                 false,
		"VALKEY_URI":                     false,
		"WORKER_DATABASE_MODE":           false,
		"DEV_HEALTH_PG_QUEUE_HOST":       false,
		"RIVER_QUEUE_DATABASE_ROLE":      false,
		"WORKER_DATABASE_URI_FILE_EXTRA": false,
	} {
		if got := IsDSN(name); got != want {
			t.Errorf("IsDSN(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestCarriedCountsComponentGroupsAndIgnoresEmptyValues(t *testing.T) {
	got := Carried(map[string]string{
		"POSTGRES_URI":                      "postgresql://x@h/db",
		"WORKER_DATABASE_URI":               "  ",
		"COORDINATOR_DATABASE_URI_FILE":     "/run/secrets/c",
		"DEV_HEALTH_PG_QUEUE_HOST":          "q",
		"DEV_HEALTH_PG_API_HOST":            "",
		"DEV_HEALTH_PG_API_PASSWORD":        "secret",
		"CLICKHOUSE_URI":                    "clickhouse://h",
		"DEV_HEALTH_PG_COORDINATOR_PORT":    "6434",
		"DEV_HEALTH_PG_DOMAIN_HOST":         "",
		"DEV_HEALTH_MIGRATION_PG_HOST":      "m",
		"WORKER_DATABASE_MODE":              "session",
		"SOME_UNRELATED_DATABASE_URI_ALIAS": "x",
	})
	want := []string{"COORDINATOR_DATABASE_URI", "MIGRATION_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Carried = %v, want %v", got, want)
	}
}

func TestClassesFiltersSecretEnvByThePattern(t *testing.T) {
	classes, err := Classes([]byte(`{
	  "processes": [
	    {"name": "sync", "secret_env": ["CLICKHOUSE_URI", "POSTGRES_URI", "VALKEY_URI", "WORKER_DATABASE_URI"]},
	    {"name": "stream-ingest", "secret_env": ["CLICKHOUSE_URI", "POSTGRES_URI"]}
	  ],
	  "operator_cli": {"secret_env": ["COORDINATOR_DATABASE_URI", "POSTGRES_URI"]},
	  "migration_job": {"secret_env": ["CLICKHOUSE_URI", "MIGRATION_DATABASE_URI"]}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]bool{
		"sync":          {"POSTGRES_URI": true, "WORKER_DATABASE_URI": true},
		"stream-ingest": {"POSTGRES_URI": true},
		"operator":      {"COORDINATOR_DATABASE_URI": true, "POSTGRES_URI": true},
		"migration":     {"MIGRATION_DATABASE_URI": true},
	}
	if !reflect.DeepEqual(classes, want) {
		t.Fatalf("Classes = %v, want %v", classes, want)
	}
	if foreign := Foreign([]string{"COORDINATOR_DATABASE_URI", "POSTGRES_URI"}, classes["sync"]); !reflect.DeepEqual(foreign, []string{"COORDINATOR_DATABASE_URI"}) {
		t.Fatalf("Foreign = %v", foreign)
	}
}

func TestClassesRefusesAnEmptyManifest(t *testing.T) {
	if _, err := Classes([]byte(`{"processes": []}`)); err == nil {
		t.Fatal("a manifest with no process must not yield an empty expectation")
	}
	if _, err := Classes([]byte(`{`)); err == nil {
		t.Fatal("a broken manifest must be refused")
	}
}

func TestWithEnvFileLetsAnEmptyEnvironmentValueWin(t *testing.T) {
	file := HostEnvFile([]byte("# DATABASE_URI=\"x\"\nPOSTGRES_URI=\"postgresql://a@h/db\"\nOTHER=1\n"))
	if file["DATABASE_URI"] == "" || file["POSTGRES_URI"] != "postgresql://a@h/db" {
		t.Fatalf("host env file = %v", file)
	}
	kept := Carried(WithEnvFile(file, map[string]string{"WORKER_DATABASE_URI": "w"}))
	if want := []string{"DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"}; !reflect.DeepEqual(kept, want) {
		t.Fatalf("without a blank the file's names reach the process: %v, want %v", kept, want)
	}
	blanked := Carried(WithEnvFile(file, map[string]string{"DATABASE_URI": ""}))
	if want := []string{"POSTGRES_URI"}; !reflect.DeepEqual(blanked, want) {
		t.Fatalf("an empty environment value must win over the file: %v, want %v", blanked, want)
	}
}
