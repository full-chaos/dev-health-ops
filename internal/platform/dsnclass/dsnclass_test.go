package dsnclass

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestSecretNamePattern(t *testing.T) {
	for name, want := range map[string]bool{
		"POSTGRES_URI":                       true,
		"WORKER_DATABASE_URI":                true,
		"COORDINATOR_DATABASE_URI":           true,
		"MIGRATION_DATABASE_URI_FILE":        true,
		"GO_API_REGISTRY_POSTGRES_URI":       true,
		"AUTH_DATABASE_URI":                  true,
		"DATABASE_URI":                       true,
		"CLICKHOUSE_URI":                     false,
		"VALKEY_URI":                         false,
		"SETTINGS_ENCRYPTION_KEY":            false,
		"RIVER_DATABASE_SCHEMA":              false,
		"DEV_HEALTH_PG_QUEUE_HOST":           false,
		"DEV_HEALTH_PG_COORDINATOR_PASSWORD": false,
	} {
		if _, got := Secret(name); got != want {
			t.Errorf("Secret(%q) matched = %v, want %v", name, got, want)
		}
	}
	if base, _ := Secret("WORKER_DATABASE_URI_FILE"); base != "WORKER_DATABASE_URI" {
		t.Errorf("a _FILE name is judged by its base, got %q", base)
	}
}

func TestAllowedSets(t *testing.T) {
	if got := Allowed(Stream); !slices.Equal(got, []string{"POSTGRES_URI"}) {
		t.Errorf("stream allowed set = %v", got)
	}
	if Allowed("nonexistent") != nil {
		t.Error("an unknown class must have no allowed DSN secrets")
	}
	got := Allowed(River)
	got[0] = "MUTATED"
	if Allowed(River)[0] == "MUTATED" {
		t.Error("Allowed must return a copy")
	}
}

func TestCheckEnvironRefusesEveryClassForeignDSN(t *testing.T) {
	for _, row := range []struct {
		name    string
		class   string
		environ []string
		want    []string
	}{
		{"river with the coordinator DSN", River, []string{"POSTGRES_URI=a", "WORKER_DATABASE_URI=b", "COORDINATOR_DATABASE_URI=c"}, []string{"COORDINATOR_DATABASE_URI"}},
		{"river with the coordinator DSN file", River, []string{"COORDINATOR_DATABASE_URI_FILE=/run/s"}, []string{"COORDINATOR_DATABASE_URI_FILE"}},
		{"river with an unlisted DSN-shaped name", River, []string{"ANALYTICS_DATABASE_URI=x"}, []string{"ANALYTICS_DATABASE_URI"}},
		{"river with a legacy alias", River, []string{"DATABASE_URI=x"}, []string{"DATABASE_URI"}},
		{"control with the API DSN", Control, []string{"API_DATABASE_URI=x", "COORDINATOR_DATABASE_URI=c"}, []string{"API_DATABASE_URI"}},
		{"stream with the queue-control DSN", Stream, []string{"POSTGRES_URI=a", "WORKER_DATABASE_URI=b"}, []string{"WORKER_DATABASE_URI"}},
		{"stream with both", Stream, []string{"WORKER_DATABASE_URI=b", "COORDINATOR_DATABASE_URI=c"}, []string{"COORDINATOR_DATABASE_URI", "WORKER_DATABASE_URI"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := CheckEnviron(row.class, row.environ)
			var refusal *Refusal
			if !errors.As(err, &refusal) {
				t.Fatalf("CheckEnviron = %v, want a refusal", err)
			}
			if refusal.Class != row.class || !slices.Equal(refusal.Names, row.want) {
				t.Fatalf("refusal = %+v, want class %s names %v", refusal, row.class, row.want)
			}
			for _, entry := range row.environ {
				if _, value, _ := strings.Cut(entry, "="); value != "" && strings.Contains(err.Error(), value) && len(value) > 1 {
					t.Fatalf("the refusal text carries a value: %q", err.Error())
				}
			}
		})
	}
}

func TestCheckEnvironAcceptsWhatTheClassMayHold(t *testing.T) {
	for _, row := range []struct {
		name    string
		class   string
		environ []string
	}{
		{"river", River, []string{"POSTGRES_URI=a", "WORKER_DATABASE_URI=b", "CLICKHOUSE_URI=c", "VALKEY_URI=d", "PATH=/bin"}},
		{"control", Control, []string{"POSTGRES_URI=a", "WORKER_DATABASE_URI=b", "COORDINATOR_DATABASE_URI=c"}},
		{"stream", Stream, []string{"POSTGRES_URI=a", "CLICKHOUSE_URI=c"}},
		{"an empty value counts as absent", Stream, []string{"POSTGRES_URI=a", "COORDINATOR_DATABASE_URI=", "WORKER_DATABASE_URI="}},
		{"component-form variables are not matched", Stream, []string{"DEV_HEALTH_PG_QUEUE_HOST=h", "DEV_HEALTH_PG_COORDINATOR_PASSWORD=p"}},
		{"entries without a separator are ignored", River, []string{"COORDINATOR_DATABASE_URI"}},
		{"a name that is not an identifier is ignored", River, []string{"BAD NAME_DATABASE_URI=x"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			if err := CheckEnviron(row.class, row.environ); err != nil {
				t.Fatalf("CheckEnviron = %v, want nil", err)
			}
		})
	}
}
