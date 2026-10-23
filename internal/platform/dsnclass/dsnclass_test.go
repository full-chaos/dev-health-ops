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
		"DATABASE_URL":                       true,
		"POSTGRES_URL":                       true,
		"WORKER_DATABASE_URL_FILE":           true,
		"REDIS_URL":                          false,
		"CLICKHOUSE_URL":                     false,
		"APP_DATABASE_DSN":                   true,
		"PG_DSN":                             true,
		"DEV_HEALTH_TEST_POSTGRES_DSN":       true,
		"RIVER_COMPAT_PGBOUNCER_URL":         false,
		"UPGRADE_URL":                        false,
		"PGBOUNCER_URL":                      false,
		"MY_PG_URL":                          true,
		"PG_URL":                             true,
		"PG_DATABASE_URL":                    true,
		"SENTRY_DSN":                         false,
		"OTEL_EXPORTER_OTLP_ENDPOINT":        false,
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
		{"stream with a URL-form alias", Stream, []string{"DATABASE_URL=x"}, []string{"DATABASE_URL"}},
		{"control with a URL-form Postgres name", Control, []string{"POSTGRES_URL=x"}, []string{"POSTGRES_URL"}},
		{"river with a DSN-shaped name that is not an identifier", River, []string{"BAD NAME_DATABASE_URI=x"}, []string{"<invalid>"}},
		{"river with an asyncpg-scheme URL under an unrelated name", River, []string{"SOME_SETTING=postgresql+asyncpg://u:p@h/db"}, []string{"SOME_SETTING"}},
		{"stream with a psycopg2-scheme URL under an unrelated name", Stream, []string{"SOME_SETTING=POSTGRESQL+PSYCOPG2://u:p@h/db"}, []string{"SOME_SETTING"}},
		{"river with a URL under a name that is not an identifier", River, []string{"MY-DSN=postgres://u:p@h/db"}, []string{"<invalid>"}},
		{"river with a PG-token DSN name", River, []string{"MY_PG_DSN=x"}, []string{"MY_PG_DSN"}},
		{"river with a DSN-suffixed name", River, []string{"APP_DATABASE_DSN=x"}, []string{"APP_DATABASE_DSN"}},
		{"stream with a PG-named URL", Stream, []string{"PG_URL=x"}, []string{"PG_URL"}},
		{"river with a postgres URL under an unrelated name", River, []string{"SOME_SETTING=postgres://u:p@h/db"}, []string{"SOME_SETTING"}},
		{"stream with a postgresql URL in mixed case", Stream, []string{"X=PostgreSQL://u:p@h/db"}, []string{"X"}},
		{"control with a postgres URL under a foreign DSN name", Control, []string{"API_DATABASE_URI=postgres://u:p@h/db"}, []string{"API_DATABASE_URI"}},
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
		{"PG as a substring of another word is not a DSN name", River, []string{"UPGRADE_URL=https://example.test", "PGBOUNCER_URL=x"}},
		{"a non-postgres scheme under an unrelated name", River, []string{"SOME_SETTING=postgrex://u:p@h/db", "X=mysql://u:p@h/db"}},
		{"an allowed name may hold a postgres URL", Stream, []string{"POSTGRES_URI=postgres://u:p@h/db"}},
		{"an allowed name of the class with a postgresql URL", River, []string{"WORKER_DATABASE_URI=postgresql://u:p@h/db"}},
		{"an unrelated value that is not a postgres URL", River, []string{"SOME_SETTING=https://example.test", "X=postgres-like"}},
		{"an empty value under a postgres-looking name is absent", Stream, []string{"SOME_SETTING=", "DATABASE_URL="}},
		{"component-form variables are not matched", Stream, []string{"DEV_HEALTH_PG_QUEUE_HOST=h", "DEV_HEALTH_PG_COORDINATOR_PASSWORD=p"}},
		{"entries without a separator are ignored", River, []string{"COORDINATOR_DATABASE_URI"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			if err := CheckEnviron(row.class, row.environ); err != nil {
				t.Fatalf("CheckEnviron = %v, want nil", err)
			}
		})
	}
}
