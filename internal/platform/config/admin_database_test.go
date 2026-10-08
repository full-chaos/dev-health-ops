package config

import (
	"bytes"
	"strings"
	"testing"
)

// The admin users and orgs verbs take the first DSN configured of
// MIGRATION_DATABASE_URI, API_DATABASE_URI and POSTGRES_URI: a worker pod's
// POSTGRES_URI is the domain role, which cannot write users.
func TestResolveAdminDatabaseOrder(t *testing.T) {
	const (
		migration = "postgresql://mig@migration-host/db"
		api       = "postgresql://api@api-host/db"
		domain    = "postgresql://dom@domain-host/db"
	)
	for _, testCase := range []struct {
		name       string
		env        map[string]string
		wantSource string
		want       string
	}{
		{"migration wins over api and postgres", map[string]string{"MIGRATION_DATABASE_URI": migration, "API_DATABASE_URI": api, "POSTGRES_URI": domain}, "MIGRATION_DATABASE_URI", migration},
		{"migration wins over api", map[string]string{"MIGRATION_DATABASE_URI": migration, "API_DATABASE_URI": api}, "MIGRATION_DATABASE_URI", migration},
		{"api wins over postgres", map[string]string{"API_DATABASE_URI": api, "POSTGRES_URI": domain}, "API_DATABASE_URI", api},
		{"api alone", map[string]string{"API_DATABASE_URI": api}, "API_DATABASE_URI", api},
		{"api component form wins over postgres", map[string]string{"DEV_HEALTH_PG_API_HOST": "api-host", "DEV_HEALTH_PG_API_USER": "api", "DEV_HEALTH_PG_DB": "db", "POSTGRES_URI": domain}, "API_DATABASE_URI", "postgresql://api@api-host:5432/db"},
		{"api driver scheme normalized", map[string]string{"API_DATABASE_URI": "postgresql+asyncpg://api@api-host/db", "POSTGRES_URI": domain}, "API_DATABASE_URI", api},
		{"empty api is not configured", map[string]string{"MIGRATION_DATABASE_URI": "", "API_DATABASE_URI": "", "POSTGRES_URI": domain}, "POSTGRES_URI", domain},
		{"postgres alone", map[string]string{"POSTGRES_URI": domain}, "POSTGRES_URI", domain},
		{"blank api is not configured", map[string]string{"API_DATABASE_URI": "   ", "POSTGRES_URI": domain}, "POSTGRES_URI", domain},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			lookup := func(key string) (string, bool) { value, ok := testCase.env[key]; return value, ok }
			var stderr bytes.Buffer
			got, source, ok := ResolveAdminDatabase(lookup, &stderr)
			if !ok || source != testCase.wantSource || got.Reveal() != testCase.want {
				t.Fatalf("resolved %q from %s (ok %v, stderr %q), want %q from %s", got.Reveal(), source, ok, stderr.String(), testCase.want, testCase.wantSource)
			}
		})
	}
}

func TestResolveAdminDatabaseWithNoDSNNamesTheGoAPIPod(t *testing.T) {
	for _, env := range []map[string]string{{}, {"POSTGRES_URI": ""}, {"POSTGRES_URI": "   "}, {"API_DATABASE_URI": "  ", "POSTGRES_URI": " "}} {
		var stderr bytes.Buffer
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		_, source, ok := ResolveAdminDatabase(lookup, &stderr)
		if ok || !strings.Contains(stderr.String(), NoAdminDatabaseMessage) || !strings.Contains(NoAdminDatabaseMessage, "go-api pod") {
			t.Fatalf("env %q: resolved from %q (ok %v), stderr %q", env, source, ok, stderr.String())
		}
	}
}
