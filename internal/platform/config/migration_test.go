package config

import (
	"io"
	"testing"
)

// The migration database resolves to a DSN pgx parses: a SQLAlchemy
// driver-qualified scheme is rewritten to postgresql://, from either source,
// and nothing else in the DSN changes.
func TestResolveMigrationDatabaseNormalizesDriverSchemes(t *testing.T) {
	for _, testCase := range []struct {
		key, raw, want string
	}{
		{"POSTGRES_URI", "postgresql+asyncpg://u:p%40ss@h:5432/db?sslmode=require", "postgresql://u:p%40ss@h:5432/db?sslmode=require"},
		{"POSTGRES_URI", "POSTGRES+ASYNCPG://u:p@h/db", "postgresql://u:p@h/db"},
		{"MIGRATION_DATABASE_URI", "postgresql+psycopg2://u:p@h/db", "postgresql://u:p@h/db"},
		{"MIGRATION_DATABASE_URI", "postgresql+psycopg://u:p@h/db", "postgresql://u:p@h/db"},
		{"POSTGRES_URI", "postgresql://u:p@h/db", "postgresql://u:p@h/db"},
		{"POSTGRES_URI", "postgres://u:p@h/db", "postgres://u:p@h/db"},
	} {
		lookup := func(key string) (string, bool) {
			if key == testCase.key {
				return testCase.raw, true
			}
			return "", false
		}
		got, source, ok := ResolveMigrationDatabase(lookup, io.Discard, true)
		if !ok || source != testCase.key || got.Reveal() != testCase.want {
			t.Fatalf("%s=%q resolved to %q from %s (ok %v), want %q", testCase.key, testCase.raw, got.Reveal(), source, ok, testCase.want)
		}
	}
}
