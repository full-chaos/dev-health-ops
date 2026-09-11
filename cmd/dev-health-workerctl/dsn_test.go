package main

import (
	"testing"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func dsnTestLookup(values map[string]string) platformsecrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// TestResolveDSNRequiredUsesTheSharedComponentForm is round-2's (2026-09-11)
// P1 fix: this binary used to read POSTGRES_URI/WORKER_DATABASE_URI/
// COORDINATOR_DATABASE_URI/CLICKHOUSE_URI directly via resolveRequired,
// bypassing config.ResolveDSN entirely -- so a component-only deployment
// could never configure this CLI at all, unlike every long-running worker
// binary. resolveDSNRequired must behave identically to the shared
// implementation the other binaries use.
func TestResolveDSNRequiredUsesTheSharedComponentForm(t *testing.T) {
	t.Parallel()

	t.Run("pre-built URI only still works, unchanged", func(t *testing.T) {
		t.Parallel()
		value, ok := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"POSTGRES_URI": "postgresql://app:app@db.internal:5432/appdb",
		}))
		if !ok {
			t.Fatal("expected success")
		}
		if value.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
			t.Fatalf("got %q", value.Reveal())
		}
	})

	t.Run("component form only now works -- the round-2 fix", func(t *testing.T) {
		t.Parallel()
		value, ok := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
			"DEV_HEALTH_PG_DOMAIN_USER":     "app",
			"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
			"DEV_HEALTH_PG_DB":              "appdb",
		}))
		if !ok {
			t.Fatal("expected success via the component form")
		}
		if value.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
			t.Fatalf("got %q", value.Reveal())
		}
	})

	t.Run("both forms set -- refused, not silently one winning", func(t *testing.T) {
		t.Parallel()
		_, ok := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"POSTGRES_URI":              "postgresql://old:old@old.invalid:5432/old",
			"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
		}))
		if ok {
			t.Fatal("expected failure when both forms are set")
		}
	})

	t.Run("neither set -- required error, unchanged", func(t *testing.T) {
		t.Parallel()
		_, ok := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(nil))
		if ok {
			t.Fatal("expected failure when neither form is set")
		}
	})

	t.Run("clickhouse component form works through the same helper", func(t *testing.T) {
		t.Parallel()
		value, ok := resolveDSNRequired("CLICKHOUSE_URI", platformconfig.ClickHouseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_CH_HOST": "ch.internal",
		}))
		if !ok {
			t.Fatal("expected success via the component form")
		}
		if value.Reveal() != "clickhouse://ch.internal:9000/default" {
			t.Fatalf("got %q", value.Reveal())
		}
	})
}
