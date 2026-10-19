package venueoracle

import "testing"

// ExternalStores are the datastores of a target that already runs (go-api or auth-service): the venue uses
// them as its Go side instead of starting containers, and the target owns their schema and roles.
type ExternalStores struct {
	PostgresURI, ClickHouseURI, ValkeyURI string
}

// Validate is an error unless the stores can stand in for the venue's Go side of a frozen or Go-only venue.
func (e ExternalStores) Validate(frozen bool) error { return nil }

// ExternalFromEnv reads the stores from the environment; ok is false when none is set.
func ExternalFromEnv() (ExternalStores, bool) { return ExternalStores{}, false }

// Environment names shared with the session corpus (sessionscenario.Env*).
const (
	EnvExternalPostgres   = "DEVHEALTH_AUTH_ORACLE_POSTGRES_URI"
	EnvExternalClickHouse = "DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI"
	EnvExternalValkey     = "DEVHEALTH_AUTH_ORACLE_VALKEY_URI"
)

var _ = (*testing.T)(nil)
