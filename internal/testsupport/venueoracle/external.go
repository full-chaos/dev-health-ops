package venueoracle

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	valkeygo "github.com/valkey-io/valkey-go"
)

// ExternalStores are the datastores of a target that already runs (go-api or auth-service): the venue uses
// them as its Go side instead of starting containers, and the target owns their schema and roles. Each URI
// names the database the target uses; the Valkey URI is the database its rate limits count in.
type ExternalStores struct {
	PostgresURI, ClickHouseURI, ValkeyURI string
}

// Environment names shared with the session corpus (sessionscenario.Env*).
const (
	EnvExternalPostgres   = "DEVHEALTH_AUTH_ORACLE_POSTGRES_URI"
	EnvExternalClickHouse = "DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI"
	EnvExternalValkey     = "DEVHEALTH_AUTH_ORACLE_VALKEY_URI"
)

// Validate is an error unless the stores can stand in for the venue's Go side. frozen is false for a venue
// that runs Python: the Python plane needs its own copy of the stores, which an external target cannot give.
func (e ExternalStores) Validate(frozen bool) error {
	if !frozen {
		return fmt.Errorf("external stores need a frozen golden or a Go-only venue: a live Python plane needs its own copy")
	}
	if e.PostgresURI == "" || e.ClickHouseURI == "" || e.ValkeyURI == "" {
		return fmt.Errorf("external stores need all of %s, %s and %s", EnvExternalPostgres, EnvExternalClickHouse, EnvExternalValkey)
	}
	if _, err := e.PostgresDatabase(); err != nil {
		return err
	}
	if _, err := e.ClickHouseDatabase(); err != nil {
		return err
	}
	return nil
}

// PostgresDatabase is the database the Postgres URI names.
func (e ExternalStores) PostgresDatabase() (string, error) {
	config, err := pgx.ParseConfig(e.PostgresURI)
	if err != nil {
		return "", fmt.Errorf("postgres URI: %w", err)
	}
	if config.Database == "" {
		return "", fmt.Errorf("postgres URI names no database")
	}
	return config.Database, nil
}

// ClickHouseDatabase is the database the ClickHouse URI names.
func (e ExternalStores) ClickHouseDatabase() (string, error) {
	parsed, err := url.Parse(e.ClickHouseURI)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("clickhouse URI %q is not a URL with a host", e.ClickHouseURI)
	}
	database := strings.TrimPrefix(parsed.Path, "/")
	if database == "" {
		return "", fmt.Errorf("clickhouse URI names no database")
	}
	return database, nil
}

// ExternalFromEnv reads the stores from the environment; ok is false when none is set.
func ExternalFromEnv() (ExternalStores, bool) {
	e := ExternalStores{PostgresURI: os.Getenv(EnvExternalPostgres), ClickHouseURI: os.Getenv(EnvExternalClickHouse), ValkeyURI: os.Getenv(EnvExternalValkey)}
	return e, e != (ExternalStores{})
}

// startExternal builds a frozen venue whose Go side is the external stores: no container, no Python, no
// template copy, no roles. The seed and the minted tokens are the same as in a built venue.
func startExternal(t *testing.T, ctx context.Context, options Options, v *Venue) *Venue {
	t.Helper()
	stores := *options.External
	if err := stores.Validate(v.frozen); err != nil {
		t.Fatalf("venue: %v", err)
	}
	postgresDB, _ := stores.PostgresDatabase()
	clickHouseDB, _ := stores.ClickHouseDatabase()
	v.postgresURI, v.clickHouseURI, v.ValkeyURI, v.PythonValkeyURI = stores.PostgresURI, stores.ClickHouseURI, stores.ValkeyURI, stores.ValkeyURI
	v.SourceDB, v.GoDB, v.PythonClickHouseDB, v.GoClickHouseDB = postgresDB, postgresDB, clickHouseDB, clickHouseDB
	v.hostEnv = inheritedValues(map[string]string{})
	perRun := map[string]string{}
	for name, value := range v.hostEnv {
		perRun[name] = value
	}
	v.pythonEnv = pythonPlaneEnv(options, perRun)
	admin := OpenPool(t, ctx, stores.PostgresURI)
	var specs map[string]map[string]any
	if options.Seed != nil {
		specs = options.Seed(t, ctx, admin, v)
	}
	if len(specs) > 0 {
		v.Tokens = v.mintGo(t, options.JWTKey, specs)
	}
	return v
}

// limiterKeyPattern matches the keys of the go-api rate-limit store (ratelimitvalkey keyPrefix "dho:rl:").
const limiterKeyPattern = "dho:rl:*"

// ResetLimiterKeys deletes the rate-limit keys on the Valkey at uri and nothing else: an external target's
// Valkey may hold other data.
func ResetLimiterKeys(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var cursor uint64
	for {
		entry, err := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(limiterKeyPattern).Count(1000).Build()).AsScanEntry()
		if err != nil {
			t.Fatal(err)
		}
		if len(entry.Elements) > 0 {
			if err := client.Do(ctx, client.B().Del().Key(entry.Elements...).Build()).Error(); err != nil {
				t.Fatal(err)
			}
		}
		if cursor = entry.Cursor; cursor == 0 {
			return
		}
	}
}
