package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	platformconfig "github.com/full-chaos/dev-health-ops/internal/platform/config"
	platformsecrets "github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func dsnTestLookup(values map[string]string) platformsecrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// TestResolveDSNRequiredUsesTheSharedComponentForm pins that this binary
// must never read POSTGRES_URI/WORKER_DATABASE_URI/
// COORDINATOR_DATABASE_URI/CLICKHOUSE_URI directly (bypassing
// config.ResolveDSN) -- doing so would mean a component-only deployment
// could never configure this CLI at all, unlike every long-running worker
// binary. resolveDSNRequired must behave identically to the shared
// implementation the other binaries use.
func TestResolveDSNRequiredUsesTheSharedComponentForm(t *testing.T) {
	t.Parallel()

	t.Run("pre-built URI only still works, unchanged", func(t *testing.T) {
		t.Parallel()
		value, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"POSTGRES_URI": "postgresql://app:app@db.internal:5432/appdb",
		}))
		if err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		if value.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
			t.Fatalf("got %q", value.Reveal())
		}
	})

	t.Run("component form only works", func(t *testing.T) {
		t.Parallel()
		value, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST":     "db.internal",
			"DEV_HEALTH_PG_DOMAIN_USER":     "app",
			"DEV_HEALTH_PG_DOMAIN_PASSWORD": "app",
			"DEV_HEALTH_PG_DB":              "appdb",
		}))
		if err != nil {
			t.Fatalf("expected success via the component form, got: %v", err)
		}
		if value.Reveal() != "postgresql://app:app@db.internal:5432/appdb" {
			t.Fatalf("got %q", value.Reveal())
		}
	})

	// resolveDSNRequired must never collapse this
	// error to a bare bool, discarding the key names ResolveDSN's own error
	// already names.
	t.Run("both forms set -- refused, naming both keys, not silently one winning", func(t *testing.T) {
		t.Parallel()
		_, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"POSTGRES_URI":              "postgresql://old:old@old.invalid:5432/old",
			"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "POSTGRES_URI") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("expected an error naming both keys, got: %v", err)
		}
	})

	t.Run("partial component set (no host) -- names the missing host key", func(t *testing.T) {
		t.Parallel()
		_, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_USER": "app",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_USER") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_PG_DOMAIN_HOST") {
			t.Fatalf("expected an error naming the missing host key, got: %v", err)
		}
	})

	t.Run("neither set -- required error, unchanged", func(t *testing.T) {
		t.Parallel()
		_, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(nil))
		if err == nil || !strings.Contains(err.Error(), "POSTGRES_URI") {
			t.Fatalf("expected a required-key error, got: %v", err)
		}
	})

	t.Run("clickhouse component form works through the same helper", func(t *testing.T) {
		t.Parallel()
		value, err := resolveDSNRequired("CLICKHOUSE_URI", platformconfig.ClickHouseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_CH_HOST": "ch.internal",
		}))
		if err != nil {
			t.Fatalf("expected success via the component form, got: %v", err)
		}
		if value.Reveal() != "clickhouse://ch.internal:9000/default" {
			t.Fatalf("got %q", value.Reveal())
		}
	})

	// ClickHouse's DB key is NOT shared (unlike the three
	// Postgres specs' DEV_HEALTH_PG_DB) and must still be swept.
	t.Run("clickhouse DB alone (no host) -- names the missing host key", func(t *testing.T) {
		t.Parallel()
		_, err := resolveDSNRequired("CLICKHOUSE_URI", platformconfig.ClickHouseSpec, dsnTestLookup(map[string]string{
			"DEV_HEALTH_CH_DB": "analytics",
		}))
		if err == nil ||
			!strings.Contains(err.Error(), "DEV_HEALTH_CH_DB") ||
			!strings.Contains(err.Error(), "DEV_HEALTH_CH_HOST") {
			t.Fatalf("expected an error naming DEV_HEALTH_CH_DB, got: %v", err)
		}
	})
}

// TestLogResolvedDatabaseReportsFormAndNameWithoutParsingAURI pins that
// configureRuntime and the two lazy ClickHouse
// dispatch paths must emit the resolution Info record every other
// entry point (config.Load's daemons, cmd/dev-health-worker-migrate)
// promises. A pre-built URI is never parsed for this
// purpose -- the URI form names only "form":"uri"; the component form's
// database name is read directly from the env, never derived from the
// assembled DSN, proven here by planting a decoy raw value with an
// unrelated database name in the same environment.
func TestLogResolvedDatabaseReportsFormAndNameWithoutParsingAURI(t *testing.T) {
	t.Parallel()

	t.Run("uri form names only the form, never a database", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		logResolvedDatabase(&stderr, dsnTestLookup(map[string]string{
			"POSTGRES_URI": "postgresql://app:app@db.internal:5432/appdb",
		}), platformconfig.DomainDatabaseSpec, "domain",
			platformsecrets.NewValue("postgresql://app:app@db.internal:5432/appdb"))
		out := stderr.String()
		if !strings.Contains(out, `"dsn":{"name":"domain","form":"uri"}`) {
			t.Fatalf("expected form=uri and no database field, got: %s", out)
		}
		if strings.Contains(out, `"database"`) || strings.Contains(out, "appdb") {
			t.Fatalf("expected no database field for the URI form, got: %s", out)
		}
	})

	t.Run("component form names the form and the db identifier read directly from the env", func(t *testing.T) {
		t.Parallel()
		var stderr bytes.Buffer
		logResolvedDatabase(&stderr, dsnTestLookup(map[string]string{
			"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
			"DEV_HEALTH_PG_DB":          "realdb",
			// Decoy: an unrelated raw URI naming a DIFFERENT database sits in
			// the same environment (as it would once ResolveDSN itself has
			// already rejected mixing forms) -- if logResolvedDatabase ever
			// took its name from anywhere but the resolved DSN, this value
			// would leak into the record instead of "realdb".
			"POSTGRES_URI": "postgresql://app:app@evil.invalid:5432/decoydb",
		}), platformconfig.DomainDatabaseSpec, "domain",
			platformsecrets.NewValue("postgresql://app:app@db.internal:5432/realdb"))
		out := stderr.String()
		if !strings.Contains(out, `"form":"components"`) {
			t.Fatalf("expected form=components, got: %s", out)
		}
		if !strings.Contains(out, `"database":"realdb"`) {
			t.Fatalf("expected database=realdb, got: %s", out)
		}
		if strings.Contains(out, "decoydb") || strings.Contains(out, "evil.invalid") {
			t.Fatalf("logResolvedDatabase parsed the decoy URI instead of reading the env directly: %s", out)
		}
	})
}

// TestConfigureRuntimeLogsAResolutionRecordForEachDSN proves the wiring
// (not just the helper in isolation): configureRuntime must call
// logResolvedDatabase for all three
// PostgreSQL DSNs it resolves, before it ever reaches OpenRuntimePools. An
// unreachable domain/queue/coordinator host (127.0.0.1:1, refused
// immediately) keeps this test fast and deterministic while still proving
// the Info lines are emitted on the path to a real failure, not only on a
// success this unit test cannot cheaply construct.
func TestConfigureRuntimeLogsAResolutionRecordForEachDSN(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	_, code := configureRuntime(ctx, dsnTestLookup(map[string]string{
		"POSTGRES_URI":             "postgresql://app:app@127.0.0.1:1/appdb",
		"WORKER_DATABASE_URI":      "postgresql://app:app@127.0.0.1:1/appdb",
		"COORDINATOR_DATABASE_URI": "postgresql://app:app@127.0.0.1:1/appdb",
	}), &stderr)
	_ = code
	out := stderr.String()
	for _, want := range []string{
		`"dsn":{"name":"domain","form":"uri"}`,
		`"dsn":{"name":"queue","form":"uri"}`,
		`"dsn":{"name":"coordinator","form":"uri"}`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %s in stderr, got: %s", want, out)
		}
	}
}

// TestConfigureRuntimePGBouncerModeErrorUsesTheSharedWriter pins that an
// invalid PGBOUNCER_TRANSACTION_MODE must go through the shared
// writeConfigError, never a bare {"code":"configuration_error"} via the
// old writeError, which named no offending key -- an operator needs to
// know PGBOUNCER_TRANSACTION_MODE
// itself was the offending key.
func TestConfigureRuntimePGBouncerModeErrorUsesTheSharedWriter(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	_, code := configureRuntime(context.Background(), dsnTestLookup(map[string]string{
		"POSTGRES_URI":               "postgresql://app:app@127.0.0.1:1/appdb",
		"WORKER_DATABASE_URI":        "postgresql://app:app@127.0.0.1:1/appdb",
		"COORDINATOR_DATABASE_URI":   "postgresql://app:app@127.0.0.1:1/appdb",
		"WORKER_OPERATOR_TOKEN":      "op-token",
		"PGBOUNCER_TRANSACTION_MODE": "not-a-bool",
	}), &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	out := stderr.String()
	if !strings.Contains(out, `"code":"configuration_error"`) {
		t.Fatalf("expected the stable configuration_error code, got: %s", out)
	}
	if !strings.Contains(out, "PGBOUNCER_TRANSACTION_MODE") {
		t.Fatalf("expected the detail to name PGBOUNCER_TRANSACTION_MODE, got: %s", out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	lastLine := lines[len(lines)-1]
	if !json.Valid([]byte(lastLine)) {
		t.Fatalf("expected the final line to be valid JSON, got: %s", lastLine)
	}
}

// TestWriteConfigErrorExposesKeyNamesNeverValues pins the CLI boundary
// rule: writeConfigError must keep the stable
// "configuration_error" JSON code (an operator script parsing it must not
// break) while adding a `detail` field carrying the resolver's key-named
// diagnostic -- and that detail must never contain a resolved value, only
// key names, since every ResolveDSN/ResolveDSNFromComponents error is
// built purely from ComponentSpec's own key-name strings.
func TestWriteConfigErrorExposesKeyNamesNeverValues(t *testing.T) {
	t.Parallel()

	_, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
		"POSTGRES_URI":              "postgresql://old:s3cr3t-password@old.invalid:5432/old",
		"DEV_HEALTH_PG_DOMAIN_HOST": "db.internal",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}

	var stderr bytes.Buffer
	status := writeConfigError(&stderr, err)
	if status != 1 {
		t.Fatalf("status = %d, want 1", status)
	}
	out := stderr.String()
	if !strings.Contains(out, `"code":"configuration_error"`) {
		t.Fatalf("expected the stable configuration_error code, got: %s", out)
	}
	if !strings.Contains(out, "POSTGRES_URI") || !strings.Contains(out, "DEV_HEALTH_PG_DOMAIN_HOST") {
		t.Fatalf("expected the detail to name both keys, got: %s", out)
	}
	if strings.Contains(out, "s3cr3t-password") {
		t.Fatalf("detail must never contain a resolved value, got: %s", out)
	}
}

// TestWriteConfigErrorNeverEchoesACredentialBearingFilePath pins the CLI
// boundary rule: a *_FILE var misconfigured to a
// raw credential string instead of an actual path must never have that
// entire string, password included, echoed on stderr -- Go's
// os.PathError.Error() embeds the exact path it tried to open, so
// writeConfigError must never print err.Error() unfiltered.
func TestWriteConfigErrorNeverEchoesACredentialBearingFilePath(t *testing.T) {
	t.Parallel()

	syntheticSecret := "s3cr3t-p@ssw0rd-should-never-appear"
	misconfiguredPath := "postgresql://svc:" + syntheticSecret + "@internal.example:5432/db"

	_, err := resolveDSNRequired("POSTGRES_URI", platformconfig.DomainDatabaseSpec, dsnTestLookup(map[string]string{
		"DEV_HEALTH_PG_DOMAIN_HOST":      "db.internal",
		"DEV_HEALTH_PG_DOMAIN_USER_FILE": misconfiguredPath,
	}))
	if err == nil {
		t.Fatal("expected a file-read failure")
	}

	var stderr bytes.Buffer
	writeConfigError(&stderr, err)
	out := stderr.String()
	if strings.Contains(out, syntheticSecret) || strings.Contains(out, misconfiguredPath) {
		t.Fatalf("the misconfigured, credential-bearing path leaked on stderr: %s", out)
	}
	if !strings.Contains(out, "DEV_HEALTH_PG_DOMAIN_USER_FILE") {
		t.Fatalf("expected the key name to still be named, got: %s", out)
	}
}

// TestWriteConfigErrorAlwaysProducesValidJSON pins that writeConfigError
// must produce valid JSON regardless of content: `%q` is Go string
// escaping, not JSON escaping -- a control byte
// (here U+0001) or an embedded quote/backslash in the underlying text
// produces invalid JSON (`\x01` is not a legal JSON escape) unless the
// payload is built with encoding/json, which escapes correctly
// regardless of content. This constructs the error
// DIRECTLY (bypassing resolveDSNRequired) so the case is not defeated by
// secrets.Resolve's own fix, which no longer lets a raw file path
// (control characters included) reach any error message at all -- this
// test exercises writeConfigError's own escaping in isolation.
func TestWriteConfigErrorAlwaysProducesValidJSON(t *testing.T) {
	t.Parallel()

	for name, message := range map[string]string{
		"control character": "DEV_HEALTH_PG_DOMAIN_USER_FILE could not be read: byte \x01 present",
		"embedded quote":    `DEV_HEALTH_PG_DOMAIN_USER_FILE: unexpected "quoted" token`,
		"backslash":         `DEV_HEALTH_PG_DOMAIN_USER_FILE: path has a trailing \`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var stderr bytes.Buffer
			writeConfigError(&stderr, errors.New(message))
			if !json.Valid(stderr.Bytes()) {
				t.Fatalf("writeConfigError produced invalid JSON: %s", stderr.String())
			}
		})
	}
}
