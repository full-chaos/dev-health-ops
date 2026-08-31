// Remote datastore targets for the integration harness (CHAOS-4428).
//
// WHY THIS EXISTS. Every Go integration suite in this repository obtains its
// PostgreSQL and ClickHouse dependency from StartPostgres/StartClickHouse in
// harness.go, which start a container through Docker. That is 164 test files
// funnelling through one helper, and on the development Mac it is the single
// largest consumer of Docker's CPU allocation. Pointing the harness at
// datastores that already exist -- the kiac cluster's in-namespace PostgreSQL
// and ClickHouse -- removes that load without touching a single test file.
//
// ISOLATION. A container gave each caller a private server. A shared server
// cannot, so this path gives each caller a private DATABASE on that server
// instead, created on entry and dropped by Instance.Close. The name is unique
// per call, not per lane: packages run in parallel and a single package may
// call StartPostgres more than once, so a lane-wide database would reintroduce
// exactly the cross-test interference the container gave us for free.
//
// NOT VALKEY. StartValkey deliberately has no remote path. Valkey has no
// CREATE DATABASE to carve a private namespace out of a shared server -- only
// 16 fixed numeric slots, which cannot cover parallel packages -- and suites
// that FLUSHDB would silently destroy a neighbour's state. Only 11 of the 164
// files touch Valkey and its container is by far the cheapest of the three, so
// the isolation risk buys almost no CPU back. It stays on testcontainers.
package containers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
)

// scratchLog receives one line per scratch database created and dropped.
//
// This is the telemetry a test harness can actually carry: there is no metrics
// registry in a `go test` process, and the operational question this path
// raises is not a rate but an identity -- WHICH databases exist on the shared
// server and did each one get dropped. A run killed between create and drop
// leaves an orphan, and this log is what names it for the sweep. It is a
// variable so the emission itself can be asserted rather than assumed.
var scratchLog io.Writer = os.Stderr

func logScratch(action string, engine string, database string) {
	fmt.Fprintf(scratchLog, "containers: %s scratch %s database %q\n", action, engine, database)
}

// Environment variables that select a remote datastore instead of a container.
// Each holds a DSN for a server the caller may create and drop databases on;
// the database named in the DSN is used only to connect, never written to.
const (
	PostgresDSNEnv       = "DEV_HEALTH_TEST_POSTGRES_DSN"
	ClickHouseDSNEnv     = "DEV_HEALTH_TEST_CLICKHOUSE_DSN"
	ClickHouseHTTPDSNEnv = "DEV_HEALTH_TEST_CLICKHOUSE_HTTP_DSN"

	// ScratchPrefixEnv namespaces this lane's scratch databases so a stuck or
	// killed run's leftovers can be identified and swept per lane rather than
	// by a pattern that would also match a concurrent lane's live databases.
	ScratchPrefixEnv = "DEV_HEALTH_TEST_SCRATCH_PREFIX"

	defaultScratchPrefix = "dh_t"
)

// scratchPrefixPattern keeps a lane prefix inside the identifier rules both
// engines share, so the prefix can be interpolated without quoting games.
var scratchPrefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,23}$`)

func remoteDSN(env string) string {
	return strings.TrimSpace(os.Getenv(env))
}

// scratchPrefix returns the configured lane prefix, rejecting anything that is
// not a plain lowercase identifier. A prefix reaches a CREATE DATABASE
// statement, so validating it here is what keeps that interpolation safe.
func scratchPrefix() (string, error) {
	prefix := strings.TrimSpace(os.Getenv(ScratchPrefixEnv))
	if prefix == "" {
		return defaultScratchPrefix, nil
	}
	if !scratchPrefixPattern.MatchString(prefix) {
		return "", fmt.Errorf(
			"%s=%q is not a valid scratch prefix: want lowercase letter followed by up to 23 letters, digits or underscores",
			ScratchPrefixEnv, prefix,
		)
	}
	return prefix, nil
}

// scratchName builds a unique database name. The random suffix -- not a
// counter or a timestamp -- is what makes two lanes pointed at the SAME server
// safe to run concurrently without coordinating.
func scratchName() (string, error) {
	prefix, err := scratchPrefix()
	if err != nil {
		return "", err
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generate scratch database suffix: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(suffix), nil
}

// withDatabasePath returns dsn addressed at database instead of whatever
// database it currently names.
func withDatabasePath(dsn string, database string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	parsed.Path = "/" + database
	return parsed.String(), nil
}

// startPostgresRemote creates a scratch database on the server named by dsn
// and returns an Instance addressed at it. Close drops it again.
func startPostgresRemote(ctx context.Context, dsn string) (*Instance, error) {
	database, err := scratchName()
	if err != nil {
		return nil, err
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to remote PostgreSQL (%s): %w", PostgresDSNEnv, err)
	}
	defer func() { _ = admin.Close(ctx) }()

	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+database+`"`); err != nil {
		return nil, fmt.Errorf("create scratch database %q: %w", database, err)
	}
	logScratch("created", "postgres", database)

	uri, err := withDatabasePath(dsn, database)
	if err != nil {
		return nil, err
	}
	return &Instance{
		URI: uri,
		cleanup: func(ctx context.Context) error {
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return fmt.Errorf("connect to drop scratch database %q: %w", database, err)
			}
			defer func() { _ = conn.Close(ctx) }()
			// FORCE terminates any connection the suite left behind; without
			// it a leaked pool makes DROP block and the scratch database
			// survives the run it belonged to.
			if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`); err != nil {
				return fmt.Errorf("drop scratch database %q: %w", database, err)
			}
			logScratch("dropped", "postgres", database)
			return nil
		},
	}, nil
}

// startClickHouseRemote creates a scratch database on the server named by dsn.
// httpDSN, when set, is rewritten to the same scratch database so
// ClickHouseHTTPDSN can serve the HTTP-speaking Python migration runner.
func startClickHouseRemote(ctx context.Context, dsn string, httpDSN string) (*Instance, error) {
	database, err := scratchName()
	if err != nil {
		return nil, err
	}
	options, err := clickhousego.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", ClickHouseDSNEnv, err)
	}
	admin, err := clickhousego.Open(options)
	if err != nil {
		return nil, fmt.Errorf("connect to remote ClickHouse (%s): %w", ClickHouseDSNEnv, err)
	}
	defer func() { _ = admin.Close() }()

	if err := admin.Exec(ctx, "CREATE DATABASE `"+database+"`"); err != nil {
		return nil, fmt.Errorf("create scratch ClickHouse database %q: %w", database, err)
	}
	logScratch("created", "clickhouse", database)

	uri, err := withDatabasePath(dsn, database)
	if err != nil {
		return nil, err
	}
	instance := &Instance{
		URI: uri,
		cleanup: func(ctx context.Context) error {
			conn, err := clickhousego.Open(options)
			if err != nil {
				return fmt.Errorf("connect to drop scratch ClickHouse database %q: %w", database, err)
			}
			defer func() { _ = conn.Close() }()
			if err := conn.Exec(ctx, "DROP DATABASE IF EXISTS `"+database+"`"); err != nil {
				return fmt.Errorf("drop scratch ClickHouse database %q: %w", database, err)
			}
			logScratch("dropped", "clickhouse", database)
			return nil
		},
	}
	if httpDSN != "" {
		httpURI, err := withDatabasePath(httpDSN, database)
		if err != nil {
			return nil, fmt.Errorf("rewrite %s: %w", ClickHouseHTTPDSNEnv, err)
		}
		instance.httpURI = httpURI
	}
	return instance, nil
}
