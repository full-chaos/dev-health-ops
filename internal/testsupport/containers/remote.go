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

// scratchNamePattern re-validates the FULL generated database name immediately
// before it reaches a DDL statement.
//
// The name is already built from a validated prefix plus crypto/rand hex, so
// this cannot fail today. It is here because neither engine can parameterise an
// identifier in DDL -- `CREATE DATABASE $1` is not a thing -- so interpolation
// is unavoidable and the guarantee has to be enforced at the point of use
// rather than inferred from how the value was constructed several calls back.
// Same shape as riverSchemaPattern in internal/joboutbox.
var scratchNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// assertSafeIdentifier fails closed on anything that is not a plain identifier.
func assertSafeIdentifier(database string) error {
	if !scratchNamePattern.MatchString(database) {
		return fmt.Errorf("refusing to build DDL for unsafe database name %q", database)
	}
	return nil
}

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

// databaseQueryKeys are the query parameters that select a database by another
// name than the URL path. Both drivers give these PRECEDENCE over the path, so
// rewriting only the path is not enough to redirect a DSN.
//
// This is the sharpest edge in this file. A DSN like
// `clickhouse://host:9000/default?database=dh_0830` rewritten to
// `.../scratch_x?database=dh_0830` still resolves to dh_0830 -- verified
// against the driver's own parser -- so a suite would run against the shared
// server's REAL data instead of its scratch database, and the scratch drop
// would then delete an empty database while the real one carried the writes.
var databaseQueryKeys = []string{"database", "dbname"}

// postgresURLSchemes are the two prefixes libpq -- and therefore pgx -- treats
// as URL form. Anything else is keyword/value form, which url.Parse cannot read.
var postgresURLSchemes = []string{"postgres://", "postgresql://"}

// withPostgresDatabase points a PostgreSQL DSN at database, in whichever of the
// two forms pgx accepts.
//
// url.Parse alone is not enough, and it fails in two different ways. Given
// `host=... dbname=acr application_name=x%zz` it errors on the invalid escape;
// given a plain `host=... dbname=acr` it SUCCEEDS and hands the whole string
// back as an opaque path, so rewriting the path yields `/scratch_x` -- a DSN
// that has silently lost the host, the user and the password. Both were
// observed directly against the parsers.
func withPostgresDatabase(dsn string, database string) (string, error) {
	for _, scheme := range postgresURLSchemes {
		if strings.HasPrefix(dsn, scheme) {
			return withDatabaseURL(dsn, database)
		}
	}
	// Keyword/value form. libpq resolves duplicate keys last-wins and pgx
	// follows it, so appending is a complete override -- verified with
	// pgx.ParseConfig, which reports Database="scratch_x" for a DSN naming
	// dbname twice. The name has already passed assertSafeIdentifier, so it
	// cannot contain a space or quote that would break the encoding.
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", fmt.Errorf("parse %s as a PostgreSQL DSN: %w", PostgresDSNEnv, err)
	}
	return dsn + " dbname=" + database, nil
}

// withDatabaseURL returns a URL-form dsn addressed at database instead of
// whatever database it currently names, by both path AND query parameter.
func withDatabaseURL(dsn string, database string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if parsed.Scheme == "" {
		// Without this, url.Parse quietly accepts a non-URL DSN and returns it
		// as an opaque path; the rewrite below would then discard everything
		// in it except the database name.
		return "", fmt.Errorf("DSN is not in URL form (no scheme): %q", dsn)
	}
	parsed.Path = "/" + database
	query := parsed.Query()
	for _, key := range databaseQueryKeys {
		if query.Has(key) {
			// Overwrite rather than delete: an explicit value keeps the
			// resulting DSN unambiguous whichever precedence a driver applies.
			query.Set(key, database)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// postgresScratchDDL returns the CREATE and DROP statements for a scratch
// database. Identifiers cannot be parameterised in DDL, so the name is quoted
// with the driver's own pgx.Identifier and has already passed
// assertSafeIdentifier at the call site.
func postgresScratchDDL(database string) (createStatement string, dropStatement string) {
	quoted := pgx.Identifier{database}.Sanitize()
	return "CREATE DATABASE " + quoted, "DROP DATABASE IF EXISTS " + quoted + " WITH (FORCE)"
}

// clickHouseScratchDDL is the ClickHouse counterpart. ClickHouse quotes
// identifiers with backticks and, like PostgreSQL, cannot parameterise one in
// DDL. assertSafeIdentifier at the call site is what makes this safe -- its
// pattern excludes the backtick entirely.
func clickHouseScratchDDL(database string) (createStatement string, dropStatement string) {
	return "CREATE DATABASE `" + database + "`", "DROP DATABASE IF EXISTS `" + database + "`"
}

// startPostgresRemote creates a scratch database on the server named by dsn
// and returns an Instance addressed at it. Close drops it again.
func startPostgresRemote(ctx context.Context, dsn string) (*Instance, error) {
	database, err := scratchName()
	if err != nil {
		return nil, err
	}
	if err := assertSafeIdentifier(database); err != nil {
		return nil, err
	}
	// pgx.Identifier.Sanitize is the driver's own identifier quoting; the
	// statement is built here rather than concatenated at the Exec call so the
	// query passed to the driver is a single prepared value.
	createStatement, dropStatement := postgresScratchDDL(database)

	// Rewrite the DSN BEFORE creating anything. Done afterwards, a DSN this
	// rewrite cannot handle leaves the database created with no Instance to
	// hold its cleanup closure -- an orphan whose only trace is a "created"
	// log line with no matching "dropped".
	uri, err := withPostgresDatabase(dsn, database)
	if err != nil {
		return nil, err
	}

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to remote PostgreSQL (%s): %w", PostgresDSNEnv, err)
	}
	defer func() { _ = admin.Close(ctx) }()

	// Announce the name BEFORE attempting the CREATE. The server can commit
	// the statement and the client still lose the response to a timeout or a
	// dropped connection; logging only on success would leave that database
	// with no name anywhere, invisible to the prefix sweep. Findability is the
	// whole design here -- the race itself cannot be closed, because there is
	// no way to create and register atomically across a network.
	logScratch("creating", "postgres", database)
	if _, err := admin.Exec(ctx, createStatement); err != nil {
		return nil, fmt.Errorf("create scratch database %q: %w", database, err)
	}
	logScratch("created", "postgres", database)

	return &Instance{
		URI: uri,
		cleanup: func(ctx context.Context) error {
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				// Most callers discard Instance.Close's error, so returning it
				// is not enough to make an orphan discoverable. Name it here.
				logScratch("ORPHANED, connect failed", "postgres", database)
				return fmt.Errorf("connect to drop scratch database %q: %w", database, err)
			}
			defer func() { _ = conn.Close(ctx) }()
			// FORCE terminates any connection the suite left behind; without
			// it a leaked pool makes DROP block and the scratch database
			// survives the run it belonged to.
			if _, err := conn.Exec(ctx, dropStatement); err != nil {
				logScratch("ORPHANED, drop failed", "postgres", database)
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
	if err := assertSafeIdentifier(database); err != nil {
		return nil, err
	}
	// ClickHouse quotes identifiers with backticks and, like PostgreSQL,
	// cannot parameterise one in DDL. assertSafeIdentifier above is what makes
	// this interpolation safe; the pattern excludes the backtick entirely.
	createStatement, dropStatement := clickHouseScratchDDL(database)

	options, err := clickhousego.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", ClickHouseDSNEnv, err)
	}

	// Rewrite the optional HTTP DSN BEFORE creating anything. Doing it after
	// the CREATE would leak the database on a malformed value: the error
	// returns without an Instance, so nothing holds the cleanup closure and
	// nothing drops what was just created.
	uri, err := withDatabaseURL(dsn, database)
	if err != nil {
		return nil, err
	}
	var httpURI string
	if httpDSN != "" {
		httpURI, err = withDatabaseURL(httpDSN, database)
		if err != nil {
			return nil, fmt.Errorf("rewrite %s: %w", ClickHouseHTTPDSNEnv, err)
		}
	}

	admin, err := clickhousego.Open(options)
	if err != nil {
		return nil, fmt.Errorf("connect to remote ClickHouse (%s): %w", ClickHouseDSNEnv, err)
	}
	defer func() { _ = admin.Close() }()

	// Announced before the attempt for the same reason as the PostgreSQL path.
	logScratch("creating", "clickhouse", database)
	if err := admin.Exec(ctx, createStatement); err != nil {
		return nil, fmt.Errorf("create scratch ClickHouse database %q: %w", database, err)
	}
	logScratch("created", "clickhouse", database)

	instance := &Instance{
		URI: uri,
		cleanup: func(ctx context.Context) error {
			conn, err := clickhousego.Open(options)
			if err != nil {
				logScratch("ORPHANED, connect failed", "clickhouse", database)
				return fmt.Errorf("connect to drop scratch ClickHouse database %q: %w", database, err)
			}
			defer func() { _ = conn.Close() }()
			if err := conn.Exec(ctx, dropStatement); err != nil {
				logScratch("ORPHANED, drop failed", "clickhouse", database)
				return fmt.Errorf("drop scratch ClickHouse database %q: %w", database, err)
			}
			logScratch("dropped", "clickhouse", database)
			return nil
		},
	}
	instance.httpURI = httpURI
	return instance, nil
}
