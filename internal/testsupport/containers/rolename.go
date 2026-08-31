// Cluster-scoped test object naming (CHAOS-4661).
//
// WHY THIS EXISTS. A scratch database (remote.go) isolates TABLES: each
// caller gets its own, created on entry and dropped on Close. It does not
// isolate roles, tablespaces or event triggers -- those live in the cluster,
// which is shared across every caller and every concurrent lane pointed at
// the same kiac server. A test file that hard-codes `CREATE ROLE some_name`
// is fine against a fresh container (nothing else has ever created that
// role) and unsafe against a shared cluster: a second run of the same suite
// fails with `role "some_name" already exists (SQLSTATE 42710)`, and two
// concurrent lanes race on the same name even if one drops it first.
//
// The fix is per-test, not a harness change (CHAOS-4661, ops #2044's
// "why not a harness change" section): the harness cannot rename a role a
// test hard-codes as a Go constant, and having it enumerate/drop unknown
// roles on a shared server would be far more dangerous than the collision it
// solves. So every test that creates a cluster-scoped object derives its
// name from something already unique per call, instead of a literal.
//
// WHERE THE UNIQUENESS COMES FROM. Instance.URI already names a database
// that is unique per call on the remote/kiac path (remote.go's scratchName,
// crypto/rand-suffixed) and fixed but harmless to repeat on the container
// path (a fresh container has no pre-existing roles either way). Reusing
// that identity -- rather than generating a second, independent random
// value -- means a role name and the scratch database it was created
// alongside can be correlated from a single log line, and it costs nothing
// extra: the harness already generated and validated it.
package containers

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5"
)

// DatabaseName returns the PostgreSQL database name instance's URI actually
// addresses: the fixed "worker_test" database on the container path, or the
// unique per-call scratch database on the remote/kiac path (remote.go). A
// test that hard-codes "worker_test" in a GRANT/REVOKE .. ON DATABASE
// statement is making the same mistake CHAOS-4661 found for role names, one
// level down -- it happens to be harmless against a container (where
// "worker_test" is real) and breaks against kiac (where it never exists).
func DatabaseName(uri string) (string, error) {
	config, err := pgx.ParseConfig(uri)
	if err != nil {
		return "", fmt.Errorf("parse instance URI to read its database name: %w", err)
	}
	if config.Database == "" {
		return "", fmt.Errorf("instance URI names no database")
	}
	// Every caller of this function concatenates its return value directly
	// into a DDL/DCL statement (CREATE ROLE, GRANT ... ON DATABASE, ...),
	// where an identifier cannot be parameterised. Re-validating here, rather
	// than trusting that the value came from scratchName()'s own
	// assertSafeIdentifier check, means this function is safe to call with
	// ANY syntactically valid PostgreSQL URI -- not only ones this package
	// itself produced -- without becoming a second, unguarded path into DDL.
	if !scratchNamePattern.MatchString(config.Database) {
		return "", fmt.Errorf("refusing to return unsafe database name %q for use in DDL/DCL", config.Database)
	}
	return config.Database, nil
}

// roleSuffixLen bounds RoleSuffix's return value. 12 characters of the
// scratch database's crypto/rand hex suffix still carry 48 bits of entropy
// -- far more than two concurrent lanes need to avoid a collision -- while
// keeping `<existing role literal>_<suffix>` inside PostgreSQL's 63-byte
// NAMEDATALEN even for this repository's longest existing role literal
// (29 characters, cmd/dev-health-workerctl). Using the FULL database name
// instead (which also carries a lane prefix, e.g. "lane_4661_scratch_...")
// would risk PostgreSQL silently truncating two different generated role
// names to the same 63-byte prefix, defeating the uniqueness this exists to
// provide.
const roleSuffixLen = 12

// RoleSuffix derives a short, collision-resistant suffix for a cluster-scoped
// object name (a role, in every current caller) from instance's real
// connected database name. Two calls against the same kiac cluster -- two
// successive runs, or two concurrent lanes -- get different suffixes because
// each StartPostgres call mints its own scratch database; two calls against
// a fresh container both get "worker_test", which is safe because nothing
// else has created a role on that container either.
func RoleSuffix(instance *Instance) (string, error) {
	if instance == nil {
		return "", fmt.Errorf("role suffix: no instance")
	}
	name, err := DatabaseName(instance.URI)
	if err != nil {
		return "", fmt.Errorf("role suffix: %w", err)
	}
	if len(name) > roleSuffixLen {
		name = name[len(name)-roleSuffixLen:]
	}
	return name, nil
}

// maxIdentifierLength is PostgreSQL's NAMEDATALEN limit (64) minus 1 for the
// C string terminator: a name at exactly this length round-trips through
// pg_roles.rolname unmodified; one byte longer is silently truncated by the
// server, not rejected.
const maxIdentifierLength = 63

// RoleName composes a cluster-scoped test role name from a static prefix and
// this call's own database identity (RoleSuffix), and FAILS rather than
// silently truncating if the result would exceed PostgreSQL's NAMEDATALEN
// limit. Truncation is the dangerous failure mode here, not merely an
// awkward one: two different prefixes -- or the same prefix from two
// different calls whose suffixes happen to share their last bytes after a
// server-side cut -- could truncate to the identical stored name, which is
// exactly the collision this whole package exists to prevent. Silently
// cutting the name would trade a loud, obvious test failure for exactly that
// silent, intermittent one.
//
// Every current caller of RoleSuffix composes a role name from it this same
// way (`prefix + "_" + suffix`); RoleName is the single place that pattern
// and its safety check live, so a future long prefix fails at the point it
// is added rather than passing review on today's evidence (the longest
// existing literal, "workerctl_coordinator_runtime", is 29 bytes -- 29+1+12
// = 42, comfortably under 63 -- which is a fact about today's names, not a
// guarantee about tomorrow's).
func RoleName(prefix string, instance *Instance) (string, error) {
	suffix, err := RoleSuffix(instance)
	if err != nil {
		return "", err
	}
	name := prefix + "_" + suffix
	if len(name) > maxIdentifierLength {
		return "", fmt.Errorf(
			"role name %q is %d bytes, exceeds PostgreSQL's %d-byte NAMEDATALEN limit -- "+
				"refusing rather than letting the server silently truncate it, which could "+
				"collide two calls that produced different suffixes",
			name, len(name), maxIdentifierLength,
		)
	}
	return name, nil
}

// roleNamePattern is the identifier class DropRole will interpolate into
// DROP OWNED BY / DROP ROLE. It matches RoleName's own composition
// (`prefix + "_" + suffix`, both lower-cased ASCII) -- never a literal
// widened to accept anything a caller might pass.
var roleNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// roleExecer is the narrow write surface DropRole needs. *pgxpool.Pool and
// *pgx.Conn both satisfy it structurally -- callers pass whichever admin
// connection created the role, without this package importing pgxpool.
type roleExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// DropRole releases a cluster-scoped role a test created via RoleName (or
// RoleSuffix composed by hand). CREATE ROLE is the half of the remote/kiac
// lifecycle StartPostgres does not clean up on its own: Instance.Close drops
// only the scratch DATABASE it created (remote.go), never a role a test
// layered on top of it. Making every role name unique per call (RoleName's
// whole point, CHAOS-4661) also disabled the old collision-avoidance
// side-effect that used to bound this: two runs never share a name anymore,
// so nothing ever exercises "DROP ROLE IF EXISTS" before a CREATE, and the
// role a test does not drop itself becomes a PERMANENT, unbounded addition
// to the shared cluster -- one per test per run, forever. Every test that
// creates a role must therefore call this itself, from a defer or
// t.Cleanup, registered so it runs while pool is still open.
//
// DROP OWNED BY runs first because a plain DROP ROLE fails closed against
// its own GRANTs: "role ... cannot be dropped because some objects depend
// on it / DETAIL: privileges for table ..." -- reproduced against a live
// kiac cluster on the exact GRANT shape these tests use (SELECT/INSERT/
// UPDATE/DELETE on ALL TABLES IN SCHEMA public). DROP OWNED BY revokes
// every privilege the role holds and drops anything it owns in pool's
// current database, which is where those GRANTs live; DROP ROLE then
// succeeds unconditionally. Verified independently to work even while a
// SEPARATE connection is still logged in as role (a live session held open
// with pg_sleep during the DROP OWNED BY / DROP ROLE pair) -- callers do
// not need to sequence this after closing a restricted pool that connected
// as the role, only after pool (the admin connection) is still usable.
//
// Both statements use IF EXISTS-equivalent tolerance: DROP OWNED BY on a
// role with nothing owned is a no-op, and DROP ROLE IF EXISTS never errors
// on an absent role -- calling this twice, or after a test already dropped
// it manually, is safe. A failure is logged through logf (typically
// t.Logf) rather than surfaced as an error: this runs from a defer/
// t.Cleanup after the test has already recorded its own pass/fail, and a
// cleanup problem must be visible without being able to flip that result.
func DropRole(pool roleExecer, role string, logf func(format string, args ...any)) {
	if !roleNamePattern.MatchString(role) {
		logf("DropRole: refusing to drop unsafe role name %q", role)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, "DROP OWNED BY "+role); err != nil {
		logf("DropRole: drop owned by %s: %v", role, err)
	}
	if _, err := pool.Exec(ctx, "DROP ROLE IF EXISTS "+role); err != nil {
		logf("DropRole: drop role %s: %v", role, err)
	}
}
