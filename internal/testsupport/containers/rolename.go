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
	"fmt"

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
