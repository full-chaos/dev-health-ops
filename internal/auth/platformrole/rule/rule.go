// Package rule is the platform role rule and switch as text and one parse,
// with no other dependency: the SQL condition that a principal holds a
// platform role now (HoldsSQL) and the platform roles switch
// (AUTH_PLATFORM_ROLES_OWNED, OwnedFromLookup). platformrole's store guards and
// platformrole.HoldsSQL / platformrole.OwnedFromLookup are these; a reader of
// the rule outside auth-service imports this package, so it links neither the
// store nor the audit writer.
package rule

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// PlatformAdmin is the catalog's platform administrator role key.
const PlatformAdmin = "platform_admin"

// PlatformOwner is the catalog's platform owner role: a marker that holds no
// action, granted only by auth-migrate.
const PlatformOwner = "platform_owner"

// WriteLock is the advisory transaction lock key every writer of
// platform_role_assignments takes exclusively before it writes. A decision
// that must not interleave with a write takes it shared (SharedWriteLockSQL).
const WriteLock = "auth.platform_role_assignments:write"

// SharedWriteLockSQL takes WriteLock ($1) shared for the rest of the
// transaction.
const SharedWriteLockSQL = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`

// OwnedEnv is the platform roles switch: while it is not true, public
// users.is_superuser is the platform authority.
const OwnedEnv = "AUTH_PLATFORM_ROLES_OWNED"

// OwnedFromLookup reads OwnedEnv: unset, empty or false is off, true is on,
// any other value is an error. Case and surrounding space are ignored, as for
// AUTH_USERS_OWNED.
func OwnedFromLookup(lookup secrets.LookupEnv) (bool, error) {
	raw, _ := lookup(OwnedEnv)
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	}
	return false, fmt.Errorf("%s must be true or false", OwnedEnv)
}

// ActiveRule is the current-state rule of an assignment, on the database
// clock of the statement that reads it: a user principal that is not
// disabled, and an assignment that has not expired. assignment and principal
// are the aliases of the assignment row and its principal row.
func ActiveRule(assignment, principal string) string {
	return `(` + principal + `.kind = 'user' AND ` + principal + `.disabled_at IS NULL AND (` +
		assignment + `.expires_at IS NULL OR ` + assignment + `.expires_at > statement_timestamp()))`
}

// HoldsSQL is the one SQL condition that principalExpr holds the platform role
// roleExpr now, under ActiveRule. schema is a quoted auth schema;
// principalExpr and roleExpr are SQL expressions of the caller's own
// statement. The subquery's aliases are its own, so an outer alias never
// shadows them.
func HoldsSQL(schema, principalExpr, roleExpr string) string {
	return `EXISTS (SELECT 1 FROM ` + schema + `.platform_role_assignments pra_held JOIN ` + schema +
		`.principals pra_holder ON pra_holder.id = pra_held.principal_id WHERE pra_held.principal_id = ` + principalExpr +
		` AND pra_held.role_key = ` + roleExpr + ` AND ` + ActiveRule("pra_held", "pra_holder") + `)`
}
