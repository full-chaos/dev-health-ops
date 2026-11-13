// Package sessionrule is the one rule for whether an auth session is live: SQL
// fragments over auth.sessions that the session store and the decision reader both
// build their statements from, so there is no second copy to drift.
//
// A session is ended when its own row is revoked, when its principal's password
// stamp is after the row was created, when the web session it hangs from (a browser
// client's session) is ended by the same two tests, or when it has expired. It
// expires at the first of its absolute cap, its newest refresh credential's expiry
// (or, with no credential, its last rotation plus the refresh lifetime) and, for a
// client session, its web session's expiry computed the same way.
package sessionrule

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// DefaultRefreshTTL is the one refresh lifetime value of the auth plane (session.DefaultRefreshTTL
// names it). It lives here so a reader of the rule outside the session store needs no import of it.
const DefaultRefreshTTL = 7 * 24 * time.Hour

// Rule builds the fragments for one auth schema and refresh lifetime.
type Rule struct {
	schema    string
	ttlMicros int64
}

// New returns the rule over quotedSchema (a quoted auth schema identifier) with
// refreshTTL as a credential's idle lifetime. Neither has a default here.
func New(quotedSchema string, refreshTTL time.Duration) (Rule, error) {
	if quotedSchema == "" {
		return Rule{}, errors.New("sessionrule: an auth schema is required")
	}
	if refreshTTL <= 0 {
		return Rule{}, errors.New("sessionrule: a positive refresh lifetime is required")
	}
	return Rule{schema: quotedSchema, ttlMicros: refreshTTL.Microseconds()}, nil
}

// Revocation builds the revoke fragment alone, for a reader that decides no expiry:
// it takes no refresh lifetime.
type Revocation struct {
	schema string
}

// NewRevocation returns the revoke fragment over quotedSchema (a quoted auth schema identifier).
func NewRevocation(quotedSchema string) (Revocation, error) {
	if quotedSchema == "" {
		return Revocation{}, errors.New("sessionrule: an auth schema is required")
	}
	return Revocation{schema: quotedSchema}, nil
}

// Revoked is Rule.Revoked: the same text, without a refresh lifetime.
func (v Revocation) Revoked(a string) string {
	return `COALESCE(` + ownRevoked(v.schema, a) + `, ` + ofWebParent(v.schema, a, func(p string) string { return ownRevoked(v.schema, p) }) + `)`
}

var aliasPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func checkAlias(a string) string {
	if !aliasPattern.MatchString(a) {
		panic("sessionrule: not a simple lower-case alias: " + a)
	}
	return a
}

// Revoked is the effective revoked_at of the row aliased a: its own revoke, else its
// principal's password stamp when after the row was created, else, for a client session,
// the same two of its web session, read against the web session's own created_at.
// NULL means not revoked.
func (r Rule) Revoked(a string) string {
	return Revocation{schema: r.schema}.Revoked(a)
}

// Expiry is the effective expiry of the row aliased a. LEAST ignores the NULL of an
// unlinked session.
func (r Rule) Expiry(a string) string {
	return `LEAST(` + r.ownExpiry(a) + `, ` + ofWebParent(r.schema, a, r.ownExpiry) + `)`
}

// Cap is the absolute cap of the row aliased a: its expires_at, or for a client
// session its web session's effective own expiry when that is first.
func (r Rule) Cap(a string) string {
	return `LEAST(` + checkAlias(a) + `.expires_at, ` + ofWebParent(r.schema, a, r.ownExpiry) + `)`
}

// Live is true for the row aliased a when it is not revoked and expires after clock
// (an SQL expression such as clock_timestamp() or now()).
func (r Rule) Live(a, clock string) string {
	return `(` + r.Revoked(a) + ` IS NULL AND ` + r.Expiry(a) + ` > ` + clock + `)`
}

func ownRevoked(schema, a string) string {
	checkAlias(a)
	return fmt.Sprintf(`COALESCE(%[2]s.revoked_at,
		(SELECT u.password_updated_at FROM %[1]s.users u
		 WHERE u.principal_id = %[2]s.principal_id AND u.password_updated_at > %[2]s.created_at))`, schema, a)
}

func (r Rule) ownExpiry(a string) string {
	checkAlias(a)
	return fmt.Sprintf(`LEAST(%[2]s.expires_at, COALESCE(
		(SELECT r.expires_at FROM %[1]s.refresh_credentials r WHERE r.session_id = %[2]s.id
		 ORDER BY r.issued_at DESC, r.created_at DESC LIMIT 1),
		COALESCE(%[2]s.last_seen_at, %[2]s.created_at) + %[3]d * interval '1 microsecond'))`, r.schema, a, r.ttlMicros)
}

// ofWebParent reads own over the web session of the row aliased a, as alias a+"_wp".
// It is NULL for an unlinked session. Depth one: a linked session's parent has no
// link (migration 0042's trigger).
func ofWebParent(schema, a string, own func(string) string) string {
	parent := a + "_wp"
	return `(SELECT ` + own(parent) + ` FROM ` + schema + `.sessions ` + parent + ` WHERE ` + parent + `.id = ` + a + `.web_session_id)`
}
