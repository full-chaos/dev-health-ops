package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

// checkRoleIdentity proves the pool IS the role (CHAOS-6804, lead D2616): the
// AUTHENTICATED user (pg_stat_activity.usename of this backend: neither SET ROLE
// nor SET SESSION AUTHORIZATION changes it), session_user and current_user all
// equal the role, and the role is an unprivileged login (no SUPERUSER, BYPASSRLS,
// CREATEROLE, CREATEDB, REPLICATION) that is a member of no role. The predicate is
// roleacl.IdentityPredicateSQL, the ONE definition every posture check shares.
// A refusal names the cause; role names are checked-in runtime identifiers, never
// connection material, so nothing here can carry a credential.
func checkRoleIdentity(ctx context.Context, pool *pgxpool.Pool, role string) error {
	var ok bool
	if err := pool.QueryRow(ctx, "SELECT "+roleacl.IdentityPredicateSQL, role).Scan(&ok); err != nil {
		return fmt.Errorf("%w: reading the login identity: %w", ErrUnavailable, err)
	}
	if ok {
		return nil
	}
	return fmt.Errorf("%w: %w for role %q: %s", ErrUnavailable, ErrPostureRefused, role, describeIdentityMismatch(ctx, pool, role))
}

// describeIdentityMismatch says which part of the identity predicate failed.
func describeIdentityMismatch(ctx context.Context, pool *pgxpool.Pool, role string) string {
	var authenticated *string
	var sessionUser, currentUser string
	var attributesOK, membershipFree bool
	err := pool.QueryRow(ctx, `SELECT
		(SELECT usename::text FROM pg_catalog.pg_stat_activity WHERE pid = pg_backend_pid()),
		session_user::text, current_user::text,
		`+roleacl.RoleAttributesSQL+`, `+roleacl.MembershipFreeSQL, role,
	).Scan(&authenticated, &sessionUser, &currentUser, &attributesOK, &membershipFree)
	if err != nil {
		return "the login identity could not be described"
	}
	authenticatedName := ""
	if authenticated != nil {
		authenticatedName = *authenticated
	}
	switch {
	case authenticatedName != role || sessionUser != role || currentUser != role:
		return fmt.Sprintf("the pool authenticated as %q, has session_user %q and acts as %q (current_user), not the named role: the login itself must be the role",
			authenticatedName, sessionUser, currentUser)
	case !attributesOK:
		return "the role is not an unprivileged login (it must be able to log in and hold none of SUPERUSER, BYPASSRLS, CREATEROLE, CREATEDB, REPLICATION)"
	case !membershipFree:
		return "the role is a member of another role"
	default:
		return "the identity predicate refused"
	}
}

// identityMismatchForDiagnosis names the identity cause for a posture refusal, or
// "" when the shared identity predicate PASSES (so the refusal is about grants, not
// the login). It runs on a detached context: the caller's may already be spent by
// the slow refusing query.
func identityMismatchForDiagnosis(ctx context.Context, pool *pgxpool.Pool, role string) string {
	identityCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postureDiagnoseTimeout)
	defer cancel()
	var ok bool
	if err := pool.QueryRow(identityCtx, "SELECT "+roleacl.IdentityPredicateSQL, role).Scan(&ok); err != nil || ok {
		return ""
	}
	return describeIdentityMismatch(identityCtx, pool, role)
}
