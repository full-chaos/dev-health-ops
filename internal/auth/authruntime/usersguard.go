package authruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrUsersOwnedNotReady refuses to start auth-service with AUTH_USERS_OWNED on
// while the auth schema does not yet hold every public.users account. The
// message carries counts only.
var ErrUsersOwnedNotReady = errors.New("authruntime: AUTH_USERS_OWNED is on but the auth users are not complete")

// usersReader is the slice of a pool the guard needs.
type usersReader interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// checkUsersOwned counts the gaps between public.users and the auth schema:
// public users with no auth user, auth users with no public user, and users of
// a provider other than local (SSO or social) with no external identity. Any
// gap refuses the switch. The first gap is not a reason to stop counting, so
// the refusal names all three.
func checkUsersOwned(ctx context.Context, db usersReader, schema authschema.ValidatedIdentifier) error {
	q := authschema.Quote(schema)
	var missingInAuth, missingInPublic, noIdentity int64
	if err := db.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM public.users u WHERE NOT EXISTS (SELECT 1 FROM `+q+`.users a WHERE a.principal_id = u.id)),
  (SELECT count(*) FROM `+q+`.users a WHERE NOT EXISTS (SELECT 1 FROM public.users u WHERE u.id = a.principal_id)),
  (SELECT count(*) FROM public.users u
     WHERE u.auth_provider IS NOT NULL AND u.auth_provider NOT IN ('', 'local')
       AND NOT EXISTS (SELECT 1 FROM `+q+`.external_identities e WHERE e.principal_id = u.id))`,
	).Scan(&missingInAuth, &missingInPublic, &noIdentity); err != nil {
		return fmt.Errorf("authruntime: reading the users-owned guard counts failed")
	}
	if missingInAuth == 0 && missingInPublic == 0 && noIdentity == 0 {
		return nil
	}
	return fmt.Errorf("%w: public users without an auth user=%d, auth users without a public user=%d, provider users without an external identity=%d",
		ErrUsersOwnedNotReady, missingInAuth, missingInPublic, noIdentity)
}

// CheckUsersOwned is the readiness check that holds auth-service not ready
// while AUTH_USERS_OWNED is on and checkUsersOwned finds a gap. Only the
// failing check's name reaches the HTTP surface; the counts are logged. The
// first pass is remembered: the guard decides whether the process may take
// ownership, and a user go-api creates afterwards must not make a running pod
// unready.
const CheckUsersOwned = "users_owned"
