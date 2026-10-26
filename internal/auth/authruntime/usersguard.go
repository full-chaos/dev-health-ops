package authruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
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
// a provider other than local (SSO or social) with no external identity; a non-interactive service placeholder (identity.IsServicePlaceholder) and an SSO account the owned store re-links at its first sign-in (identity.IsSSORelinkable, or a bound SAML account whose stored NameID parts key under saml.SubjectFromStored) are not counted. Any
// gap refuses the switch. The first gap is not a reason to stop counting, so
// the refusal names all three, and the part of the third that is the
// sso_subject_unverifiable bucket (identity.IsSSOSubjectUnverifiable).
func checkUsersOwned(ctx context.Context, db usersReader, schema authschema.ValidatedIdentifier) error {
	q := authschema.Quote(schema)
	var missingInAuth, missingInPublic, noIdentity, unverifiable int64
	var candidates string
	if err := db.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM public.users u WHERE NOT EXISTS (SELECT 1 FROM `+q+`.users a WHERE a.principal_id = u.id)),
  (SELECT count(*) FROM `+q+`.users a WHERE NOT EXISTS (SELECT 1 FROM public.users u WHERE u.id = a.principal_id)),
  (SELECT count(*) FROM public.users u
     WHERE u.auth_provider IS NOT NULL AND u.auth_provider NOT IN ('', 'local')
       AND NOT `+identity.ServicePlaceholderSQL("u")+`
       AND NOT `+identity.SSORelinkableSQL("u")+`
       AND NOT EXISTS (SELECT 1 FROM `+q+`.external_identities e WHERE e.principal_id = u.id)),
  (SELECT count(*) FROM public.users u
     WHERE `+identity.SSOSubjectUnverifiableSQL("u")+`
       AND NOT EXISTS (SELECT 1 FROM `+q+`.external_identities e WHERE e.principal_id = u.id)),
  (SELECT coalesce(json_agg(json_build_array(u.auth_provider_id, u.saml_nameid_format, u.saml_sp_name_qualifier)), '[]')::text
     FROM public.users u
     WHERE `+identity.SSOSubjectUnverifiableSQL("u")+` AND u.saml_nameid_format IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM `+q+`.external_identities e WHERE e.principal_id = u.id))`,
	).Scan(&missingInAuth, &missingInPublic, &noIdentity, &unverifiable, &candidates); err != nil {
		return fmt.Errorf("authruntime: reading the users-owned guard counts failed")
	}
	waived, err := keyableSAML(candidates)
	if err != nil {
		return fmt.Errorf("authruntime: reading the users-owned guard counts failed")
	}
	noIdentity -= waived
	unverifiable -= waived
	if missingInAuth == 0 && missingInPublic == 0 && noIdentity == 0 {
		return nil
	}
	return fmt.Errorf("%w: public users without an auth user=%d, auth users without a public user=%d, provider users without an external identity=%d (sso_subject_unverifiable=%d)",
		ErrUsersOwnedNotReady, missingInAuth, missingInPublic, noIdentity, unverifiable)
}

// keyableSAML counts the accounts of the sso_subject_unverifiable bucket whose stored NameID parts key under
// saml.SubjectFromStored, the runtime rule: the owned store re-links them at their next sign-in, so the guard waives
// them. candidates is the JSON the query built: [value, format, spNameQualifier] of every bound SAML account with a
// captured format and no identity.
func keyableSAML(candidates string) (int64, error) {
	var rows [][3]*string
	if err := json.Unmarshal([]byte(candidates), &rows); err != nil {
		return 0, err
	}
	var n int64
	for _, r := range rows {
		if r[0] == nil {
			continue
		}
		if _, err := saml.SubjectFromStored(*r[0], r[1], r[2]); err == nil {
			n++
		}
	}
	return n, nil
}

// CheckUsersOwned is the readiness check that holds auth-service not ready
// while AUTH_USERS_OWNED is on and checkUsersOwned finds a gap. Only the
// failing check's name reaches the HTTP surface; the counts are logged. The
// first pass is remembered: the guard decides whether the process may take
// ownership, and a user go-api creates afterwards must not make a running pod
// unready.
const CheckUsersOwned = "users_owned"
