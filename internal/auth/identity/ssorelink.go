package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrRelinkRefused reports an owned SSO sign-in whose subject a go-api account carries but cannot be re-linked: it
// is unbound, bound to a provider row of another protocol, or has no auth user. It is an ambiguous identity
// (ErrMultipleUsers), never a miss, so the flow never provisions a second principal for that account.
var ErrRelinkRefused = fmt.Errorf("%w: a go-api account carries the subject but cannot be re-linked", ErrMultipleUsers)

// ssoRelinkProtocols are the users.auth_provider values whose go-api account binds its subject to one provider row
// (users.sso_provider_id, pgmigrate revision 0155).
var ssoRelinkProtocols = []string{"oidc", "saml"}

func isSSORelinkProtocol(authProvider string) bool {
	for _, p := range ssoRelinkProtocols {
		if authProvider == p {
			return true
		}
	}
	return false
}

// isSSOBound: provider oidc or saml, bound to an existing provider row of that same protocol (boundProtocol is that
// row's protocol, nil when the account is unbound or the row is gone).
func isSSOBound(authProvider *string, ssoProviderID *uuid.UUID, boundProtocol *string) bool {
	return authProvider != nil && isSSORelinkProtocol(*authProvider) &&
		ssoProviderID != nil && boundProtocol != nil && *boundProtocol == *authProvider
}

// IsSSORelinkable is the one definition of a go-api SSO account that readiness waives because the owned store
// re-links it at its first sign-in: it holds only when that sign-in can key it. The account is OIDC (its stored
// subject IS the key subject), bound to an existing OIDC provider row, and its subject passes ValidSubject, the rule
// IdentityBySubject and the OIDC subject check apply. A SAML account is not waived here: its key needs the NameID
// Format and SPNameQualifier that go-api stores at SAML sign-in, so usersbackfill.Relinkable and the users-owned guard
// waive it only when saml.SubjectFromStored keys those parts (a transient NameID or an account that never signed in
// stays in IsSSOSubjectUnverifiable's bucket). The users-owned guard leaves a relinkable account out of its identity
// count.
func IsSSORelinkable(authProvider, authProviderID *string, ssoProviderID *uuid.UUID, boundProtocol *string) bool {
	return isSSOBound(authProvider, ssoProviderID, boundProtocol) && *authProvider == "oidc" &&
		authProviderID != nil && ValidSubject(*authProviderID)
}

// IsSSOSubjectUnverifiable is the readiness bucket sso_subject_unverifiable: a SAML account bound to an existing SAML
// provider row whose stored NameID value passes ValidSubject. It is the candidate set: the guard and the backfill
// report leave out the accounts whose stored NameID parts key under saml.SubjectFromStored; an account that never
// signed in (NULL parts) or has a transient NameID stays counted (fail closed) and holds the users-owned guard until
// it has an identity.
func IsSSOSubjectUnverifiable(authProvider, authProviderID *string, ssoProviderID *uuid.UUID, boundProtocol *string) bool {
	return isSSOBound(authProvider, ssoProviderID, boundProtocol) && *authProvider == "saml" &&
		authProviderID != nil && ValidSubject(*authProviderID)
}

// subjectEdgeSpace is every rune unicode.IsSpace accepts that unicode.IsControl does not (the controls are refused
// anywhere in the value), as a PostgreSQL bracket expression body.
const subjectEdgeSpace = ` \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000`

// SubjectKeyableSQL is ValidSubject over the text expression expr, for a UTF8 database (text there is always valid
// UTF-8 and never holds NUL); never NULL.
func SubjectKeyableSQL(expr string) string {
	return fmt.Sprintf(`(coalesce(octet_length(%[1]s), 0) BETWEEN 1 AND %[2]d`+
		` AND %[1]s !~ '[\u0001-\u001f\u007f-\u009f\ufffd]'`+
		` AND %[1]s !~ '^[%[3]s]' AND %[1]s !~ '[%[3]s]$')`, expr, maxSubjectBytes, subjectEdgeSpace)
}

// ssoBoundSQL is isSSOBound over the public.users row aliased alias; never NULL.
func ssoBoundSQL(alias string) string {
	return fmt.Sprintf(`(coalesce(%[1]s.auth_provider, '') IN ('oidc', 'saml')`+
		` AND %[1]s.sso_provider_id IS NOT NULL AND EXISTS (SELECT 1 FROM public.sso_providers sp_relink`+
		` WHERE sp_relink.id = %[1]s.sso_provider_id AND sp_relink.protocol = %[1]s.auth_provider))`, alias)
}

// SSORelinkableSQL is IsSSORelinkable as a predicate over the public.users row aliased alias; never NULL.
func SSORelinkableSQL(alias string) string {
	return fmt.Sprintf(`(%[2]s AND %[1]s.auth_provider = 'oidc' AND %[3]s)`,
		alias, ssoBoundSQL(alias), SubjectKeyableSQL(alias+".auth_provider_id"))
}

// SSOSubjectUnverifiableSQL is IsSSOSubjectUnverifiable as a predicate over the public.users row aliased alias;
// never NULL.
func SSOSubjectUnverifiableSQL(alias string) string {
	return fmt.Sprintf(`(%[2]s AND %[1]s.auth_provider = 'saml' AND %[3]s)`,
		alias, ssoBoundSQL(alias), SubjectKeyableSQL(alias+".auth_provider_id"))
}

// relinkOwned is IdentityBySubject's fallback after the users cutover, for an SSO key that missed. The go-api
// accounts of legacy.AuthProvider that carry the stored subject are read; one bound to another provider row of the
// same protocol is that provider's identity and is not read. No such account is a miss. Exactly one that is bound
// (isSSOBound) to the key's provider row and holds an auth user is linked to the key under audit (reason
// identity_relinked); the key itself already passed Validate, and a SAML key is built from the live NameID format,
// which saml.Subject refuses when transient. Nothing is written to public.users or to the account's auth rows. Any
// other carrier is ErrRelinkRefused.
func (s *Store) relinkOwned(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, bool, error) {
	if !key.Provider.IsSSO() || !isSSORelinkProtocol(legacy.AuthProvider) {
		return Principal{}, false, nil
	}
	stored := key.Subject
	if legacy.Subject != "" {
		stored = legacy.Subject
	}
	rows, err := s.pool.Query(ctx, `SELECT u.id, `+ssoBoundToKeySQL("u", "$3")+`
       AND EXISTS (SELECT 1 FROM `+s.q+`.users a WHERE a.principal_id = u.id)
FROM public.users u
WHERE u.auth_provider = $1 AND u.auth_provider_id = $2
  AND `+ssoCarrierScopeSQL("u", "$3"),
		legacy.AuthProvider, stored, string(key.Provider))
	if err != nil {
		return Principal{}, false, s.storeErr(ctx, "read relink user", err)
	}
	ids, relinkable, err := scanRelinkCarriers(rows)
	if err != nil {
		return Principal{}, false, s.storeErr(ctx, "read relink user", err)
	}
	switch {
	case len(ids) == 0:
		return Principal{}, false, nil
	case len(ids) > 1 || !relinkable:
		return Principal{}, false, ErrRelinkRefused
	}
	if err := s.relinkIdentity(ctx, key, ids[0]); err != nil {
		if errors.Is(err, ErrProviderAlreadyLinked) {
			return Principal{ID: ids[0]}, false, err
		}
		return Principal{}, false, err
	}
	return s.principalByKey(ctx, key)
}

func scanRelinkCarriers(rows pgx.Rows) ([]uuid.UUID, bool, error) {
	defer rows.Close()
	var ids []uuid.UUID
	var relinkable bool
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id, &relinkable); err != nil {
			return nil, false, err
		}
		ids = append(ids, id)
	}
	return ids, relinkable, rows.Err()
}

// relinkIdentity links key to the backfilled go-api account principal through attachIdentity (principal lock, one
// identity per provider, conflict check). A concurrent re-link of the same key is success.
func (s *Store) relinkIdentity(ctx context.Context, key ExternalKey, principal uuid.UUID) (err error) {
	defer func() { s.countLink(ctx, "relink", err) }()
	linkID := uuid.New()
	target, subject := linkID.String(), principal.String()
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			// A disabled account is still linked; its sign-in is refused by the caller from DisabledAt.
			return s.attachIdentity(ctx, tx, linkID, key, principal, false, errProjectedConcurrently, nil)
		},
		Audit:  identityAudit("auth.identity.relinked", "identity_relinked", key, target, subject, Actor{}),
		Events: []audit.OutboxEvent{identityEvent("external_identity.linked", key, target, subject)},
	})
	if errors.Is(err, errProjectedConcurrently) {
		return nil
	}
	if err != nil {
		return s.domainErr(ctx, "relink external identity", err)
	}
	s.logger.InfoContext(ctx, "identity: go-api account re-linked to its SSO identity", "provider", providerLabel(key.Provider))
	return nil
}
