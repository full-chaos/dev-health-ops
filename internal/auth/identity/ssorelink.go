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

// IsSSORelinkable is the one definition of a go-api SSO account that the owned store re-links at its first sign-in
// instead of the users backfill carrying its identity: provider oidc or saml, a non-empty stored subject, and a
// binding to an existing provider row of that same protocol (boundProtocol is that row's protocol, nil when the
// account is unbound or the row is gone). The users-owned guard leaves such an account out of its identity count.
func IsSSORelinkable(authProvider, authProviderID *string, ssoProviderID *uuid.UUID, boundProtocol *string) bool {
	return authProvider != nil && isSSORelinkProtocol(*authProvider) &&
		authProviderID != nil && *authProviderID != "" &&
		ssoProviderID != nil && boundProtocol != nil && *boundProtocol == *authProvider
}

// SSORelinkableSQL is IsSSORelinkable as a predicate over the public.users row aliased alias; never NULL.
func SSORelinkableSQL(alias string) string {
	return fmt.Sprintf(`(coalesce(%[1]s.auth_provider, '') IN ('oidc', 'saml') AND coalesce(%[1]s.auth_provider_id, '') <> ''`+
		` AND %[1]s.sso_provider_id IS NOT NULL AND EXISTS (SELECT 1 FROM public.sso_providers sp_relink`+
		` WHERE sp_relink.id = %[1]s.sso_provider_id AND sp_relink.protocol = %[1]s.auth_provider))`, alias)
}

// relinkOwned is IdentityBySubject's fallback after the users cutover, for an SSO key that missed. The go-api
// accounts of legacy.AuthProvider that carry the stored subject are read; one bound to another provider row of the
// same protocol is that provider's identity and is not read. No such account is a miss. Exactly one that is
// re-linkable (IsSSORelinkable), bound to the key's provider row and holding an auth user is linked to the key
// under audit (reason identity_relinked); nothing is written to public.users or to the account's auth rows. Any
// other carrier is ErrRelinkRefused.
func (s *Store) relinkOwned(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, bool, error) {
	if !key.Provider.IsSSO() || !isSSORelinkProtocol(legacy.AuthProvider) {
		return Principal{}, false, nil
	}
	stored := key.Subject
	if legacy.Subject != "" {
		stored = legacy.Subject
	}
	rows, err := s.pool.Query(ctx, `SELECT u.id, `+SSORelinkableSQL("u")+` AND coalesce(u.sso_provider_id = $3::uuid, false)
       AND EXISTS (SELECT 1 FROM `+s.q+`.users a WHERE a.principal_id = u.id)
FROM public.users u
WHERE u.auth_provider = $1 AND u.auth_provider_id = $2
  AND (u.sso_provider_id IS NULL OR u.sso_provider_id = $3::uuid
       OR NOT EXISTS (SELECT 1 FROM public.sso_providers p WHERE p.id = u.sso_provider_id AND p.protocol = u.auth_provider))`,
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
