-- Revision 0155 (down_revision 0154), written by hand.
-- The migrator runs this file and the alembic_version update in one transaction, so the index cannot be built
-- CONCURRENTLY: the build holds a SHARE lock on users (writes wait) until the run commits.
-- users.sso_provider_id names the provider whose identity an OIDC or SAML account holds, so the pair (provider,
-- subject) identifies the account. The backfill binds an account only when exactly one provider of its protocol
-- belongs to an organization the account is a member of, and only when no other account would hold the same
-- (provider, subject); every other account stays unbound (NULL) and matches no provider's identity.

ALTER TABLE users ADD COLUMN sso_provider_id uuid REFERENCES sso_providers (id) ON DELETE SET NULL;

UPDATE users u
SET sso_provider_id = c.provider_id
FROM (
    SELECT user_id, provider_id,
           count(*) OVER (PARTITION BY provider_id, subject) AS holders
    FROM (
        SELECT u2.id AS user_id, u2.auth_provider_id AS subject, (array_agg(p.id))[1] AS provider_id
        FROM users u2
        JOIN memberships m ON m.user_id = u2.id
        JOIN sso_providers p ON p.org_id = m.org_id AND p.protocol = u2.auth_provider
        WHERE u2.auth_provider IN ('oidc', 'saml') AND u2.auth_provider_id IS NOT NULL
        GROUP BY u2.id, u2.auth_provider_id
        HAVING count(DISTINCT p.id) = 1
    ) bound
) c
WHERE u.id = c.user_id AND c.holders = 1;

CREATE UNIQUE INDEX ix_users_sso_identity ON users (sso_provider_id, auth_provider_id) WHERE sso_provider_id IS NOT NULL;
