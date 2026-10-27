//go:build integration

package sso_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ownedIdentities is the auth-service identity store with users owned, on the go-api test database: the auth schema
// applied with the users-owned grants, and a store that connects as the auth runtime role.
func ownedIdentities(t *testing.T, ctx context.Context, st stack) *identity.Store {
	t.Helper()
	role := "auth_flipback_rt_" + uuid.NewString()[:8]
	const password = "flipback-test-only-not-a-credential"
	if _, err := st.pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	if _, err := authschema.Apply(ctx, st.pool, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply users-owned: %v", err)
	}
	cfg := st.pool.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// TestFlipBackSignInFindsAnAccountProvisionedUnderOwned: an OIDC or SAML account that auth-service provisioned while
// it owned users signs in through go-api after the switch back, as the same account: go-api finds it by its
// (auth_provider, sso_provider_id, auth_provider_id) key and writes no new account.
func TestFlipBackSignInFindsAnAccountProvisionedUnderOwned(t *testing.T) {
	for _, rig := range scopeRigs() {
		t.Run(rig.protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			st := startStack(t, ctx)
			provider := rig.seed(t, ctx, st, false)
			store := ownedIdentities(t, ctx, st)

			const email = "flipback.person@allowed.example"
			subject := "subject-owned-" + uuid.NewString()[:8]
			keySubject := subject
			if rig.protocol == "saml" {
				keySubject = "saml1|urn:oasis:names:tc:SAML:2.0:nameid-format:persistent||" + subject
			}
			principal, err := store.ProvisionFederated(ctx, identity.ProvisionRequest{
				Key:   identity.ExternalKey{Issuer: "https://idp.flipback.test/" + rig.protocol, Provider: identity.Provider(provider.String()), Subject: keySubject},
				Email: email, Actor: identity.SelfSignUp(nil), LegacyProvider: rig.protocol, LegacySubject: subject,
			})
			if err != nil {
				t.Fatalf("provision under users-owned: %v", err)
			}
			if _, err := st.pool.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
SELECT $1, org_id, $2, 'member', now(), now(), now() FROM sso_providers WHERE id = $3`, uuid.New(), principal, provider); err != nil {
				t.Fatalf("seed membership: %v", err)
			}
			before := snapshotRows(t, ctx, st)

			status, body := rig.signIn(t, st, provider, email, subject)
			if status != http.StatusOK || body["access_token"] == nil {
				t.Fatalf("go-api sign-in after the switch back: %d %v; want 200 as the provisioned account", status, body)
			}
			var stamped int
			if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE id = $1 AND last_login_at IS NOT NULL`, principal).Scan(&stamped); err != nil || stamped != 1 {
				t.Fatalf("the sign-in did not resolve the provisioned account %s (stamped %d, err %v)", principal, stamped, err)
			}
			if after := snapshotRows(t, ctx, st); after.users != before.users || after.memberships != before.memberships {
				t.Fatalf("the sign-in wrote accounts or memberships: before %+v, after %+v", before, after)
			}
		})
	}
}
