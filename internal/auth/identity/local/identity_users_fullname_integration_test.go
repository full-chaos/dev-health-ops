//go:build integration

package local

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func TestIdentityUsersByEmailCarriesTheStoredFullNameInBothReadModes(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := identity.New(identity.Config{Pool: rig.env.runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := identity.New(identity.Config{Pool: rig.env.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	email := "fullname-" + id.String()[:8] + "@example.com"
	if _, err := rig.env.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, full_name, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, '$2b$04$C6UzMDM.H6dfI/f/IKxGhuwM4V2y6W1pYxF1Iy5lC3l3OZ0W6Q2Iu', 'local', true, true, false, 0, now(), now())`,
		id, email, "Zoë Müller 山田"); err != nil {
		t.Fatal(err)
	}
	user, err := IdentityUsers{Store: legacy}.ByEmail(ctx, email)
	if err != nil || user == nil || user.FullName == nil || *user.FullName != "Zoë Müller 山田" {
		t.Fatalf("legacy IdentityUsers.ByEmail = %+v, %v; want the stored full name", user, err)
	}

	ownedEmail := "fullname-owned-" + id.String()[:8] + "@example.com"
	hash := "$2b$04$C6UzMDM.H6dfI/f/IKxGhuwM4V2y6W1pYxF1Iy5lC3l3OZ0W6Q2Iu"
	request := uuid.NewString()
	name := "Zoë Müller 山田"
	ownedID, err := owned.ProvisionLocal(ctx, identity.LocalProvisionRequest{Email: ownedEmail, FullName: &name, PasswordHash: &hash, Actor: identity.SelfSignUp(&request)})
	if err != nil {
		t.Fatal(err)
	}
	user, err = IdentityUsers{Store: owned}.ByEmail(ctx, ownedEmail)
	if err != nil || user == nil || user.ID != ownedID || user.FullName == nil || *user.FullName != name {
		t.Fatalf("owned IdentityUsers.ByEmail = %+v, %v; want the stored display name", user, err)
	}
}
