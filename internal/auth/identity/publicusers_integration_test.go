//go:build integration

package identity

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type publicRow struct {
	Email          string
	Username       *string
	PasswordHash   *string
	FullName       *string
	AuthProvider   *string
	AuthProviderID *string
	IsActive       bool
	IsVerified     bool
	IsSuperuser    bool
	TokenVersion   int
	SameStamp      bool
}

func (e *env) publicUser(t *testing.T, ctx context.Context, id uuid.UUID) (publicRow, bool) {
	t.Helper()
	var r publicRow
	err := e.admin.QueryRow(ctx, `SELECT email, username, password_hash, full_name, auth_provider, auth_provider_id,
	is_active, is_verified, is_superuser, token_version, created_at = updated_at FROM public.users WHERE id = $1`, id).
		Scan(&r.Email, &r.Username, &r.PasswordHash, &r.FullName, &r.AuthProvider, &r.AuthProviderID,
			&r.IsActive, &r.IsVerified, &r.IsSuperuser, &r.TokenVersion, &r.SameStamp)
	if err != nil {
		return publicRow{}, false
	}
	return r, true
}

func projectionHash(t *testing.T, material string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(material), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

// A local account created through auth-service is the row go-api's register
// route writes: no username, provider local, active, unverified, not a
// superuser, token_version 0, created_at = updated_at, the same id.
func TestProvisionLocalWritesTheRowGoAPIRegisterWrites(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	hash := projectionHash(t, "projection-local-material")
	email := uniqueEmailCom("proj-local")
	id, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: strings.ToUpper(email), FullName: "Local Person", PasswordHash: &hash, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	row, ok := e.publicUser(t, ctx, id)
	if !ok {
		t.Fatal("no public.users row")
	}
	if row.Email != email || row.Username != nil || row.PasswordHash == nil || *row.PasswordHash != hash ||
		row.FullName == nil || *row.FullName != "Local Person" || row.AuthProvider == nil || *row.AuthProvider != "local" ||
		row.AuthProviderID != nil || !row.IsActive || row.IsVerified || row.IsSuperuser || row.TokenVersion != 0 || !row.SameStamp {
		t.Fatalf("public row differs from go-api's register row: %+v", row)
	}
}

// A social account is the row go-api's social login writes.
func TestProvisionFederatedWritesTheRowGoAPISocialLoginWrites(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	subject := "gh-" + uuid.NewString()
	key := ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: subject}
	id, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: uniqueEmailCom("proj-social"), FullName: "Display", Username: "octo", LegacyFullName: "Octo Cat", Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatalf("ProvisionFederated: %v", err)
	}
	row, ok := e.publicUser(t, ctx, id)
	if !ok {
		t.Fatal("no public.users row")
	}
	if row.Username == nil || *row.Username != "octo" || row.FullName == nil || *row.FullName != "Octo Cat" ||
		row.AuthProvider == nil || *row.AuthProvider != "github" || row.AuthProviderID == nil || *row.AuthProviderID != subject ||
		row.PasswordHash != nil || !row.IsActive || !row.IsVerified || row.IsSuperuser || row.TokenVersion != 0 || !row.SameStamp {
		t.Fatalf("public row differs from go-api's social row: %+v", row)
	}
}

// An SSO key without the legacy provider and subject writes nothing: neither
// public.users nor the auth rows exist afterwards.
func TestProvisionFederatedRefusesAnSSOAccountItCannotProject(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmailCom("proj-sso")
	key := ExternalKey{Issuer: "https://idp.example.test", Provider: Provider(uuid.NewString()), Subject: "sso-" + uuid.NewString()}
	if _, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: email, Actor: SelfSignUp(nil)}); !errors.Is(err, ErrLegacyProjection) {
		t.Fatalf("err = %v, want ErrLegacyProjection", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = lower($1)`, email); n != 0 {
		t.Fatalf("%d auth.users rows after the refusal", n)
	}
	id, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: email, LegacyProvider: "oidc", LegacySubject: key.Subject, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatalf("with the legacy fields: %v", err)
	}
	if row, ok := e.publicUser(t, ctx, id); !ok || row.AuthProvider == nil || *row.AuthProvider != "oidc" || row.AuthProviderID == nil || *row.AuthProviderID != key.Subject {
		t.Fatalf("public row: %+v ok=%v", row, ok)
	}
}

// An address a go-api user holds is refused as taken, and no auth row is left.
func TestProvisionLocalRefusesAnAddressPublicUsersHolds(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	email := uniqueEmailCom("proj-taken")
	e.seedUser(t, ctx, email, true, true)
	if _, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: email, Actor: SelfSignUp(nil)}); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("err = %v, want ErrEmailTaken", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE email_lower = $1`, email); n != 0 {
		t.Fatalf("%d auth.users rows after the refusal", n)
	}
}

// A password change updates both tables in one transaction, never bumps
// token_version, and a missing public row rolls the auth change back.
func TestSetPasswordHashProjectsTheVerifierAndLeavesTokenVersionAlone(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	first := projectionHash(t, "projection-first-material")
	id, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: uniqueEmailCom("proj-pw"), PasswordHash: &first, Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	second := projectionHash(t, "projection-second-material")
	if err := e.owned.SetPasswordHash(ctx, id, second, Actor{}); err != nil {
		t.Fatalf("SetPasswordHash: %v", err)
	}
	row, _ := e.publicUser(t, ctx, id)
	if row.PasswordHash == nil || *row.PasswordHash != second || row.TokenVersion != 0 {
		t.Fatalf("public row after the change: %+v", row)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_hash = $2`, id, second); n != 1 {
		t.Fatalf("auth verifier not updated: %d", n)
	}

	if _, err := e.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	third := projectionHash(t, "projection-third-material")
	if err := e.owned.SetPasswordHash(ctx, id, third, Actor{}); err == nil {
		t.Fatal("a password change with no public row succeeded")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_hash = $2`, id, second); n != 1 {
		t.Fatalf("the auth change was not rolled back: %d", n)
	}
}

func TestSetEmailVerifiedProjectsToPublicUsers(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: uniqueEmailCom("proj-verify"), Actor: SelfSignUp(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetEmailVerified(ctx, id, time.Now(), Actor{}); err != nil {
		t.Fatalf("SetEmailVerified: %v", err)
	}
	if row, _ := e.publicUser(t, ctx, id); !row.IsVerified {
		t.Fatalf("public row not verified: %+v", row)
	}
}

func uniqueEmailCom(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8] + "@example.com"
}
