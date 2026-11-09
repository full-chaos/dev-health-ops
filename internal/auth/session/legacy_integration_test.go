//go:build integration

package session

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// goUser seeds a go-api user at token_version tv and its auth principal.
func (e *env) goUser(t *testing.T, ctx context.Context, tv int) uuid.UUID {
	t.Helper()
	id := e.principal(t, ctx)
	tag := id.String()[:8]
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2, username = $3, full_name = 'Legacy User', password_hash = NULL,
		auth_provider = 'local', is_active = true, is_verified = true, is_superuser = false, token_version = $4 WHERE id = $1`,
		id, "legacy-"+tag+"@example.test", "legacy-"+tag, tv); err != nil {
		t.Fatalf("seed go-api user: %v", err)
	}
	return id
}

func TestCreateAndCreateAuditedPinTheUsersTokenVersion(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	user := e.goUser(t, ctx, 5)
	plain := e.create(t, ctx, user)
	audited, err := e.store.CreateAudited(ctx, user, nil, []string{"pwd"}, signInEvent(user))
	if err != nil {
		t.Fatalf("CreateAudited: %v", err)
	}
	for name, issued := range map[string]Issued{"Create": plain, "CreateAudited": audited} {
		if issued.Session.LegacyTokenVersion == nil || *issued.Session.LegacyTokenVersion != 5 {
			t.Fatalf("%s pinned %v, want 5", name, issued.Session.LegacyTokenVersion)
		}
		if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND legacy_token_version = 5`, issued.Session.ID.String()); n != 1 {
			t.Fatalf("%s: the session row is not pinned at 5", name)
		}
	}
	// A principal go-api does not hold has no token to pin: 0, never NULL.
	other := e.principal(t, ctx)
	issued := e.create(t, ctx, other)
	if issued.Session.LegacyTokenVersion == nil || *issued.Session.LegacyTokenVersion != 0 {
		t.Fatalf("a principal without a go-api row pinned %v, want 0", issued.Session.LegacyTokenVersion)
	}
	current, err := e.store.Current(ctx, plain.Session.ID)
	if err != nil || current.LegacyTokenVersion == nil || *current.LegacyTokenVersion != 5 {
		t.Fatalf("Current reads pin %v (%v), want 5", current.LegacyTokenVersion, err)
	}
}

// A session row without a pin (older than migration 0016) is refused under its
// lock before anything is written: its credential is neither consumed nor
// revoked, and the session stays as it was.
func TestExchangeRefusesAPinlessSessionWithoutRotating(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	credential, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.New()
	e.seedPinless(t, ctx, sessionID, principal)
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, $3, now() + interval '1 hour')`, sessionID, principal, hashCredential(credential)); err != nil {
		t.Fatalf("seed its credential: %v", err)
	}

	result, err := e.store.Exchange(ctx, credential)
	if err != nil || result.Reason != ReasonRevisionStale {
		t.Fatalf("Exchange = %s, %v; want revision_stale", result.Reason, err)
	}
	if result.Credential.Reveal() != "" {
		t.Fatal("a refused exchange returned a credential")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials
		WHERE session_id = $1 AND consumed_at IS NULL AND revoked_at IS NULL`, sessionID.String()); n != 1 {
		t.Fatalf("%d untouched credentials after the refusal, want the one", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, sessionID.String()); n != 1 {
		t.Fatalf("%d credentials after the refusal, want no successor", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NULL AND last_seen_at IS NULL`, sessionID.String()); n != 1 {
		t.Fatal("the refusal changed the session row")
	}
	again, err := e.store.Exchange(ctx, credential)
	if err != nil || again.Reason != ReasonRevisionStale {
		t.Fatalf("the same credential again = %s, %v; want revision_stale, not reuse", again.Reason, err)
	}
}
