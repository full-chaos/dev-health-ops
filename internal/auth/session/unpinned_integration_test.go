//go:build integration

package session

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// seedPinless stores a live session without a pin, as a row from before the
// pin existed: the pin constraint is lifted for the insert and put back NOT
// VALID, as the migration left it.
func (e *env) seedPinless(t *testing.T, ctx context.Context, sessionID, principal uuid.UUID) {
	e.seedPinlessExpiring(t, ctx, sessionID, principal, "infinity")
}

func (e *env) seedPinlessExpiring(t *testing.T, ctx context.Context, sessionID, principal uuid.UUID, expires string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.sessions DROP CONSTRAINT sessions_pin_present`); err != nil {
		t.Fatalf("lift the pin constraint: %v", err)
	}
	_, insertErr := e.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, amr)
		VALUES ($1, $2, $3, $4::timestamptz, '{pwd}')`, sessionID, principal, hashCredential(contracts.NewSecretString("pinless-handle-"+sessionID.String())), expires)
	_, restoreErr := e.admin.Exec(ctx, `ALTER TABLE auth.sessions ADD CONSTRAINT sessions_pin_present
		CHECK (legacy_token_version IS NOT NULL OR revoked_at IS NOT NULL) NOT VALID`)
	if insertErr != nil || restoreErr != nil {
		t.Fatalf("seed a pinless session: %v / %v", insertErr, restoreErr)
	}
}

func (e *env) seedPinlessWithCredential(t *testing.T, ctx context.Context, principal uuid.UUID, expires string) uuid.UUID {
	t.Helper()
	sessionID := uuid.New()
	e.seedPinlessExpiring(t, ctx, sessionID, principal, expires)
	credential, err := newCredential()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		VALUES ($1, $2, $3, now() + interval '1 hour')`, sessionID, principal, hashCredential(credential)); err != nil {
		t.Fatalf("seed its credential: %v", err)
	}
	return sessionID
}

func (e *env) revokedState(t *testing.T, ctx context.Context, sessionID uuid.UUID) (bool, string, int) {
	t.Helper()
	var revoked bool
	var reason *string
	var liveCredentials int
	if err := e.admin.QueryRow(ctx, `SELECT revoked_at IS NOT NULL, revoked_reason FROM auth.sessions WHERE id = $1`, sessionID).Scan(&revoked, &reason); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`, sessionID).Scan(&liveCredentials); err != nil {
		t.Fatal(err)
	}
	if reason == nil {
		return revoked, "", liveCredentials
	}
	return revoked, *reason, liveCredentials
}

// A raw insert without a pin is refused by the database; so is clearing the
// pin of a live row.
func TestSessionRowWithoutAPinIsRefused(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at) VALUES ($1, $2, 'infinity')`,
		principal, hashCredential(contracts.NewSecretString("unpinned-insert"))); err == nil {
		t.Fatal("a live session without a pin was stored")
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at, revoked_at, revoked_reason)
		VALUES ($1, $2, 'infinity', now(), 'x')`, principal, hashCredential(contracts.NewSecretString("unpinned-revoked"))); err != nil {
		t.Fatalf("a revoked session without a pin was refused: %v", err)
	}
}

// Every session Create stores carries a pin, with or without a public.users
// row.
func TestCreatedSessionsAreAlwaysPinned(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	withUser := e.principal(t, ctx)
	e.publicUser(t, ctx, withUser)
	withoutUser := e.principal(t, ctx)
	for _, principal := range []uuid.UUID{withUser, withoutUser} {
		issued := e.create(t, ctx, principal)
		var pinned bool
		if err := e.admin.QueryRow(ctx, `SELECT legacy_token_version IS NOT NULL FROM auth.sessions WHERE id = $1`, issued.Session.ID.String()).Scan(&pinned); err != nil {
			t.Fatal(err)
		}
		if !pinned {
			t.Fatalf("session of %s has no pin", principal)
		}
	}
}

// RevokeUnpinned ends only the unrevoked sessions without a pin, with their
// refresh credentials; a second run changes nothing.
func TestRevokeUnpinnedTouchesOnlyUnpinnedRows(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	pinned := e.create(t, ctx, e.principal(t, ctx))
	old := e.seedPinlessWithCredential(t, ctx, e.principal(t, ctx), "infinity")
	older := e.seedPinlessWithCredential(t, ctx, e.principal(t, ctx), "2000-01-01T00:00:00Z")

	if n, err := CountUnpinned(ctx, e.admin, e.schema); err != nil || n != 2 {
		t.Fatalf("CountUnpinned = %d, %v; want 2", n, err)
	}
	if n, err := RevokeUnpinned(ctx, e.admin, e.schema); err != nil || n != 2 {
		t.Fatalf("RevokeUnpinned = %d, %v; want 2", n, err)
	}
	for _, id := range []uuid.UUID{old, older} {
		if revoked, reason, live := e.revokedState(t, ctx, id); !revoked || reason != "pin_missing" || live != 0 {
			t.Fatalf("session %s: revoked=%t reason=%q live credentials=%d", id, revoked, reason, live)
		}
	}
	if revoked, _, live := e.revokedState(t, ctx, pinned.Session.ID); revoked || live != 1 {
		t.Fatalf("pinned session: revoked=%t live credentials=%d, want a live session", revoked, live)
	}
	if n, err := RevokeUnpinned(ctx, e.admin, e.schema); err != nil || n != 0 {
		t.Fatalf("second RevokeUnpinned = %d, %v; want 0", n, err)
	}
	if n, err := CountUnpinned(ctx, e.admin, e.schema); err != nil || n != 0 {
		t.Fatalf("CountUnpinned after = %d, %v; want 0", n, err)
	}
}

// More rows than one batch are all revoked.
func TestRevokeUnpinnedRunsInBatches(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	principal := e.principal(t, ctx)
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.sessions DROP CONSTRAINT sessions_pin_present`); err != nil {
		t.Fatal(err)
	}
	total := UnpinnedBatch + 7
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (principal_id, token_hash, expires_at)
		SELECT $1, decode(lpad(to_hex(g), 64, '0'), 'hex'), 'infinity' FROM generate_series(1, $2) g`, principal, total); err != nil {
		t.Fatal(err)
	}
	if n, err := RevokeUnpinned(ctx, e.admin, e.schema); err != nil || n != total {
		t.Fatalf("RevokeUnpinned = %d, %v; want %d", n, err, total)
	}
}
