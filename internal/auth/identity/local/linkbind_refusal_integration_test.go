//go:build integration

package local

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// preAgedBound is preAged with the row bound to the principal's current
// public.users.token_version, as IssueBound stores it.
func (e *pgEnv) preAgedBound(t *testing.T, ctx context.Context, p uuid.UUID, purpose Purpose, secret, expires string) {
	t.Helper()
	h := linkHash(secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, created_at, issued_token_version)
		VALUES ($1, $2, $3, `+expires+`, now() - interval '3 hours', (SELECT token_version FROM public.users WHERE id = $1))`, p.String(), string(purpose), h[:]); err != nil {
		t.Fatalf("seed bound credential: %v", err)
	}
}

// requireBindingPasses fails unless the link of secret is bound to its
// principal's current public.users.token_version, so a refusal of it comes from
// another predicate.
func (e *pgEnv) requireBindingPasses(t *testing.T, ctx context.Context, secret string) {
	t.Helper()
	h := linkHash(secret)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.link_credentials c
WHERE c.token_hash = $1 AND c.issued_token_version = (SELECT u.token_version FROM public.users u WHERE u.id = c.principal_id)`, h[:]); n != 1 {
		t.Fatalf("link %q: rows bound to the current token_version = %d, want 1", secret, n)
	}
}

func consumeBound(ctx context.Context, rig *endRig, id uuid.UUID, secret string) error {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return err
	}
	subject := id.String()
	return audit.Commit(ctx, rig.env.runtime, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			return rig.links.ConsumeBoundResetLinkInTx(ctx, tx, id, linkHash(secret))
		},
		Audit:     audit.AuditEvent{EventType: "auth.password.reset", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject},
		OffStream: audit.OffStreamLinkCredentialLifecycle,
	})
}

// TestAResetLinkOfAUserWithoutAPublicRowIsRefusedInPeekAndConsume: a link
// stored with issued_token_version 0 is refused by PeekResetLink and by
// ConsumeBoundResetLinkInTx once the principal's public.users row is gone: the
// counter reads NULL, not 0. The control link of a user whose row is there
// passes both.
func TestAResetLinkOfAUserWithoutAPublicRowIsRefusedInPeekAndConsume(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	provision := func() uuid.UUID {
		hash, err := cheapHash("Old-pass-12345")
		if err != nil {
			t.Fatal(err)
		}
		id, err := rig.accounts.ProvisionLocal(ctx, ProvisionLocalRequest{Email: uuid.NewString() + "@example.com", PasswordHash: hash})
		if err != nil {
			t.Fatalf("ProvisionLocal: %v", err)
		}
		return id
	}
	issue := func(id uuid.UUID, secret string) {
		if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: id, Purpose: PurposePasswordReset, TokenHash: linkHash(secret), TTL: ResetTTL}); err != nil {
			t.Fatal(err)
		}
		if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND issued_token_version = 0`, id); n != 1 {
			t.Fatalf("links stored with issued_token_version 0 = %d, want 1", n)
		}
	}

	kept, gone := provision(), provision()
	issue(kept, "kept-link")
	issue(gone, "gone-link")
	if _, ok, err := rig.links.PeekResetLink(ctx, linkHash("gone-link")); err != nil || !ok {
		t.Fatalf("PeekResetLink before the public row is removed = ok %v, %v; want ok", ok, err)
	}
	if _, err := rig.env.admin.Exec(ctx, `DELETE FROM public.users WHERE id = $1`, gone); err != nil {
		t.Fatalf("remove the public row: %v", err)
	}

	if _, ok, err := rig.links.PeekResetLink(ctx, linkHash("gone-link")); err != nil || ok {
		t.Errorf("PeekResetLink without a public row = ok %v, %v; want unknown", ok, err)
	}
	if out, err := rig.flows.Reset(ctx, "gone-link", "A-new-pass-7788"); err != nil || out.Kind != FlowInvalidToken {
		t.Errorf("Reset without a public row = %+v, %v; want %s", out, err, FlowInvalidToken)
	}
	if err := consumeBound(ctx, rig, gone, "gone-link"); !errors.Is(err, identity.ErrLinkUnusable) {
		t.Errorf("ConsumeBoundResetLinkInTx without a public row = %v, want %v", err, identity.ErrLinkUnusable)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, gone); n != 0 {
		t.Errorf("the refused link was consumed (%d)", n)
	}

	// Control: the consume runs after the password write raised the counter by one.
	if _, ok, err := rig.links.PeekResetLink(ctx, linkHash("kept-link")); err != nil || !ok {
		t.Fatalf("control PeekResetLink = ok %v, %v; want ok", ok, err)
	}
	if _, err := rig.env.admin.Exec(ctx, `UPDATE public.users SET token_version = token_version + 1 WHERE id = $1`, kept); err != nil {
		t.Fatal(err)
	}
	if err := consumeBound(ctx, rig, kept, "kept-link"); err != nil {
		t.Errorf("control ConsumeBoundResetLinkInTx at counter + 1 = %v, want nil", err)
	}
}
