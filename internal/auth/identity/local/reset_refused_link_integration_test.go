//go:build integration

package local

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
)

// statementTracer labels every statement a pool runs by the table it reads.
type statementTracer struct{ rec callseq.Recorder }

func (s *statementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	label := "other"
	switch {
	case strings.Contains(data.SQL, "link_credentials"):
		label = "link_credentials"
	case strings.Contains(data.SQL, ".users u"):
		label = "users"
	}
	s.rec.Add(label)
	return ctx
}

func (*statementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// A refused reset link (unknown, expired, of a service-provider account, of an account that is gone) takes the
// same store statements, and answers FlowInvalidToken without a password.
func TestResetOfARefusedLinkTakesTheStatementsOfAnUnknownOne(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	tracer := &statementTracer{}
	cfg := rig.env.runtime.Config().Copy()
	cfg.ConnConfig.Tracer = tracer
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(traced.Close)
	store, err := identity.New(identity.Config{Pool: traced, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	links := NewPGLinks(traced, schema)
	flows, err := NewFlows(FlowsConfig{Accounts: IdentityAccounts{Store: store, Sessions: rig.sessions, Links: links},
		Links: links, Sessions: rig.sessions, Mail: &liveMail{}, Locker: links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}

	svc := uuid.New()
	svcEmail := "svc-" + svc.String() + "@example.com"
	if _, err := rig.env.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'service', NULL, true, true, false, 0, now(), now())`, svc, svcEmail); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BackfillUser(ctx, identity.BackfillTarget{ID: svc, Email: svcEmail, Active: true, Verified: true}, false); err != nil {
		t.Fatal(err)
	}
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: svc, Purpose: PurposePasswordReset, TokenHash: linkHash("svc-link"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	gone := rig.env.principal(t, ctx)
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: gone, Purpose: PurposePasswordReset, TokenHash: linkHash("gone-link"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	expired := rig.account(t, ctx, "Old-pass-12345")
	rig.env.preAged(t, ctx, expired, PurposePasswordReset, "expired-link", "now() - interval '1 hour'")

	const newPassword = "A-new-pass-7788"
	sequences := map[string][]string{}
	for name, secret := range map[string]string{"unknown": "no-such-link", "service-provider": "svc-link", "gone": "gone-link", "expired": "expired-link"} {
		before := len(tracer.rec.Names())
		out, err := flows.Reset(ctx, secret, newPassword)
		if err != nil || out.Kind != FlowInvalidToken {
			t.Fatalf("%s: Reset = %+v, %v; want %s", name, out, err, FlowInvalidToken)
		}
		sequences[name] = tracer.rec.Names()[before:]
	}
	if got := sequences["unknown"]; len(got) != 1 || got[0] != "link_credentials" {
		t.Fatalf("an unknown link took %v, want one read of link_credentials", got)
	}
	callseq.Same(t, "refused reset links", sequences)
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.users WHERE password_hash IS NOT NULL AND principal_id = $1`, svc); n != 0 {
		t.Errorf("the service account has a password after Reset")
	}

	ok := expired
	if err := rig.links.Issue(ctx, IssueRequest{PrincipalID: ok, Purpose: PurposePasswordReset, TokenHash: linkHash("ok-link"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if out, err := flows.Reset(ctx, "ok-link", newPassword); err != nil || out.Kind != FlowOK || out.PrincipalID != ok {
		t.Fatalf("control: Reset = %+v, %v", out, err)
	}
}
