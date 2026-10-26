//go:build integration

package session

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// flipBackPreRoll runs the grants pass of the 4->3 flip-back release as its
// pre-upgrade hooks do: switch off, with the operator acknowledgement of the
// narrowing that pass reports and -defer-narrowing (the chart at phase 3).
// It returns the digest.
func (e *env) flipBackPreRoll(t *testing.T, ctx context.Context, role string) string {
	t.Helper()
	options := authschema.Options{Schema: "auth", RuntimeRole: role, DeferNarrowing: true}
	_, err := authschema.Apply(ctx, e.admin, options)
	var narrowed *authschema.NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("switch-off pass without the acknowledgement = %v, want a narrowing refusal", err)
	}
	options.AcknowledgeNarrowing = narrowed.Digest
	if _, err := authschema.Apply(ctx, e.admin, options); err != nil {
		t.Fatalf("switch-off pass with the acknowledgement: %v", err)
	}
	return narrowed.Digest
}

// An owned auth-service pod still serves while the flip-back release's
// pre-upgrade hooks run. Its sign-in stamps public.users (signin_stamp.go), so
// the pre-roll pass must leave it able to: a refused write is SQLSTATE 42501,
// which is not a dependency failure, so the API answers it 500.
func TestOwnedSignInAfterThePreRollNarrowingIsNotADependencyFailure(t *testing.T) {
	ctx := testContext(t)
	e := newEnvOwned(t, ctx, true)
	owned := e.newStore(t, e.keyText, Config{UsersOwned: true})
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	principal := e.principal(t, ctx)
	e.publicUser(t, ctx, principal)
	if _, err := owned.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal)); err != nil {
		t.Fatalf("owned sign-in before the flip-back: %v", err)
	}

	digest := e.flipBackPreRoll(t, ctx, role)

	_, err := owned.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal))
	if err != nil {
		var pgErr *pgconn.PgError
		code := "none"
		if errors.As(err, &pgErr) {
			code = pgErr.Code
		}
		t.Fatalf("owned sign-in after the flip-back pre-roll pass: SQLSTATE %s, DependencyFailure=%v (false = answered 500): %v",
			code, httpapi.DependencyFailure(err), err)
	}

	// The later narrowAfterFlipBack release: the same acknowledgement without
	// the deferral narrows, so an owned writer is refused from then on.
	if _, err := authschema.Apply(ctx, e.admin, authschema.Options{Schema: "auth", RuntimeRole: role, AcknowledgeNarrowing: digest}); err != nil {
		t.Fatalf("narrowAfterFlipBack pass with the same acknowledgement: %v", err)
	}
	_, err = owned.CreateAudited(WithSignIn(ctx), principal, nil, []string{"pwd"}, signInEvent(principal))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("owned sign-in after the narrowAfterFlipBack pass = %v, want SQLSTATE 42501 (the grants were narrowed)", err)
	}
}
