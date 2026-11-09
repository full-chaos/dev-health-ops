//go:build integration

package authschema

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The upgrade from the live release: after the owner move the migrator holds the
// old refresh_tokens column privileges WITH GRANT OPTION and the table-level anchor
// privilege (the old owner pass gave both to every PublicPrivilegeSet table) and
// has granted them to the runtime role (the old grants pass). The new grants pass,
// run as the migrator, refuses the revoke with the pinned digest, then revokes all
// seven with it, and the next run needs no acknowledgement.
func TestTheLiveReleaseRefreshTokensGrantLeavesWithTheAcknowledgedDigest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))

	migrator := Quote(mustValidatedIdentifier(env.migratorRole))
	runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
	const selectCols = `SELECT (token_hash, user_id, org_id, family_id, expires_at, revoked_at)`
	for _, statement := range []string{
		`GRANT ` + selectCols + ` ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT ` + PublicAnchorPrivilege + ` ON public.refresh_tokens TO ` + migrator,
	} {
		if _, err := env.old.Exec(ctx, statement); err != nil {
			t.Fatalf("live owner pass %q: %v", statement, err)
		}
	}
	for _, statement := range []string{
		`GRANT ` + selectCols + ` ON public.refresh_tokens TO ` + runtime,
		`GRANT UPDATE (revoked_at) ON public.refresh_tokens TO ` + runtime,
	} {
		if _, err := env.migrator.Exec(ctx, statement); err != nil {
			t.Fatalf("live grants pass %q: %v", statement, err)
		}
	}
	held := func() map[string]bool {
		t.Helper()
		out := map[string]bool{}
		for _, g := range retiredRefreshTokensLost() {
			var ok bool
			if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.refresh_tokens', $2, $3)`,
				env.runtimeRole, g.Column, g.Privilege).Scan(&ok); err != nil {
				t.Fatal(err)
			}
			if ok {
				out[g.Privilege+" "+g.Column] = true
			}
		}
		return out
	}
	if got := held(); len(got) != len(retiredRefreshTokensLost()) {
		t.Fatalf("the live release state is not seeded: runtime holds %v", got)
	}

	options := env.options(true)
	_, err := Apply(ctx, env.migrator, options)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("Apply as the migrator without the acknowledgement: err = %v, want the narrowing refusal", err)
	}
	if narrowed.Digest != retiredRefreshTokensAckDigest || len(narrowed.Lost) != len(retiredRefreshTokensLost()) {
		t.Fatalf("narrowing = %s, %d lost; want %s, %d lost:\n%v", narrowed.Digest, len(narrowed.Lost), retiredRefreshTokensAckDigest, len(retiredRefreshTokensLost()), narrowed)
	}
	if got := held(); len(got) != len(retiredRefreshTokensLost()) {
		t.Fatalf("a refused run changed the grants: runtime holds %v", got)
	}

	options.AcknowledgeNarrowing = retiredRefreshTokensAckDigest
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator with the acknowledgement: %v", err)
	}
	if got := held(); len(got) != 0 {
		t.Fatalf("after the acknowledged Apply the runtime still holds %v on public.refresh_tokens", got)
	}
	if _, err := env.runtime.Exec(ctx, `SELECT token_hash FROM public.refresh_tokens LIMIT 1`); err == nil {
		t.Error("the runtime role still reads public.refresh_tokens")
	}

	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("the next Apply, without the acknowledgement: %v", err)
	}
}
