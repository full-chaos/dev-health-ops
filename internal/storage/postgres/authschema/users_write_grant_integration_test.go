//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestUsersWriteGrantFollowsTheSwitch: with the switch on the runtime role
// holds INSERT and UPDATE on exactly UsersWriteGrant's columns of public.users
// and nothing else there; with it off it holds neither, and turning it off is a
// narrowing that needs its acknowledgement.
func TestUsersWriteGrantFollowsTheSwitch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	posture := func(owned bool) []Violation {
		t.Helper()
		conn, err := env.migration.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		options := env.options(&bytes.Buffer{})
		options.UsersOwned = owned
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		return found
	}
	insert := `INSERT INTO public.users (id, email, username, password_hash, full_name, auth_provider, auth_provider_id,
	is_active, is_verified, token_version, created_at, updated_at)
VALUES (gen_random_uuid(), 'projection-` + env.runtimeRole + `@example.test', NULL, NULL, NULL, 'local', NULL, true, false, 0, now(), now())`
	// CHAOS-10993: the runtime role writes no is_superuser in either state.
	superuserWrites := map[string]string{
		"insert is_superuser": `INSERT INTO public.users (id, email, is_superuser) VALUES (gen_random_uuid(), 's-` + env.runtimeRole + `@example.test', false)`,
		"update is_superuser": `UPDATE public.users SET is_superuser = true WHERE id = gen_random_uuid()`,
	}
	refusesSuperuserWrites := func(state string) {
		t.Helper()
		for name, statement := range superuserWrites {
			if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s, %s: err = %v, want permission denied", state, name, err)
			}
		}
	}

	if found := posture(false); len(found) > 0 {
		t.Fatalf("posture with the switch off is not clean:\n%s", describe(found))
	}
	if err := env.runtimeStatement(ctx, insert); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("insert with the switch off: err = %v, want permission denied", err)
	}
	refusesSuperuserWrites("switch off")

	options := env.options(&bytes.Buffer{})
	options.UsersOwned = true
	if _, err := Apply(ctx, env.migration, options); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
	if found := posture(true); len(found) > 0 {
		t.Fatalf("posture with the switch on is not clean:\n%s", describe(found))
	}
	if err := env.runtimeStatement(ctx, insert); err != nil {
		t.Fatalf("insert with the switch on: %v", err)
	}
	refusesSuperuserWrites("switch on")
	for name, statement := range map[string]string{
		"update the verifier":  `UPDATE public.users SET password_hash = NULL, updated_at = now() WHERE id = gen_random_uuid()`,
		"update is_verified":   `UPDATE public.users SET is_verified = true WHERE id = gen_random_uuid()`,
		"bump token_version":   `UPDATE public.users SET token_version = token_version + 1 WHERE id = gen_random_uuid()`,
		"read after the write": `SELECT id FROM public.users WHERE id = gen_random_uuid() FOR UPDATE`,
		"change the email":     `UPDATE public.users SET email = 'x@example.test' WHERE id = gen_random_uuid()`,
		"disable":              `UPDATE public.users SET is_active = false WHERE id = gen_random_uuid()`,
		"change the username":  `UPDATE public.users SET username = 'x' WHERE id = gen_random_uuid()`,
		"change the full_name": `UPDATE public.users SET full_name = 'x' WHERE id = gen_random_uuid()`,
		"change the avatar":    `UPDATE public.users SET avatar_url = 'x' WHERE id = gen_random_uuid()`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, statement := range map[string]string{
		"update sso_provider_id": `UPDATE public.users SET sso_provider_id = NULL WHERE id = gen_random_uuid()`,
		"delete":                 `DELETE FROM public.users WHERE id = gen_random_uuid()`,
		"truncate":               `TRUNCATE public.users`,
		"insert avatar_url":      `INSERT INTO public.users (id, email, avatar_url) VALUES (gen_random_uuid(), 'a@example.test', 'x')`,
		"write memberships":      `UPDATE public.memberships SET user_id = user_id`,
	} {
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: err = %v, want permission denied", name, err)
		}
	}
	if found := describe(posture(false)); !strings.Contains(found, "users.password_hash") || !strings.Contains(found, "UPDATE") {
		t.Errorf("the write grant was not reported with the switch off:\n%s", found)
	}

	off := env.options(&bytes.Buffer{})
	_, err := Apply(ctx, env.migration, off)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("turning the switch off: err = %v, want the narrowing refusal", err)
	}
	off.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("turning the switch off with the acknowledgement: %v", err)
	}
	if found := posture(false); len(found) > 0 {
		t.Fatalf("posture after the revoke is not clean:\n%s", describe(found))
	}
	if err := env.runtimeStatement(ctx, insert); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("insert after the revoke: err = %v, want permission denied", err)
	}
}

// TestUsersWriteGrantNarrowsTheOldSuperuserInsert: a runtime role that still
// holds INSERT (is_superuser) on public.users (the grant before CHAOS-10993)
// loses it on the next grants pass with the switch on. The loss is a narrowing:
// without the acknowledgement the pass refuses and names the digest; a deferred
// pass does not keep it (no auth build after #5531 inserts the column); with
// the acknowledgement it goes and the posture is clean.
func TestUsersWriteGrantNarrowsTheOldSuperuserInsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	options := env.options(&bytes.Buffer{})
	options.UsersOwned = true
	if _, err := Apply(ctx, env.migration, options); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
	holdsSuperuserInsert := func() bool {
		t.Helper()
		var held bool
		if err := env.migration.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.users', 'is_superuser', 'INSERT')`, env.runtimeRole).Scan(&held); err != nil {
			t.Fatal(err)
		}
		return held
	}
	if holdsSuperuserInsert() {
		t.Fatal("the grants pass gave INSERT (is_superuser)")
	}
	if _, err := env.migration.Exec(ctx, `GRANT INSERT (is_superuser) ON TABLE public.users TO `+quoteIdentifier(mustValidatedIdentifier(env.runtimeRole))); err != nil {
		t.Fatal(err)
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	conn.Release()
	if err != nil {
		t.Fatalf("VerifyRuntimePosture: %v", err)
	}
	if got := describe(found); !strings.Contains(got, "is_superuser") {
		t.Errorf("the old INSERT (is_superuser) is not reported as excess:\n%s", got)
	}

	_, err = Apply(ctx, env.migration, options)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("grants pass over the old grant: err = %v, want the narrowing refusal", err)
	}
	lostSuperuser := false
	for _, lost := range narrowed.Lost {
		if lost.Kind == "column" && lost.Column == "is_superuser" && lost.Privilege == "INSERT" {
			lostSuperuser = true
		}
	}
	if !lostSuperuser || narrowed.Digest == "" || !strings.Contains(err.Error(), narrowed.Digest) {
		t.Fatalf("the refusal does not name INSERT (is_superuser) and its digest: %v", err)
	}
	if !holdsSuperuserInsert() {
		t.Fatal("a refused pass changed the grants")
	}
	wrong := options
	wrong.AcknowledgeNarrowing = strings.Repeat("0", len(narrowed.Digest))
	if _, err := Apply(ctx, env.migration, wrong); !errors.As(err, &narrowed) {
		t.Fatalf("a wrong acknowledgement: err = %v, want the narrowing refusal", err)
	}

	deferred := options
	deferred.DeferNarrowing = true
	deferred.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, deferred); err != nil {
		t.Fatalf("deferred pass with the acknowledgement: %v", err)
	}
	if holdsSuperuserInsert() {
		t.Fatal("a deferred pass kept INSERT (is_superuser)")
	}
	conn, err = env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if found, err := VerifyRuntimePosture(ctx, conn.Conn(), options); err != nil || len(found) > 0 {
		t.Fatalf("posture after the narrowing: %v\n%s", err, describe(found))
	}
}
