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
	is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES (gen_random_uuid(), 'projection-` + env.runtimeRole + `@example.test', NULL, NULL, NULL, 'local', NULL, true, false, false, 0, now(), now())`

	if found := posture(false); len(found) > 0 {
		t.Fatalf("posture with the switch off is not clean:\n%s", describe(found))
	}
	if err := env.runtimeStatement(ctx, insert); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("insert with the switch off: err = %v, want permission denied", err)
	}

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
	for name, statement := range map[string]string{
		"update the verifier":  `UPDATE public.users SET password_hash = NULL, updated_at = now() WHERE id = gen_random_uuid()`,
		"update is_verified":   `UPDATE public.users SET is_verified = true WHERE id = gen_random_uuid()`,
		"read after the write": `SELECT id FROM public.users WHERE id = gen_random_uuid() FOR UPDATE`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, statement := range map[string]string{
		"bump token_version": `UPDATE public.users SET token_version = token_version + 1 WHERE id = gen_random_uuid()`,
		"change the email":   `UPDATE public.users SET email = 'x@example.test' WHERE id = gen_random_uuid()`,
		"disable":            `UPDATE public.users SET is_active = false WHERE id = gen_random_uuid()`,
		"delete":             `DELETE FROM public.users WHERE id = gen_random_uuid()`,
		"truncate":           `TRUNCATE public.users`,
		"insert avatar_url":  `INSERT INTO public.users (id, email, avatar_url) VALUES (gen_random_uuid(), 'a@example.test', 'x')`,
		"write memberships":  `UPDATE public.memberships SET role = role`,
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
