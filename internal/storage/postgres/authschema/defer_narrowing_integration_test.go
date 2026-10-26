//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func (e *publicReadsEnv) switchOff(t *testing.T, ctx context.Context, ack string, deferNarrowing bool) error {
	t.Helper()
	options := e.options(&bytes.Buffer{})
	options.AcknowledgeNarrowing, options.DeferNarrowing = ack, deferNarrowing
	_, err := Apply(ctx, e.migration, options)
	return err
}

func (e *publicReadsEnv) switchOn(t *testing.T, ctx context.Context) {
	t.Helper()
	options := e.options(&bytes.Buffer{})
	options.UsersOwned = true
	if _, err := Apply(ctx, e.migration, options); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
}

func narrowingDigestOf(t *testing.T, err error) string {
	t.Helper()
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("err = %v, want a narrowing refusal", err)
	}
	return narrowed.Digest
}

// usersOwnedHeld reports whether the role holds every grant the users-owned
// switch gives, and whether it holds none of them.
func (e *publicReadsEnv) usersOwnedHeld(t *testing.T, ctx context.Context) (all, none bool) {
	t.Helper()
	all, none = true, true
	note := func(held bool) {
		all = all && held
		none = none && !held
	}
	insert, update := UsersWriteGrant()
	for privilege, columns := range map[string][]string{"INSERT": insert, "UPDATE": update} {
		held := e.heldColumns(t, ctx, "users", privilege)
		for _, column := range columns {
			note(slices.Contains(held, column))
		}
	}
	for _, grant := range RegisterOrganizationGrants() {
		held := e.heldColumns(t, ctx, grant.Table, "INSERT")
		for _, column := range grant.Columns {
			note(slices.Contains(held, column))
		}
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		note(e.tablePrivilege(t, ctx, grant.Table, "DELETE"))
		held := e.heldColumns(t, ctx, grant.Table, "SELECT")
		for _, column := range grant.Select {
			note(slices.Contains(held, column))
		}
	}
	return all, none
}

// The flip-back release (switch off, acknowledged, deferred) keeps every
// users-owned grant and passes its posture check; the later release (switch
// off, the same acknowledgement, no deferral) narrows them.
func TestDeferredNarrowingKeepsTheUsersOwnedGrantsUntilTheLaterRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	env.switchOn(t, ctx)

	digest := narrowingDigestOf(t, env.switchOff(t, ctx, "", true))
	if all, _ := env.usersOwnedHeld(t, ctx); !all {
		t.Fatal("a refused deferred run changed the users-owned grants")
	}
	if err := env.switchOff(t, ctx, digest, true); err != nil {
		t.Fatalf("deferred run with the acknowledgement: %v", err)
	}
	if all, _ := env.usersOwnedHeld(t, ctx); !all {
		t.Fatal("the deferred run revoked a users-owned grant")
	}

	if got := narrowingDigestOf(t, env.switchOff(t, ctx, "", false)); got != digest {
		t.Fatalf("the later release's digest = %s, want the flip-back release's %s", got, digest)
	}
	if err := env.switchOff(t, ctx, digest, false); err != nil {
		t.Fatalf("later release with the acknowledgement: %v", err)
	}
	if _, none := env.usersOwnedHeld(t, ctx); !none {
		t.Fatal("the later release left a users-owned grant")
	}
	if found := env.violations(t, ctx); len(found) > 0 {
		t.Fatalf("posture after the later release is not clean:\n%s", describe(found))
	}
}

// The deferral keeps only what the users-owned switch gives: any other
// acknowledged lost grant is revoked in the same run, and stays a posture
// violation if it is put back.
func TestDeferredNarrowingStillNarrowsWhatTheSwitchDoesNotGive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	env.switchOn(t, ctx)
	grantExtra := func() {
		t.Helper()
		if _, err := env.admin.Exec(ctx, `GRANT UPDATE (full_name) ON public.users TO `+Quote(mustValidatedIdentifier(env.runtimeRole))); err != nil {
			t.Fatal(err)
		}
	}
	grantExtra()

	digest := narrowingDigestOf(t, env.switchOff(t, ctx, "", true))
	if err := env.switchOff(t, ctx, digest, true); err != nil {
		t.Fatalf("deferred run with the acknowledgement: %v", err)
	}
	if slices.Contains(env.heldColumns(t, ctx, "users", "UPDATE"), "full_name") {
		t.Fatal("the deferred run kept UPDATE(full_name), which the users-owned switch does not give")
	}
	if all, _ := env.usersOwnedHeld(t, ctx); !all {
		t.Fatal("the deferred run revoked a users-owned grant")
	}

	grantExtra()
	if err := env.switchOff(t, ctx, "", true); !errors.Is(err, ErrRuntimeGrantsNarrowed) {
		t.Fatalf("a deferred run over a new unacknowledged loss = %v, want a narrowing refusal", err)
	}
}
