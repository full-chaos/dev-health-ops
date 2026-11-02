//go:build integration

package authschema

import (
	"bytes"
	"context"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

func (e *publicReadsEnv) applyOwned(t *testing.T, ctx context.Context, orgsOwned bool) Options {
	t.Helper()
	options := e.options(&bytes.Buffer{})
	options.UsersOwned, options.OrgsOwned = true, orgsOwned
	if _, err := Apply(ctx, e.migration, options); err != nil {
		t.Fatalf("Apply (users owned, orgs owned %v): %v", orgsOwned, err)
	}
	return options
}

func (e *publicReadsEnv) postureClean(t *testing.T, ctx context.Context, options Options) {
	t.Helper()
	conn, err := e.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
	if err != nil || len(found) > 0 {
		t.Fatalf("posture (orgs owned %v): %v\n%s", options.OrgsOwned, err, describe(found))
	}
}

// With AUTH_ORGS_OWNED the runtime role may UPDATE exactly name, is_active and updated_at of public.organizations,
// never the whole table; with users owned alone it may update none, and turning the switch off is a narrowing.
func TestOrgGrantIsThreeColumnsWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	usersOnly := env.applyOwned(t, ctx, false)
	if got := env.heldColumns(t, ctx, "organizations", "UPDATE"); len(got) != 0 {
		t.Fatalf("UPDATE held on public.organizations with users owned alone: %v", got)
	}
	env.postureClean(t, ctx, usersOnly)

	owned := env.applyOwned(t, ctx, true)
	want := append([]string(nil), OrganizationWriteGrants().Update...)
	sort.Strings(want)
	if got := env.heldColumns(t, ctx, "organizations", "UPDATE"); !reflect.DeepEqual(got, want) {
		t.Fatalf("UPDATE columns on public.organizations = %v, want %v", got, want)
	}
	for _, privilege := range []string{"UPDATE", "DELETE", "TRUNCATE", "INSERT"} {
		if env.tablePrivilege(t, ctx, "organizations", privilege) {
			t.Fatalf("table-level %s held on public.organizations with orgs owned", privilege)
		}
	}
	env.postureClean(t, ctx, owned)
	for name, statement := range map[string]string{
		"rename":      `UPDATE public.organizations SET name = 'n', updated_at = now() WHERE id = gen_random_uuid()`,
		"active flag": `UPDATE public.organizations SET is_active = false, updated_at = now() WHERE id = gen_random_uuid()`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("%s with orgs owned: %v", name, err)
		}
	}
	for name, statement := range map[string]string{
		"tier":        `UPDATE public.organizations SET tier = 'enterprise' WHERE id = gen_random_uuid()`,
		"slug":        `UPDATE public.organizations SET slug = 's' WHERE id = gen_random_uuid()`,
		"description": `UPDATE public.organizations SET description = 'd' WHERE id = gen_random_uuid()`,
		"stripe id":   `UPDATE public.organizations SET stripe_customer_id = 'c' WHERE id = gen_random_uuid()`,
		"delete":      `DELETE FROM public.organizations WHERE id = gen_random_uuid()`,
	} {
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s with orgs owned: err = %v, want permission denied", name, err)
		}
	}

	off := env.options(&bytes.Buffer{})
	off.UsersOwned = true
	_, err := Apply(ctx, env.migration, off)
	off.AcknowledgeNarrowing = narrowingDigestOf(t, err)
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("turning orgs owned off with the acknowledgement: %v", err)
	}
	if got := env.heldColumns(t, ctx, "organizations", "UPDATE"); len(got) != 0 {
		t.Fatalf("UPDATE held on public.organizations after the switch went off: %v", got)
	}
}

// The posture check names a missing column of the grant while the switch is on.
func TestOrgGrantMissingColumnIsAPostureViolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	owned := env.applyOwned(t, ctx, true)
	if _, err := env.admin.Exec(ctx, `REVOKE UPDATE (is_active) ON public.organizations FROM `+quoteIdentifier(mustValidatedIdentifier(env.runtimeRole))); err != nil {
		t.Fatal(err)
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), owned)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(found, func(v Violation) bool {
		return v.Object == "organizations.is_active" && v.Privilege == "UPDATE"
	}) {
		t.Fatalf("posture after revoking UPDATE (is_active): %s, want the missing column named", describe(found))
	}
}

// The 5->4 flip-back release (switch off, acknowledged, deferred) keeps the organization write grant, as it keeps the
// users-owned grants, while old owned pods still serve.
func TestDeferredNarrowingKeepsTheOrgWriteGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	env.applyOwned(t, ctx, true)
	back := env.options(&bytes.Buffer{})
	back.UsersOwned = true
	_, err := Apply(ctx, env.migration, back)
	back.AcknowledgeNarrowing, back.DeferNarrowing = narrowingDigestOf(t, err), true
	if _, err := Apply(ctx, env.migration, back); err != nil {
		t.Fatalf("deferred flip-back: %v", err)
	}
	want := append([]string(nil), OrganizationWriteGrants().Update...)
	sort.Strings(want)
	if got := env.heldColumns(t, ctx, "organizations", "UPDATE"); !reflect.DeepEqual(got, want) {
		t.Fatalf("UPDATE columns after the deferred flip-back = %v, want %v kept", got, want)
	}
}
