//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func (e *publicReadsEnv) heldColumns(t *testing.T, ctx context.Context, table, privilege string) []string {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT a.attname FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped
  AND has_column_privilege($2, c.oid, a.attnum, $3) ORDER BY a.attname`, table, e.runtimeRole, privilege)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out = append(out, name)
	}
	return out
}

func (e *publicReadsEnv) tablePrivilege(t *testing.T, ctx context.Context, table, privilege string) bool {
	t.Helper()
	var held bool
	if err := e.admin.QueryRow(ctx, `SELECT has_table_privilege($1, format('public.%I', $2::text), $3)`, e.runtimeRole, table, privilege).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

func TestRegisterOrganizationGrantIsExactlyTheDeclaredInsertColumnsWhenOwnedAndNoneWhenOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	for _, grant := range RegisterOrganizationGrants() {
		if got := env.heldColumns(t, ctx, grant.Table, "INSERT"); len(got) != 0 {
			t.Fatalf("%s: INSERT held with the switch off: %v", grant.Table, got)
		}
	}
	owned := env.options(&bytes.Buffer{})
	owned.UsersOwned = true
	if _, err := Apply(ctx, env.migration, owned); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
	for _, grant := range RegisterOrganizationGrants() {
		want := append([]string(nil), grant.Columns...)
		if grant.Table == MembershipWriteGrants().Table {
			// MembershipWriteGrants' INSERT (invited_by_id), switch-gated: acp ruling 10-29 01:2xZ.
			want = append(want, MembershipWriteGrants().Insert...)
		}
		sort.Strings(want)
		if got := env.heldColumns(t, ctx, grant.Table, "INSERT"); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: INSERT columns = %v, want %v", grant.Table, got, want)
		}
		// memberships also carries MembershipWriteGrants (PLAN-v4 section 2, CHAOS-10982):
		// UPDATE (role, updated_at) and table DELETE, with the switch on only.
		member := grant.Table == MembershipWriteGrants().Table
		for _, privilege := range []string{"UPDATE", "REFERENCES"} {
			var want []string
			if member && privilege == "UPDATE" {
				want = append(want, MembershipWriteGrants().Update...)
				sort.Strings(want)
			}
			if got := env.heldColumns(t, ctx, grant.Table, privilege); len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
				t.Fatalf("%s: %s held on %v, want %v", grant.Table, privilege, got, want)
			}
		}
		for _, privilege := range []string{"DELETE", "TRUNCATE", "INSERT", "UPDATE"} {
			want := member && privilege == "DELETE"
			if got := env.tablePrivilege(t, ctx, grant.Table, privilege); got != want {
				t.Fatalf("%s: table-level %s held = %v, want %v", grant.Table, privilege, got, want)
			}
		}
	}
	conn, err := env.migration.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found, err := VerifyRuntimePosture(ctx, conn.Conn(), owned)
	conn.Release()
	if err != nil || len(found) > 0 {
		t.Fatalf("posture with the switch on: %v\n%s", err, describe(found))
	}

	off := env.options(&bytes.Buffer{})
	_, err = Apply(ctx, env.migration, off)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		t.Fatalf("turning the switch off: err = %v, want the narrowing refusal", err)
	}
	off.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migration, off); err != nil {
		t.Fatalf("turning the switch off with the acknowledgement: %v", err)
	}
	for _, grant := range RegisterOrganizationGrants() {
		for _, privilege := range []string{"INSERT", "UPDATE"} {
			if got := env.heldColumns(t, ctx, grant.Table, privilege); len(got) != 0 {
				t.Fatalf("%s: %s held after the switch went off: %v", grant.Table, privilege, got)
			}
		}
		if env.tablePrivilege(t, ctx, grant.Table, "DELETE") {
			t.Fatalf("%s: table-level DELETE held after the switch went off", grant.Table)
		}
	}
}

func TestRegisterOrganizationGrantRefusesAnUndeclaredColumnAndAnyUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPublicReadsFixture(t, ctx)
	owned := env.options(&bytes.Buffer{})
	owned.UsersOwned = true
	if _, err := Apply(ctx, env.migration, owned); err != nil {
		t.Fatalf("Apply with the switch on: %v", err)
	}
	for name, statement := range map[string]string{
		"organizations.description":        `INSERT INTO public.organizations (id, slug, name, description) VALUES (gen_random_uuid(), 's-1', 'n', 'x')`,
		"organizations.stripe_customer_id": `INSERT INTO public.organizations (id, slug, name, stripe_customer_id) VALUES (gen_random_uuid(), 's-2', 'n', 'x')`,
		"memberships.user_id update":       `UPDATE public.memberships SET user_id = user_id`,
		"update an organization":           `UPDATE public.organizations SET name = 'z' WHERE id = gen_random_uuid()`,
		"update a membership's org":        `UPDATE public.memberships SET org_id = gen_random_uuid() WHERE id = gen_random_uuid()`,
		"delete an organization":           `DELETE FROM public.organizations WHERE id = gen_random_uuid()`,
		"truncate the memberships":         `TRUNCATE public.memberships`,
	} {
		if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("%s: err = %v, want permission denied", name, err)
		}
	}
	// A membership's role change and removal are MembershipWriteGrants (PLAN-v4 section 2, CHAOS-10982).
	for name, statement := range map[string]string{
		"update a membership's role": `UPDATE public.memberships SET role = 'owner', updated_at = now() WHERE id = gen_random_uuid()`,
		"delete a membership":        `DELETE FROM public.memberships WHERE id = gen_random_uuid()`,
	} {
		if err := env.runtimeStatement(ctx, statement); err != nil {
			t.Errorf("%s with the switch on: %v", name, err)
		}
	}
	// invited_by_id is MembershipWriteGrants' INSERT when owned (acp ruling 10-29 01:2xZ); the random
	// ids then fail the foreign keys, never the privilege check. Off, the WhenOwnedAndNoneWhenOff test pins none.
	if err := env.runtimeStatement(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'member', gen_random_uuid(), now(), now(), now())`); err == nil || strings.Contains(err.Error(), "permission denied") {
		t.Errorf("insert with invited_by_id with the switch on: err = %v, want a foreign-key refusal, not permission denied", err)
	}
	if err := env.runtimeStatement(ctx, `INSERT INTO public.organizations (id, slug, name, settings, tier, managed_by, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), 'declared-columns', 'n', '{}', 'community', 'stripe', true, now(), now())`); err != nil {
		t.Fatalf("an insert of the declared organization columns: %v", err)
	}
}
