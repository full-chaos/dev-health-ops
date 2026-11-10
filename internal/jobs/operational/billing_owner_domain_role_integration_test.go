//go:build integration

package operational

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
)

// The billing owner lookup runs on the domain pool. On the migrated schema and the real go-river-migrate grants, the
// domain role reads the memberships columns the lookup names and returns the earliest owner; no statement is refused.
func TestLoadOrgOwnerRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "billing_owner")
	venue.RequireReady(t, ctx)

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if _, err := venue.Admin.Exec(ctx,
		`INSERT INTO public.organizations (id, slug, name, tier, is_active, created_at, updated_at)
		 VALUES ($1, 'owner-org', 'Owner Org', 'community', TRUE, now(), now())`, fenceOrgID); err != nil {
		t.Fatal(err)
	}
	members := []struct {
		id      string
		email   string
		role    string
		created time.Time
	}{
		{"00000000-0000-4000-8000-0000000000b2", "admin@example.test", "admin", base},
		{fenceUserID, "owner@example.test", "owner", base.Add(time.Hour)},
		{"00000000-0000-4000-8000-0000000000b3", "later@example.test", "owner", base.Add(2 * time.Hour)},
	}
	for _, member := range members {
		if _, err := venue.Admin.Exec(ctx,
			`INSERT INTO public.users (id, email, full_name) VALUES ($1, $2, 'Owner Person')`,
			member.id, member.email); err != nil {
			t.Fatal(err)
		}
		if _, err := venue.Admin.Exec(ctx,
			`INSERT INTO public.memberships (id, user_id, org_id, role, created_at)
			 VALUES (gen_random_uuid(), $1, $2, $3, $4)`,
			member.id, fenceOrgID, member.role, member.created); err != nil {
			t.Fatal(err)
		}
	}

	owner, err := (&PostgresStore{pool: venue.Domain}).LoadOrgOwner(ctx, fenceOrgID)
	if err != nil {
		t.Fatalf("owner lookup as the domain role: %v (refused: %q)", err, venue.Denials.All())
	}
	if owner.Email != "owner@example.test" || owner.FullName != "Owner Person" || owner.OrgName != "Owner Org" {
		t.Errorf("owner = %+v, want the earliest owner of Owner Org", owner)
	}
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}

// The domain role reads exactly the four memberships columns the owner lookup names: no table-wide SELECT, and
// invited_by_id, joined_at and updated_at stay unreadable.
func TestDomainRoleReadsOnlyTheOwnerLookupMembershipsColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "billing_members")

	var tableWide bool
	if err := venue.Admin.QueryRow(ctx,
		`SELECT has_table_privilege($1, 'public.memberships', 'SELECT')`, venue.DomainRole).Scan(&tableWide); err != nil {
		t.Fatal(err)
	}
	if tableWide {
		t.Fatal("the domain role holds table-wide SELECT on public.memberships")
	}
	for column, want := range map[string]bool{
		"user_id": true, "org_id": true, "role": true, "created_at": true,
		"id": false, "invited_by_id": false, "joined_at": false, "updated_at": false,
	} {
		var held bool
		if err := venue.Admin.QueryRow(ctx,
			`SELECT has_column_privilege($1, 'public.memberships', $2, 'SELECT')`, venue.DomainRole, column).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held != want {
			t.Errorf("domain role SELECT on memberships.%s = %t, want %t", column, held, want)
		}
	}
	// DELETE has no column form: has_any_column_privilege rejects it (22023), so it is checked table-wide only.
	for privilege, query := range map[string]string{
		"INSERT": `SELECT has_any_column_privilege($1, 'public.memberships', $2) OR has_table_privilege($1, 'public.memberships', $2)`,
		"UPDATE": `SELECT has_any_column_privilege($1, 'public.memberships', $2) OR has_table_privilege($1, 'public.memberships', $2)`,
		"DELETE": `SELECT has_table_privilege($1, 'public.memberships', $2)`,
	} {
		var held bool
		if err := venue.Admin.QueryRow(ctx, query, venue.DomainRole, privilege).Scan(&held); err != nil {
			t.Fatal(err)
		}
		if held {
			t.Errorf("domain role holds %s on public.memberships", privilege)
		}
	}
}
