//go:build integration

package apiservice

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

// With AUTH_PROXY_ORG_PROVISIONING on, a superuser's DELETE /api/v1/admin/orgs/{org_id} deprovisions the organization
// in auth-service before the purge: the answer is 200 with the deletion report, the auth row stays, suspended, with
// its slug freed, no live auth membership, invite or session is left, go-api's public rows are gone, and the same name
// and slug can be taken by a new organization that signs in on both planes. A repeat DELETE of the deleted id
// answers 200 and changes nothing.
func TestOrgDeleteSwitchOnFreesNameAndSlug(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	s := newSlugStack(t, ctx, "true")
	name := "Switch on delete " + uuid.NewString()[:8]
	ownerX, emailX := s.user(t, ctx, "x")
	x, slugX := s.create(t, name, "", ownerX)
	wantSignedIn(t, s, ctx, "X", emailX, ownerX, x)

	publicRows := func(id uuid.UUID) (orgs, memberships int) {
		t.Helper()
		return s.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE id = $1`, id),
			s.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1`, id)
	}
	if o, m := publicRows(x); o != 1 || m != 1 {
		t.Fatalf("before the delete: public org rows=%d memberships=%d, want 1 and 1", o, m)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NULL`, x, slugX); n != 1 {
		t.Fatalf("before the delete: %d live auth rows with slug %q, want 1", n, slugX)
	}

	status, answer := s.goAPICall(t, http.MethodDelete, "/api/v1/admin/orgs/"+x.String()+"?dry_run=true", "")
	if status != http.StatusOK || answer["dry_run"] != true {
		t.Fatalf("dry run: %d %v, want 200 with dry_run true", status, answer)
	}
	if o, m := publicRows(x); o != 1 || m != 1 {
		t.Fatalf("after the dry run: public org rows=%d memberships=%d, want 1 and 1", o, m)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL`, x); n != 1 {
		t.Fatalf("after the dry run: %d live auth rows, want 1", n)
	}

	status, answer = s.goAPICall(t, http.MethodDelete, "/api/v1/admin/orgs/"+x.String(), "")
	if status != http.StatusOK {
		t.Fatalf("DELETE: %d %v, want 200", status, answer)
	}
	if answer["organization_id"] != x.String() || answer["dry_run"] != false {
		t.Fatalf("DELETE answer organization_id=%v dry_run=%v, want %s and false", answer["organization_id"], answer["dry_run"], x)
	}
	if _, ok := answer["postgres"].(map[string]any); !ok {
		t.Fatalf("DELETE answer has no postgres report: %v", answer)
	}
	if _, ok := answer["clickhouse"].(map[string]any); !ok {
		t.Fatalf("DELETE answer has no clickhouse report: %v", answer)
	}
	if warnings, _ := answer["warnings"].([]any); len(warnings) != 0 {
		t.Fatalf("DELETE answer warnings = %v, want none", warnings)
	}

	deleted := "~deleted~" + x.String()
	authAfter := func() {
		t.Helper()
		if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND slug = $2`, x, deleted); n != 1 {
			t.Errorf("auth.organizations: %d suspended rows with slug %q for the deleted org, want 1", n, deleted)
		}
		if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE slug = $1`, slugX); n != 0 {
			t.Errorf("auth.organizations: %d rows still hold slug %q", n, slugX)
		}
		if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed'`, x); n != 0 {
			t.Errorf("%d live auth memberships in the deleted org, want 0", n)
		}
		if n := s.count(t, ctx, `SELECT count(*) FROM auth.invites WHERE organization_id = $1 AND status = 'pending'`, x); n != 0 {
			t.Errorf("%d pending auth invites in the deleted org, want 0", n)
		}
		if n := s.liveSessionsIn(t, ctx, ownerX, x); n != 0 {
			t.Errorf("%d live auth sessions in the deleted org, want 0", n)
		}
		if o, m := publicRows(x); o != 0 || m != 0 {
			t.Errorf("public rows after the delete: organizations=%d memberships=%d, want 0 and 0", o, m)
		}
	}
	authAfter()

	ownerY, emailY := s.user(t, ctx, "y")
	y, slugY := s.create(t, name, "", ownerY)
	if y == x || slugY != slugX {
		t.Fatalf("re-created org id=%s slug=%q, want a new id and slug %q", y, slugY, slugX)
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NULL`, y, slugY); n != 1 {
		t.Fatalf("new org's active auth rows with slug %q: %d, want 1", slugY, n)
	}
	wantSignedIn(t, s, ctx, "Y", emailY, ownerY, y)
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, y, ownerX); n != 0 {
		t.Fatalf("the deleted org's owner holds %d auth memberships in the new org, want 0", n)
	}

	status, answer = s.goAPICall(t, http.MethodDelete, "/api/v1/admin/orgs/"+x.String(), "")
	if status != http.StatusOK {
		t.Fatalf("repeat DELETE: %d %v, want 200", status, answer)
	}
	authAfter()
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NULL`, y, slugY); n != 1 {
		t.Fatalf("after the repeat delete the new org's active auth rows: %d, want 1", n)
	}
}
