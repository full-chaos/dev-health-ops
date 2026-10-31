//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestOrgCreateRoutesEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	user := e.account(t, ctx, acctMail(), false, true)
	onboard := func(name string) answer {
		return e.doWith(t, "/internal/v1/organizations/onboard", jsonBody(t, map[string]any{"principal_id": user.String(), "organization_name": name}), header)
	}
	first := onboard("First Org")
	if first.status != http.StatusOK {
		t.Fatalf("onboard: %d %s", first.status, first.body)
	}
	org, _ := object(t, first)["organization_id"].(string)
	var both int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM organizations p JOIN auth.organizations a ON a.id = p.id
JOIN memberships m ON m.org_id = p.id JOIN auth.memberships am ON am.id = m.id WHERE p.id = $1::uuid AND m.user_id = $2 AND am.status = 'active'`, org, user).Scan(&both); err != nil || both != 1 {
		t.Fatalf("rows in both schemas = %d, err %v", both, err)
	}
	if again := onboard("Second Org"); again.status != http.StatusConflict || object(t, again)["reason_code"] != "already_member" {
		t.Fatalf("a member's onboarding: %d %s", again.status, again.body)
	}
	super := e.account(t, ctx, acctMail(), true, true)
	other := e.account(t, ctx, acctMail(), false, true)
	if added := e.doWith(t, "/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org, "principal_id": other.String(),
		"actor_principal_id": super.String(), "role": "owner"}), header); added.status != http.StatusOK {
		t.Fatalf("add: %d %s", added.status, added.body)
	}
	if removed := e.doWith(t, "/internal/v1/memberships/remove", jsonBody(t, map[string]any{"organization_id": org, "principal_id": user.String(),
		"actor_principal_id": super.String()}), header); removed.status != http.StatusOK {
		t.Fatalf("remove: %d %s", removed.status, removed.body)
	}
	if next := onboard("Second Org"); next.status != http.StatusOK {
		t.Fatalf("a removed member's onboarding: %d %s", next.status, next.body)
	}
	slug := "e2e-" + uuid.NewString()[:8]
	created := e.doWith(t, "/internal/v1/organizations/create", jsonBody(t, map[string]any{"actor_principal_id": super.String(), "organization_name": "Admin Org", "slug": slug,
		"organization_description": "made here", "settings": map[string]any{"a": 1}, "tier": "team", "managed_by": "manual"}), header)
	if created.status != http.StatusOK {
		t.Fatalf("create: %d %s", created.status, created.body)
	}
	var kept int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM organizations p JOIN auth.organizations a ON a.id = p.id
WHERE p.slug = $1 AND p.description = 'made here' AND p.tier = 'team'`, slug).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("the created organization: %d rows, err %v", kept, err)
	}
	if refused := e.doWith(t, "/internal/v1/organizations/create", jsonBody(t, map[string]any{"actor_principal_id": user.String(), "organization_name": "No", "slug": "no-" + slug,
		"tier": "community", "managed_by": "stripe"}), header); refused.status != http.StatusForbidden {
		t.Fatalf("a create by a plain user: %d %s", refused.status, refused.body)
	}
}
