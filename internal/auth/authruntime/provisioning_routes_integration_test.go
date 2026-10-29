//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

const (
	provisioningInviteSecret   = "provisioning-routes-invite-signing-key"
	provisioningInvitePrevious = "provisioning-routes-invite-previous-key"
)

// newProvisioningEnv is the real stack with users owned and the provisioning
// routes on, and a go-api credential holding both of its scopes.
func newProvisioningEnv(t *testing.T, ctx context.Context) (*routesEnv, map[string]string) {
	t.Helper()
	e := newRoutesEnvWith(t, ctx, map[string]string{
		authconfig.EnvUsersOwned:                "true",
		authconfig.EnvMembershipRoutes:          "true",
		authconfig.EnvInviteTokenSecret:         provisioningInviteSecret,
		authconfig.EnvInviteTokenSecretPrevious: provisioningInvitePrevious,
	})
	token, _ := e.issuerCredential(t, ctx, "auth-session-issuer", `["memberships:write", "sessions:issue"]`, false, false)
	return e, map[string]string{"Authorization": "Bearer " + token, "Content-Type": "application/json"}
}

// account inserts a go-api user and projects its principal.
func (e *routesEnv) account(t *testing.T, ctx context.Context, email string, superuser, active bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser,
token_version, created_at, updated_at) VALUES ($1, $2, NULL, 'local', $3, true, $4, 0, now(), now())`, id, email, active, superuser); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("project: %v", err)
	}
	return id
}

func acctMail() string { return "acct-" + uuid.NewString()[:8] + "@example.com" }

func jsonBody(t *testing.T, body map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func (e *routesEnv) memberRows(t *testing.T, ctx context.Context, org, user uuid.UUID) (publicRole, authStatus string) {
	t.Helper()
	if err := e.admin.QueryRow(ctx, `SELECT
  COALESCE((SELECT role FROM public.memberships WHERE org_id = $1 AND user_id = $2), ''),
  COALESCE((SELECT status FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2), '')`, org, user).
		Scan(&publicRole, &authStatus); err != nil {
		t.Fatal(err)
	}
	return publicRole, authStatus
}

// The routes through configure, as the runtime role: only the go-api
// credential holding memberships:write is served; a member is added, changed
// and removed in both tables with its audit row naming the actor; go-api's
// PEP rule is checked again by auth-service.
func TestProvisioningMembershipRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	member := e.account(t, ctx, acctMail(), false, true)
	add := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": member.String(), "actor_principal_id": owner.String(), "role": "member"})

	for name, scopes := range map[string]string{"sessions:issue only": `["sessions:issue"]`, "no scope": `[]`} {
		token, _ := e.issuerCredential(t, ctx, "auth-session-issuer", scopes, false, false)
		if got := e.doWith(t, "/internal/v1/memberships/add", add, map[string]string{"Authorization": "Bearer " + token}); got.status != http.StatusUnauthorized {
			t.Fatalf("%s: %d %s, want 401", name, got.status, got.body)
		}
	}
	for name, cred := range map[string][2]any{"revoked": {true, false}, "expired": {false, true}} {
		token, _ := e.issuerCredential(t, ctx, "auth-session-issuer", `["memberships:write"]`, cred[0].(bool), cred[1].(bool))
		if got := e.doWith(t, "/internal/v1/memberships/add", add, map[string]string{"Authorization": "Bearer " + token}); got.status != http.StatusUnauthorized {
			t.Fatalf("%s: %d, want 401", name, got.status)
		}
	}
	// memberships:write alone does not issue sessions.
	writeOnly, _ := e.issuerCredential(t, ctx, "auth-session-issuer", `["memberships:write"]`, false, false)
	issue := `{"principal_id":"` + owner.String() + `","organization_id":null,"amr":["pwd"]}`
	if got := e.doWith(t, "/internal/v1/sessions", issue, map[string]string{"Authorization": "Bearer " + writeOnly}); got.status != http.StatusUnauthorized {
		t.Fatalf("session issue with memberships:write only: %d, want 401", got.status)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "" || auth != "" {
		t.Fatalf("refused credentials wrote rows: %q %q", pub, auth)
	}

	got := e.doWith(t, "/internal/v1/memberships/add", add, header)
	if got.status != http.StatusOK {
		t.Fatalf("add: %d %s", got.status, got.body)
	}
	if out := object(t, got); out["role"] != "member" || out["status"] != "active" || out["principal_id"] != member.String() {
		t.Fatalf("add answer = %v", out)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "member" || auth != "active" {
		t.Fatalf("after add: public %q auth %q", pub, auth)
	}
	var actor string
	if err := e.admin.QueryRow(ctx, `SELECT actor_principal_id::text FROM auth.security_audit_events
WHERE event_type = 'auth.membership.added' AND organization_id = $1 AND subject_principal_id = $2`, org, member).Scan(&actor); err != nil || actor != owner.String() {
		t.Fatalf("added audit actor = %q (%v), want the owner", actor, err)
	}
	role := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": member.String(), "actor_principal_id": owner.String(), "role": "admin"})
	if got := e.doWith(t, "/internal/v1/memberships/role", role, header); got.status != http.StatusOK {
		t.Fatalf("role: %d %s", got.status, got.body)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "admin" || auth != "active" {
		t.Fatalf("after role: public %q auth %q", pub, auth)
	}

	// go-api's PEP rule, again in auth-service.
	viewer := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, viewer, owner, "viewer")
	inactive := e.account(t, ctx, acctMail(), false, false)
	super := e.account(t, ctx, acctMail(), true, true)
	e.addVia(t, header, org, super, super, "member")
	third := e.account(t, ctx, acctMail(), false, true)
	for name, tc := range map[string]struct {
		actor, target uuid.UUID
		reason        string
	}{
		"viewer actor":            {viewer, third, "admin_required"},
		"inactive actor":          {inactive, third, "principal_deactivated"},
		"a stranger to the org":   {third, viewer, "admin_required"},
		"unknown actor principal": {uuid.New(), third, "principal_deactivated"},
	} {
		body := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": tc.target.String(), "actor_principal_id": tc.actor.String(), "role": "member"})
		got := e.doWith(t, "/internal/v1/memberships/add", body, header)
		if got.status != http.StatusForbidden || object(t, got)["reason_code"] != tc.reason {
			t.Fatalf("%s: %d %s, want 403 %s", name, got.status, got.body, tc.reason)
		}
	}
	if pub, auth := e.memberRows(t, ctx, org, third); pub != "" || auth != "" {
		t.Fatalf("refused actors wrote rows: %q %q", pub, auth)
	}
	// An admin may not change a superuser member; a superuser may.
	superRole := func(actor uuid.UUID) answer {
		return e.doWith(t, "/internal/v1/memberships/role", jsonBody(t, map[string]any{"organization_id": org.String(),
			"principal_id": super.String(), "actor_principal_id": actor.String(), "role": "admin"}), header)
	}
	if got := superRole(member); got.status != http.StatusForbidden || object(t, got)["reason_code"] != "superuser_required" {
		t.Fatalf("admin changes a superuser member: %d %s, want 403 superuser_required", got.status, got.body)
	}
	if pub, _ := e.memberRows(t, ctx, org, super); pub != "member" {
		t.Fatalf("a refused change moved the superuser member to %q", pub)
	}
	if got := superRole(super); got.status != http.StatusOK {
		t.Fatalf("superuser changes a superuser member: %d %s", got.status, got.body)
	}

	remove := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": member.String(), "actor_principal_id": owner.String()})
	if got := e.doWith(t, "/internal/v1/memberships/remove", remove, header); got.status != http.StatusOK {
		t.Fatalf("remove: %d %s", got.status, got.body)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "" || auth != "removed" {
		t.Fatalf("after remove: public %q auth %q", pub, auth)
	}
	lastOwner := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": owner.String(), "actor_principal_id": owner.String()})
	if got := e.doWith(t, "/internal/v1/memberships/remove", lastOwner, header); got.status != http.StatusConflict || object(t, got)["reason_code"] != "last_owner" {
		t.Fatalf("remove of the last owner: %d %s", got.status, got.body)
	}
	transfer := jsonBody(t, map[string]any{"organization_id": org.String(), "from_principal_id": owner.String(), "to_principal_id": viewer.String(),
		"actor_principal_id": owner.String()})
	if got := e.doWith(t, "/internal/v1/memberships/transfer", transfer, header); got.status != http.StatusOK {
		t.Fatalf("transfer: %d %s", got.status, got.body)
	}
	if pub, _ := e.memberRows(t, ctx, org, viewer); pub != "owner" {
		t.Fatalf("after transfer the target is %q, want owner", pub)
	}
}

func (e *routesEnv) addVia(t *testing.T, header map[string]string, org, user, actor uuid.UUID, role string) {
	t.Helper()
	body := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": user.String(), "actor_principal_id": actor.String(), "role": role})
	if got := e.doWith(t, "/internal/v1/memberships/add", body, header); got.status != http.StatusOK {
		t.Fatalf("add %s: %d %s", role, got.status, got.body)
	}
}

// An admin's add races the admin's own demotion: each add is either served
// (before the demotion) or refused as admin_required (after it), never a 5xx.
func TestProvisioningActorRuleUnderAConcurrentDemotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	outcomes := map[int]int{}
	for round := range 20 {
		admin := e.account(t, ctx, acctMail(), false, true)
		e.addVia(t, header, org, admin, owner, "admin")
		target := e.account(t, ctx, acctMail(), false, true)
		var wg sync.WaitGroup
		var added, demoted answer
		wg.Add(2)
		go func() {
			defer wg.Done()
			added = e.doWith(t, "/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org.String(),
				"principal_id": target.String(), "actor_principal_id": admin.String(), "role": "member"}), header)
		}()
		go func() {
			defer wg.Done()
			demoted = e.doWith(t, "/internal/v1/memberships/role", jsonBody(t, map[string]any{"organization_id": org.String(),
				"principal_id": admin.String(), "actor_principal_id": owner.String(), "role": "viewer"}), header)
		}()
		wg.Wait()
		if demoted.status != http.StatusOK {
			t.Fatalf("round %d: demotion %d %s", round, demoted.status, demoted.body)
		}
		switch added.status {
		case http.StatusOK:
		case http.StatusForbidden:
			if object(t, added)["reason_code"] != "admin_required" {
				t.Fatalf("round %d: add refused %s", round, added.body)
			}
			if pub, auth := e.memberRows(t, ctx, org, target); pub != "" || auth != "" {
				t.Fatalf("round %d: a refused add wrote rows", round)
			}
		default:
			t.Fatalf("round %d: add %d %s", round, added.status, added.body)
		}
		outcomes[added.status]++
	}
	t.Logf("add outcomes: %v", outcomes)
}

// Invite create, accept and replay through the routes; the token is answered
// once and is in no log line.
func TestProvisioningInviteRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	address := acctMail()
	created := e.doWith(t, "/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": address,
		"role": "member", "actor_principal_id": owner.String(), "description": "Organization invite created",
		"request_metadata": map[string]any{"ip_address": "198.51.100.7"}}), header)
	if created.status != http.StatusOK {
		t.Fatalf("create: %d %s", created.status, created.body)
	}
	token, _ := object(t, created)["token"].(string)
	if token == "" {
		t.Fatal("no token answered")
	}
	var metadata, description string
	if err := e.admin.QueryRow(ctx, `SELECT request_metadata::text, description FROM public.audit_logs WHERE org_id = $1 AND action = 'member_invited'`, org).
		Scan(&metadata, &description); err != nil || !strings.Contains(metadata, "198.51.100.7") || description != "Organization invite created" {
		t.Fatalf("go-api audit_logs row: %q %q (%v)", metadata, description, err)
	}
	invitee := e.account(t, ctx, address, false, true)
	accept := jsonBody(t, map[string]any{"token": token, "principal_id": invitee.String()})
	got := e.doWith(t, "/internal/v1/invites/accept", accept, header)
	if got.status != http.StatusOK || object(t, got)["role"] != "member" {
		t.Fatalf("accept: %d %s", got.status, got.body)
	}
	if pub, auth := e.memberRows(t, ctx, org, invitee); pub != "member" || auth != "active" {
		t.Fatalf("after accept: public %q auth %q", pub, auth)
	}
	replay := e.doWith(t, "/internal/v1/invites/accept", accept, header)
	unknown := e.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": strings.Repeat("0", 32) + "." + strings.Repeat("0", 64),
		"principal_id": invitee.String()}), header)
	for name, refused := range map[string]answer{"replay": replay, "unknown": unknown} {
		if refused.status != http.StatusBadRequest || object(t, refused)["reason_code"] != "invite_invalid" {
			t.Fatalf("%s: %d %s, want 400 invite_invalid", name, refused.status, refused.body)
		}
	}
	if logs := e.logs.String(); strings.Contains(logs, token) || strings.Contains(logs, address) || strings.Contains(logs, "198.51.100.7") {
		t.Fatal("a log line carries the token, the address or the request metadata")
	}
}

// A deprovision is a superuser's: it ends every membership and invite and
// suspends the organization in one commit; after it, an add, an invite create
// and an accept of a token minted before are refused.
func TestProvisioningDeprovisionThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	super := e.account(t, ctx, acctMail(), true, true)
	member := e.account(t, ctx, acctMail(), false, true)
	e.addVia(t, header, org, member, owner, "member")
	address := acctMail()
	created := e.doWith(t, "/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": address,
		"role": "member", "actor_principal_id": owner.String()}), header)
	token, _ := object(t, created)["token"].(string)

	deprovision := func(actor uuid.UUID) answer {
		return e.doWith(t, "/internal/v1/organizations/deprovision", jsonBody(t, map[string]any{"organization_id": org.String(),
			"actor_principal_id": actor.String()}), header)
	}
	if got := deprovision(owner); got.status != http.StatusForbidden || object(t, got)["reason_code"] != "superuser_required" {
		t.Fatalf("deprovision by the owner: %d %s", got.status, got.body)
	}
	got := deprovision(super)
	if got.status != http.StatusOK {
		t.Fatalf("deprovision: %d %s", got.status, got.body)
	}
	if out := object(t, got); out["removed_memberships"] != float64(2) || out["revoked_invites"] != float64(1) || out["suspended"] != true {
		t.Fatalf("deprovision answer = %v", out)
	}
	var live int
	if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM public.memberships WHERE org_id = $1) +
  (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed') +
  (SELECT count(*) FROM auth.invites WHERE organization_id = $1 AND status = 'pending') +
  (SELECT count(*) FROM public.org_invites WHERE org_id = $1 AND status = 'pending') +
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL)`, org).Scan(&live); err != nil || live != 0 {
		t.Fatalf("%d live rows after the deprovision (%v), want 0", live, err)
	}
	late := e.account(t, ctx, acctMail(), false, true)
	add := e.doWith(t, "/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": late.String(),
		"actor_principal_id": super.String(), "role": "member"}), header)
	invite := e.doWith(t, "/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": acctMail(),
		"role": "member", "actor_principal_id": super.String()}), header)
	for name, refused := range map[string]answer{"add": add, "invite": invite} {
		if refused.status != http.StatusConflict || object(t, refused)["reason_code"] != "organization_suspended" {
			t.Fatalf("%s after deprovision: %d %s", name, refused.status, refused.body)
		}
	}
	invitee := e.account(t, ctx, address, false, true)
	accepted := e.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": token, "principal_id": invitee.String()}), header)
	if accepted.status != http.StatusBadRequest || object(t, accepted)["reason_code"] != "invite_invalid" {
		t.Fatalf("accept after deprovision: %d %s", accepted.status, accepted.body)
	}
	if again := deprovision(super); again.status != http.StatusOK || object(t, again)["suspended"] != false {
		t.Fatalf("repeat deprovision: %d %s", again.status, again.body)
	}
}

// With the switch off the routes are not served.
func TestProvisioningRoutesOffByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnvWith(t, ctx, map[string]string{authconfig.EnvUsersOwned: "true"})
	token, _ := e.issuerCredential(t, ctx, "auth-session-issuer", `["memberships:write", "sessions:issue"]`, false, false)
	for _, path := range []string{"/internal/v1/memberships/add", "/internal/v1/memberships/role", "/internal/v1/memberships/remove",
		"/internal/v1/memberships/transfer", "/internal/v1/invites/create", "/internal/v1/invites/accept", "/internal/v1/organizations/deprovision"} {
		if got := e.doWith(t, path, `{}`, map[string]string{"Authorization": "Bearer " + token}); got.status != http.StatusNotFound {
			t.Fatalf("%s with the switch off: %d, want 404", path, got.status)
		}
	}
}

// After a deprovision, a sign-in that names the organization (a former
// member's login with a wrong password, an unknown address, a session issue)
// is refused and leaves the organization suspended, with no membership row.
// Each sign-in path refreshes the organization from go-api's row, which stays
// active until go-api's purge.
func TestDeprovisionedOrganizationStaysSuspendedOnSignIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	owner, org := e.seed(t, ctx)
	super := e.account(t, ctx, acctMail(), true, true)
	address := acctMail()
	member := e.account(t, ctx, address, false, true)
	e.addVia(t, header, org, member, owner, "member")
	got := e.doWith(t, "/internal/v1/organizations/deprovision", jsonBody(t, map[string]any{"organization_id": org.String(),
		"actor_principal_id": super.String()}), header)
	if got.status != http.StatusOK {
		t.Fatalf("deprovision: %d %s", got.status, got.body)
	}
	suspended := func(step string) {
		t.Helper()
		var n int
		if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL) +
  (SELECT count(*) FROM public.memberships WHERE org_id = $1) +
  (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed')`, org).Scan(&n); err != nil || n != 0 {
			t.Fatalf("after %s: %d live organization or membership rows (%v), want 0", step, n, err)
		}
	}
	suspended("the deprovision")
	client := map[string]string{"Content-Type": "application/json"}
	for name, body := range map[string]string{
		"former member, wrong password": `{"email":"` + address + `","password":"not-the-password","org_id":"` + org.String() + `"}`,
		"unknown address":               `{"email":"` + acctMail() + `","password":"not-the-password","org_id":"` + org.String() + `"}`,
	} {
		if got := e.doWith(t, "/api/v1/auth/login", body, client); got.status < 400 {
			t.Fatalf("login, %s: %d %s, want a refusal", name, got.status, got.body)
		}
		suspended("a login, " + name)
	}
	issuer, _ := e.issuerCredential(t, ctx, "auth-session-issuer", `["sessions:issue"]`, false, false)
	issue := `{"principal_id":"` + member.String() + `","organization_id":"` + org.String() + `","amr":["pwd"]}`
	if got := e.doWith(t, "/internal/v1/sessions", issue, map[string]string{"Authorization": "Bearer " + issuer}); got.status != http.StatusForbidden {
		t.Fatalf("session issue for a former member: %d %s, want 403", got.status, got.body)
	}
	suspended("a session issue")
}
