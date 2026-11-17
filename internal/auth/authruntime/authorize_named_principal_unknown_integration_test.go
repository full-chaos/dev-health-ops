//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// A named principal reference whose principal does not exist (never created, or erased by the admin force delete)
// is decided, on the single and the batch route, for a high-risk action: a deny, never a 5xx, the whole batch, and
// the decision's audit row as the audit code writes it, with no foreign key error.
func TestNamedPrincipalUnknownOrErasedIsDeniedForAHighRiskAction(t *testing.T) {
	ctx := context.Background()
	caller := session.WirePrincipalID(uuid.New())
	a := newAuthorizeRouterEnv(t, ctx, map[string]string{
		authconfig.EnvAuthorizeCallerSubjects:       caller,
		authconfig.EnvAuthorizeNamedPrincipalCaller: caller,
	})
	a.caller = caller
	org := a.azOrg(t, ctx, true, true)
	member := a.azUser(t, ctx, false)
	a.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now())`,
		member, org)

	erased := a.azUser(t, ctx, false)
	a.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now())`,
		erased, org)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	// The admin force delete as the table owner, as the erase decision cell runs it: this harness's runtime role
	// holds no users-owned grants.
	store, err := identity.New(identity.Config{Pool: a.admin, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	actor := a.azUser(t, ctx, true)
	none := func(context.Context, audit.TxOps) error { return nil }
	if _, err := store.AdminForceDeleteUser(ctx, identity.AdminForceDeleteRequest{Principal: erased, Actor: actor, Authorize: none, RevokeRoles: none}); err != nil {
		t.Fatalf("erase: %v", err)
	}
	var left int
	if err := a.admin.QueryRow(ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, erased).Scan(&left); err != nil || left != 0 {
		t.Fatalf("the erased principal row is still there (count %d, err %v)", left, err)
	}

	body := func(principal uuid.UUID) []byte {
		raw, err := json.Marshal(azRequestDoc{
			SchemaVersion: "authorization_request.v1", PrincipalReference: session.WirePrincipalID(principal), Action: "analytics.export",
			Resource: azResourceDoc{Type: "organization", Key: "organization", OrganizationID: org.String()},
			Context:  azContextDoc{RequestID: azRequestID, ClientService: "dev-health-query-api"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// rowOf is the high-risk audit row of one decision: its actor column and whether the attributes name the
	// principal the reference carried.
	rowOf := func(t *testing.T, decisionID string, principal uuid.UUID) (actor *uuid.UUID, outcome string, namesPrincipal bool) {
		t.Helper()
		var attributes string
		if err := a.admin.QueryRow(ctx, `SELECT actor_principal_id, outcome, attributes::text FROM auth.security_audit_events
WHERE event_type = $1 AND attributes->>'decision_id' = $2`, audit.EventAuthorizeHighRiskDecided, decisionID).Scan(&actor, &outcome, &attributes); err != nil {
			t.Fatalf("the audit row of decision %s: %v", decisionID, err)
		}
		return actor, outcome, strings.Contains(attributes, principal.String()) || strings.Contains(attributes, session.WirePrincipalID(principal))
	}
	type verdict struct {
		DecisionID string `json:"decision_id"`
		Allowed    bool   `json:"allowed"`
		ReasonCode string `json:"reason_code"`
	}
	denied := func(t *testing.T, d verdict, principal uuid.UUID) {
		t.Helper()
		if d.Allowed || d.ReasonCode != "principal_inactive" {
			t.Fatalf("allowed=%v reason %q, want a principal_inactive deny", d.Allowed, d.ReasonCode)
		}
		actor, outcome, names := rowOf(t, d.DecisionID, principal)
		if actor != nil || outcome != string(audit.OutcomeDenied) || names {
			t.Fatalf("audit row: actor %v outcome %q names the principal %v, want actor NULL, denied, no id", actor, outcome, names)
		}
	}
	single := func(t *testing.T, principal uuid.UUID) verdict {
		t.Helper()
		got := a.authorize(t, body(principal))
		if got.status != http.StatusOK {
			t.Fatalf("single route: status %d, want 200 with a decision; body %s", got.status, got.body)
		}
		var d verdict
		if err := json.Unmarshal(got.body, &d); err != nil {
			t.Fatalf("single route: %v; body %s", err, got.body)
		}
		return d
	}
	highRiskRows := func(t *testing.T) int {
		t.Helper()
		var n int
		if err := a.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, audit.EventAuthorizeHighRiskDecided).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The control: the same high-risk question for a live owner is decided and audited.
	before := highRiskRows(t)
	control := single(t, member)
	if !control.Allowed {
		t.Fatalf("control: the live owner is refused (%s): the cell would measure nothing", control.ReasonCode)
	}
	if n := highRiskRows(t) - before; n != 1 {
		t.Fatalf("control: %d high-risk audit rows, want 1", n)
	}
	if actor, outcome, _ := rowOf(t, control.DecisionID, member); actor == nil || *actor != member || outcome != string(audit.OutcomeAllowed) {
		t.Fatalf("control: audit row actor %v outcome %q, want the owner and allowed", actor, outcome)
	}

	for _, c := range []struct {
		name      string
		principal uuid.UUID
	}{{"never existed", uuid.New()}, {"erased by the admin force delete", erased}} {
		t.Run(c.name+"/single", func(t *testing.T) {
			before := highRiskRows(t)
			d := single(t, c.principal)
			denied(t, d, c.principal)
			if n := highRiskRows(t) - before; n != 1 {
				t.Fatalf("%d high-risk audit rows, want 1", n)
			}
		})
		t.Run(c.name+"/batch", func(t *testing.T) {
			request, err := azBatchBody([][]byte{body(member), body(c.principal)})
			if err != nil {
				t.Fatal(err)
			}
			before := highRiskRows(t)
			got := a.authorizeBatch(t, request)
			if got.status != http.StatusOK {
				t.Fatalf("batch route: status %d, want 200 with both decisions; body %s", got.status, got.body)
			}
			var doc struct {
				Decisions []struct {
					Index    int     `json:"index"`
					Decision verdict `json:"decision"`
				} `json:"decisions"`
			}
			if err := json.Unmarshal(got.body, &doc); err != nil || len(doc.Decisions) != 2 {
				t.Fatalf("batch route: %d decisions (err %v), want 2; body %s", len(doc.Decisions), err, got.body)
			}
			if !doc.Decisions[0].Decision.Allowed {
				t.Fatalf("batch route: the live owner's item is refused (%s)", doc.Decisions[0].Decision.ReasonCode)
			}
			denied(t, doc.Decisions[1].Decision, c.principal)
			if n := highRiskRows(t) - before; n != 2 {
				t.Fatalf("batch route: %d high-risk audit rows, want 2", n)
			}
		})
	}
	logs := a.logs.String()
	for _, needle := range []string{"23503", "foreign key", "violates"} {
		if n := strings.Count(logs, needle); n != 0 {
			t.Errorf("the auth-service log holds %d %q lines", n, needle)
		}
	}
}
