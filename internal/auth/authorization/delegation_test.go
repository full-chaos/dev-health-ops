package authorization

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	testActor     = "prn_actor"
	ownerActorRol = "platform_owner"
)

// floorOrgActions is the approved deny list of the org catalog under
// impersonation, written out independently of the code under test.
var floorOrgActions = map[string]bool{
	"members.invite": true, "members.manage": true, "billing.manage": true, "org.delete": true,
}

var credentialShape = regexp.MustCompile(`^(credentials?|tokens?)\.(create|issue|rotate|revoke)$`)

type delegatedSnapshot struct {
	fakeSnapshot
	d  Delegation
	on bool
}

func (f delegatedSnapshot) Delegation() (Delegation, bool) { return f.d, f.on }

func delegated(role string, permitted []string, actorRole string) delegatedSnapshot {
	return delegatedSnapshot{
		fakeSnapshot: orgSnapshot(role),
		d: Delegation{
			Chain:                []Hop{{ActorPrincipalID: testActor, PermittedActions: permitted}},
			ActorPlatformRole:    actorRole,
			TargetOrganizationID: testOrg,
			Assurance:            "aal1",
		},
		on: true,
	}
}

func (f delegatedSnapshot) withChain(c []Hop) delegatedSnapshot {
	f.d.Chain = c
	return f
}

func allOrgActions(t *testing.T) []string {
	t.Helper()
	actions, _, _ := vocab(t)
	var out []string
	for _, a := range actions {
		if a.Scope == "organization" {
			out = append(out, a.Action)
		}
	}
	return out
}

func TestDelegation_FloorDenyListUnderImpersonation(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	for id := range floorOrgActions {
		got := e.Evaluate(orgRequest(id), delegated("owner", all, ""))
		if got.Allowed || got.Reason != ReasonImpersonationDenied {
			t.Errorf("%s: %+v want impersonation_denied", id, got)
		}
		if got.Obligations != nil || got.Audit != nil {
			t.Errorf("%s: a deny carries obligations or audit: %+v", id, got)
		}
	}
}

func TestDelegation_FloorSetAndShapes(t *testing.T) {
	for _, id := range []string{
		"members.invite", "members.manage", "billing.manage", "org.delete", "admin.impersonate",
		"credentials.issue", "credentials.rotate", "credentials.revoke", "credential.create",
		"tokens.create", "tokens.rotate", "token.revoke", "token.issue",
	} {
		if !impersonationFloor(id) {
			t.Errorf("%s is not on the floor deny set", id)
		}
	}
	for _, id := range []string{"org.read", "billing.read", "members.read", "credentials.read", "tokens.list", "analytics.export"} {
		if impersonationFloor(id) {
			t.Errorf("%s is on the floor deny set", id)
		}
	}
}

func TestDelegation_PlantedCredentialActionIsDeniedUnderImpersonation(t *testing.T) {
	var af, rf map[string]any
	if err := json.Unmarshal(vocabulary.ActionsV1, &af); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(vocabulary.RolesV1, &rf); err != nil {
		t.Fatal(err)
	}
	ids := []string{"credentials.issue", "credentials.rotate", "credentials.revoke", "tokens.create", "admin.impersonate"}
	for _, id := range ids {
		af["actions"] = append(af["actions"].([]any), map[string]any{
			"action": id, "scope": "organization", "resource_kind": "organization", "risk_class": "high", "cache_class": "none",
		})
		for _, r := range rf["roles"].([]any) {
			if m := r.(map[string]any); m["role"] == "owner" {
				m["actions"] = append(m["actions"].([]any), id)
			}
		}
	}
	ab, _ := json.Marshal(af)
	rb, _ := json.Marshal(rf)
	cat, err := policycatalog.Load(ab, rb)
	if err != nil {
		t.Fatal(err)
	}
	e := newEvaluator(cat)
	if got := e.Evaluate(orgRequest("credentials.rotate"), orgSnapshot("owner")); !got.Allowed {
		t.Fatalf("planted action not allowed without delegation: %+v", got)
	}
	for _, id := range ids {
		got := e.Evaluate(orgRequest(id), delegated("owner", ids, ""))
		if got.Allowed || got.Reason != ReasonImpersonationDenied {
			t.Errorf("%s: %+v want impersonation_denied", id, got)
		}
	}
	got := e.Evaluate(orgRequest("admin.impersonate"), delegated("owner", ids, ownerActorRol))
	if got.Allowed || got.Reason != ReasonImpersonationDenied {
		t.Errorf("a nested impersonation is denied for platform_owner too: %+v", got)
	}
	if got := e.Evaluate(orgRequest("credentials.rotate"), delegated("owner", ids, ownerActorRol)); !got.Allowed || got.Audit == nil {
		t.Errorf("platform_owner exemption covers a planted credential action: %+v", got)
	}
}

func TestDelegation_NestedChainDeniedForEveryone(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	for _, actorRole := range []string{"", "platform_admin", ownerActorRol} {
		s := delegated("owner", all, actorRole)
		s.d.Chain = append(s.d.Chain, Hop{ActorPrincipalID: "prn_second", PermittedActions: all})
		for _, id := range []string{"org.read", "billing.manage"} {
			got := e.Evaluate(orgRequest(id), s)
			if got.Allowed || got.Reason != ReasonImpersonationDenied {
				t.Errorf("actor %q %s: %+v want impersonation_denied", actorRole, id, got)
			}
		}
	}
}

func TestDelegation_PlatformOwnerExemptionIsAllowedAndAudited(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	for id := range floorOrgActions {
		got := e.Evaluate(orgRequest(id), delegated("owner", all, ownerActorRol))
		if !got.Allowed || got.Reason != ReasonAllowedByRole {
			t.Fatalf("%s: %+v want allow", id, got)
		}
		want := DelegationAudit{ActorPrincipalID: testActor, OrganizationID: testOrg, Action: id, Reason: AuditReasonOwnerExempt}
		if got.Audit == nil || *got.Audit != want {
			t.Errorf("%s: audit %+v want %+v", id, got.Audit, want)
		}
	}
	got := e.Evaluate(orgRequest("org.read"), delegated("owner", all, ownerActorRol))
	if !got.Allowed || got.Audit != nil {
		t.Errorf("a non-floor action needs no exemption: %+v", got)
	}
}

func TestDelegation_OwnerExemptionNeverSkipsTheEffectiveRole(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	for id := range floorOrgActions {
		got := e.Evaluate(orgRequest(id), delegated("viewer", all, ownerActorRol))
		if got.Allowed || got.Reason != ReasonRoleLacksAction {
			t.Errorf("%s: %+v want role_lacks_action", id, got)
		}
	}
}

func TestDelegation_RealActorRoleNeverWidensTheEffectiveSubject(t *testing.T) {
	e := NewEvaluator()
	actions, roles, _ := vocab(t)
	all := allOrgActions(t)
	held := map[string]map[string]bool{}
	for _, r := range roles {
		held[r.Role] = map[string]bool{}
		for _, a := range r.Actions {
			held[r.Role][a] = true
		}
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		for _, actorRole := range []string{"", "platform_admin", "superuser", "root", "owner", ownerActorRol} {
			for _, a := range actions {
				if a.Scope != "organization" {
					continue
				}
				want := held[role][a.Action] && (!floorOrgActions[a.Action] || actorRole == ownerActorRol)
				got := e.Evaluate(orgRequest(a.Action), delegated(role, all, actorRole))
				if got.Allowed != want {
					t.Errorf("role %s actor %q %s: allowed=%v want %v (%s)", role, actorRole, a.Action, got.Allowed, want, got.Reason)
				}
			}
		}
	}
}

func TestDelegation_EmptyPermittedActionsDeniesEverything(t *testing.T) {
	e := NewEvaluator()
	for _, permitted := range [][]string{nil, {}} {
		for _, actorRole := range []string{"", ownerActorRol} {
			for _, id := range allOrgActions(t) {
				got := e.Evaluate(orgRequest(id), delegated("owner", permitted, actorRole))
				if got.Allowed || got.Reason != ReasonImpersonationDenied {
					t.Errorf("permitted %#v actor %q %s: %+v want impersonation_denied", permitted, actorRole, id, got)
				}
			}
		}
	}
}

func TestDelegation_ActionOutsidePermittedActionsDenies(t *testing.T) {
	e := NewEvaluator()
	got := e.Evaluate(orgRequest("org.read"), delegated("owner", []string{"analytics.read"}, ""))
	if got.Allowed || got.Reason != ReasonImpersonationDenied {
		t.Fatalf("%+v", got)
	}
	got = e.Evaluate(orgRequest("analytics.read"), delegated("owner", []string{"analytics.read"}, ""))
	if !got.Allowed {
		t.Fatalf("%+v", got)
	}
}

func TestDelegation_PlatformActionsAreDeniedUnderImpersonation(t *testing.T) {
	e := NewEvaluator()
	s := delegated("owner", []string{"admin.users"}, ownerActorRol)
	s.fakeSnapshot.platformRole = "platform_admin"
	got := e.Evaluate(platformRequest("admin.users"), s)
	if got.Allowed || got.Reason != ReasonImpersonationDenied {
		t.Fatalf("%+v", got)
	}
}

func TestDelegation_MalformedDelegationDenies(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	base := func(mut func(*delegatedSnapshot)) Snapshot {
		s := delegated("owner", all, "")
		mut(&s)
		return s
	}
	cases := map[string]Snapshot{
		"empty chain":       base(func(s *delegatedSnapshot) { s.d.Chain = nil }),
		"empty actor":       base(func(s *delegatedSnapshot) { s.d.Chain[0].ActorPrincipalID = "" }),
		"empty target org":  base(func(s *delegatedSnapshot) { s.d.TargetOrganizationID = "" }),
		"other target org":  base(func(s *delegatedSnapshot) { s.d.TargetOrganizationID = "org-2" }),
		"unknown assurance": base(func(s *delegatedSnapshot) { s.d.Assurance = "aal9" }),
		"empty assurance":   base(func(s *delegatedSnapshot) { s.d.Assurance = "" }),
		"request org differs": base(func(s *delegatedSnapshot) {
			s.orgStatus["org-2"] = StatusActive
			s.memberships["org-2"] = Membership{OrgID: "org-2", Role: "owner", Status: StatusActive}
		}),
	}
	for name, s := range cases {
		req := orgRequest("org.read")
		if name == "request org differs" {
			req.OrganizationID = "org-2"
		}
		got := e.Evaluate(req, s)
		if got.Allowed || got.Reason != ReasonDelegationInvalid {
			t.Errorf("%s: %+v want delegation_invalid", name, got)
		}
		if got.PolicyRevision == 0 {
			t.Errorf("%s: no policy revision on the deny", name)
		}
	}
}

func TestDelegation_AllowCarriesObligationsPerSchema(t *testing.T) {
	e := NewEvaluator()
	got := e.Evaluate(orgRequest("org.read"), delegated("viewer", allOrgActions(t), ""))
	if !got.Allowed || got.Obligations == nil {
		t.Fatalf("%+v", got)
	}
	if got.Obligations.TenantScope != testOrg || got.Obligations.MinimumAssurance != "aal1" {
		t.Fatalf("%+v", got.Obligations)
	}
	raw, err := json.Marshal(got.Obligations)
	if err != nil {
		t.Fatal(err)
	}
	var obl map[string]any
	if err := json.Unmarshal(raw, &obl); err != nil {
		t.Fatal(err)
	}
	if _, ok := obl["redaction"]; ok {
		t.Errorf("redaction must be absent: %s", raw)
	}
	if _, ok := obl["step_up"]; ok {
		t.Errorf("step_up must be absent: %s", raw)
	}
	doc := map[string]any{
		"schema_version": "authorization_decision.v1", "decision_id": "dec_EXAMPLE0000000000000000001",
		"allowed": true, "reason_code": "allowed_by_repository_grant", "principal_id": "prn_EXAMPLE0000000000000001",
		"organization_id": "org_EXAMPLE0000000000000001", "policy_revision": 4,
		"expires_at": "2026-09-03T05:00:30Z", "obligations": obl,
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(root, contracts.AuthorizationDecisionSurface, doc); err != nil {
		t.Fatalf("obligations are not per schema: %v\n%s", err, raw)
	}
}

func TestDelegation_DenyCarriesNoObligationsOrAudit(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	for _, s := range []delegatedSnapshot{
		delegated("viewer", all, ownerActorRol), delegated("owner", nil, ""), delegated("owner", all, ""),
	} {
		for _, id := range all {
			got := e.Evaluate(orgRequest(id), s)
			if !got.Allowed && (got.Obligations != nil || got.Audit != nil) {
				t.Errorf("%s: deny carries %+v %+v", id, got.Obligations, got.Audit)
			}
		}
	}
}

func TestDelegation_NonDelegatedDecisionsAreUnchanged(t *testing.T) {
	e := NewEvaluator()
	actions, roles, ver := vocab(t)
	held := map[string]map[string]bool{}
	for _, r := range roles {
		held[r.Role] = map[string]bool{}
		for _, a := range r.Actions {
			held[r.Role][a] = true
		}
	}
	for _, r := range roles {
		if r.Scope != "organization" {
			continue
		}
		for _, a := range actions {
			if a.Scope != "organization" {
				continue
			}
			plain := e.Evaluate(orgRequest(a.Action), orgSnapshot(r.Role))
			aware := e.Evaluate(orgRequest(a.Action), delegatedSnapshot{fakeSnapshot: orgSnapshot(r.Role)})
			want := Decision{Allowed: held[r.Role][a.Action], Reason: ReasonRoleLacksAction, PolicyRevision: ver}
			if want.Allowed {
				want.Reason = ReasonAllowedByRole
			}
			if plain != want || aware != want {
				t.Errorf("%s %s: plain %+v aware %+v want %+v", r.Role, a.Action, plain, aware, want)
			}
		}
	}
}
