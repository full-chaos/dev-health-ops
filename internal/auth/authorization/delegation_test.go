package authorization

import (
	"encoding/json"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
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

// deniedOrgActions is floorOrgActions plus the actions 4.16a flags denied (ACP ruling: floor or flag).
var deniedOrgActions = map[string]bool{
	"members.invite": true, "members.manage": true, "billing.manage": true, "org.delete": true,
	"settings.write": true, "integrations.write": true, "org.write": true, "members.update": true,
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
		if a.Scope == "organization" && a.ResourceKind == "organization" {
			out = append(out, a.Action)
		}
	}
	return out
}

func heldBy(t *testing.T, role string) []string {
	t.Helper()
	_, roles, _ := vocab(t)
	var out []string
	for _, r := range roles {
		if r.Role != role {
			continue
		}
		for _, id := range allOrgActions(t) {
			for _, a := range r.Actions {
				if a == id {
					out = append(out, id)
				}
			}
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
	ids := []string{"credentials.issue", "credentials.rotate", "credentials.revoke", "tokens.create"}
	for _, id := range ids {
		af["actions"] = append(af["actions"].([]any), map[string]any{
			"action": id, "scope": "organization", "resource_kind": "organization", "risk_class": "high", "cache_class": "none", "impersonation": "allowed",
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
	if got := e.Evaluate(orgRequest("credentials.rotate"), delegated("owner", ids, ownerActorRol)); !got.Allowed || got.Audit == nil {
		t.Errorf("platform_owner exemption covers a planted credential action: %+v", got)
	}
}

func TestDelegation_PlatformImpersonateIsDeniedForEveryone(t *testing.T) {
	e := NewEvaluator()
	for _, actorRole := range []string{"", ownerActorRol} {
		s := delegated("owner", []string{"admin.impersonate"}, actorRole)
		s.fakeSnapshot.platformRole = "platform_admin"
		got := e.Evaluate(platformRequest("admin.impersonate"), s)
		if got.Allowed || got.Reason != ReasonImpersonationDenied {
			t.Errorf("actor %q: %+v want impersonation_denied", actorRole, got)
		}
	}
	if got := e.Evaluate(platformRequest("admin.impersonate"), platformSnapshot("platform_admin")); !got.Allowed {
		t.Errorf("not allowed without delegation: %+v", got)
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
	// all is a planted full action set: the real Start path stores it for a platform_owner actor with 10987 PR-C.
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
				if a.Scope != "organization" || a.ResourceKind != "organization" {
					continue
				}
				want := held[role][a.Action] && (!deniedOrgActions[a.Action] || actorRole == ownerActorRol)
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
			for _, id := range heldBy(t, "owner") {
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
			if a.Scope != "organization" || a.ResourceKind != "organization" {
				continue
			}
			plain := e.Evaluate(orgRequest(a.Action), orgSnapshot(r.Role))
			aware := e.Evaluate(orgRequest(a.Action), delegatedSnapshot{fakeSnapshot: orgSnapshot(r.Role)})
			want := Decision{Allowed: held[r.Role][a.Action], Reason: ReasonRoleLacksAction, PolicyRevision: ver}
			if want.Allowed {
				want.Reason = ReasonAllowedByRole
			} else {
				want.CacheClass = CacheNone
			}
			if plain != want || aware != want {
				t.Errorf("%s %s: plain %+v aware %+v want %+v", r.Role, a.Action, plain, aware, want)
			}
		}
	}
}

type delegatedGrantSnapshot struct {
	grantFake
	d Delegation
}

func (f delegatedGrantSnapshot) Delegation() (Delegation, bool) { return f.d, true }

func delegatedRepo(permitted []string, grants ...Grant) delegatedGrantSnapshot {
	return delegatedGrantSnapshot{
		grantFake: repoSnapshot("member", grants...),
		d: Delegation{
			Chain:                []Hop{{ActorPrincipalID: testActor, PermittedActions: permitted}},
			TargetOrganizationID: testOrg,
			Assurance:            "aal2",
		},
	}
}

func TestDelegation_RepositoryAllowCarriesTheAllowlistAndKeepsTheCacheBound(t *testing.T) {
	e := plantedEvaluator()
	plain := e.Evaluate(repoRequest("acme/api"), repoSnapshot("member", liveGrant("acme/api")))
	got := e.Evaluate(repoRequest("acme/api"), delegatedRepo([]string{repoAction}, liveGrant("acme/api")))
	if !got.Allowed || got.Reason != ReasonAllowedByRepositoryGrant {
		t.Fatalf("%+v", got)
	}
	want := &Obligations{TenantScope: testOrg, MinimumAssurance: "aal2", RepositoryAllowlist: []string{"acme/api"}}
	if got.Obligations == nil || got.Obligations.TenantScope != want.TenantScope || got.Obligations.MinimumAssurance != "aal2" ||
		len(got.Obligations.RepositoryAllowlist) != 1 || got.Obligations.RepositoryAllowlist[0] != "acme/api" {
		t.Fatalf("obligations %+v want %+v", got.Obligations, want)
	}
	if got.CacheClass != plain.CacheClass || got.ExpiresAtUnixNano != plain.ExpiresAtUnixNano {
		t.Errorf("delegation changed the cache bound: %+v vs %+v", got, plain)
	}
}

func TestDelegation_RepositoryAllowOutsidePermittedActionsDenies(t *testing.T) {
	e := plantedEvaluator()
	for _, permitted := range [][]string{nil, {"org.read"}} {
		got := e.Evaluate(repoRequest("acme/api"), delegatedRepo(permitted, liveGrant("acme/api")))
		if got.Allowed || got.Reason != ReasonImpersonationDenied || got.Obligations != nil {
			t.Errorf("permitted %v: %+v want impersonation_denied", permitted, got)
		}
	}
}

func TestDelegation_RepositoryRequestOfAnotherOrganizationIsInvalid(t *testing.T) {
	e := plantedEvaluator()
	req := repoRequest("acme/api")
	req.OrganizationID = "org-2"
	got := e.Evaluate(req, delegatedRepo([]string{repoAction}, liveGrant("acme/api")))
	if got.Allowed || got.Reason != ReasonDelegationInvalid {
		t.Fatalf("%+v", got)
	}
}

func TestDelegation_RepositoryDenyKeepsTheGrantReason(t *testing.T) {
	e := plantedEvaluator()
	got := e.Evaluate(repoRequest("acme/api"), delegatedRepo([]string{repoAction}, liveGrant("acme/web")))
	if got.Allowed || got.Reason != ReasonRepositoryNotGranted || got.Obligations != nil {
		t.Fatalf("%+v", got)
	}
}

// Catalog flag (4.16a): an action not flagged allowed is denied under impersonation, off the floor too.
func TestDelegation_FlaggedActionIsDeniedUnlessThePlatformOwnerActs(t *testing.T) {
	cat := policycatalog.Default()
	e := NewEvaluator()
	all := allOrgActions(t)
	n := 0
	for _, id := range heldBy(t, "owner") {
		a, _ := cat.Action(id)
		if a.Impersonation == policycatalog.ImpersonationAllowed || impersonationFloor(id) {
			continue
		}
		n++
		if got := e.Evaluate(orgRequest(id), delegated("owner", all, "")); got.Allowed || got.Reason != ReasonImpersonationDenied {
			t.Errorf("%s: %+v want impersonation_denied", id, got)
		}
		got := e.Evaluate(orgRequest(id), delegated("owner", all, ownerActorRol))
		if !got.Allowed || got.Audit == nil || got.Audit.Reason != AuditReasonOwnerExempt {
			t.Errorf("%s: platform_owner %+v want allow with owner_exempt", id, got)
		}
	}
	if n == 0 {
		t.Fatal("no flagged action off the floor: the cell proves nothing")
	}
}

func TestDelegation_ActionWithoutAFlagIsDeniedUnderImpersonation(t *testing.T) {
	e := newEvaluator(plantedCatalog{base: policycatalog.Default(), noFlag: true})
	got := e.Evaluate(repoRequest("acme/api"), delegatedRepo([]string{repoAction}, liveGrant("acme/api")))
	if got.Allowed || got.Reason != ReasonImpersonationDenied {
		t.Fatalf("%+v", got)
	}
	if got := newEvaluator(plantedCatalog{base: policycatalog.Default()}).Evaluate(repoRequest("acme/api"), delegatedRepo([]string{repoAction}, liveGrant("acme/api"))); !got.Allowed {
		t.Fatalf("flagged allowed action denied: %+v", got)
	}
}

// R14: every floor action that exists in the catalog is flagged denied there.
func TestDelegation_FloorIsASubsetOfTheCatalogFlag(t *testing.T) {
	cat := policycatalog.Default()
	seen := 0
	for _, a := range cat.Actions() {
		if impersonationFloor(a.ID) {
			seen++
			if a.Impersonation != policycatalog.ImpersonationDenied {
				t.Errorf("%s is on the floor but the catalog flag is %q", a.ID, a.Impersonation)
			}
		}
	}
	for id := range floorExact {
		if _, err := cat.Action(id); err != nil {
			t.Errorf("floor id %s is not in the catalog: %v", id, err)
		}
	}
	if seen < len(floorExact) {
		t.Errorf("only %d floor actions found in the catalog", seen)
	}
}

// R15: permitted_actions as a real session.Record carries them.
func TestDelegation_PermittedActionsOfARealSessionRecord(t *testing.T) {
	cat := policycatalog.Default()
	e := NewEvaluator()
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		permitted, err := cat.DelegatedActions(role)
		if err != nil {
			t.Fatal(err)
		}
		rec := session.Record{Delegation: &session.Delegation{Actor: uuid.New(), Reason: "r", PermittedActions: permitted}}
		for _, id := range allOrgActions(t) {
			s := delegatedSnapshot{fakeSnapshot: orgSnapshot(role), on: true, d: Delegation{
				Chain:                []Hop{{ActorPrincipalID: rec.Delegation.Actor.String(), PermittedActions: rec.Delegation.PermittedActions}},
				TargetOrganizationID: testOrg, Assurance: "aal1",
			}}
			a, _ := cat.Action(id)
			want := cat.Allows(role, id) && a.Impersonation == policycatalog.ImpersonationAllowed
			if got := e.Evaluate(orgRequest(id), s); got.Allowed != want {
				t.Errorf("role %s %s: allowed=%v want %v (%s)", role, id, got.Allowed, want, got.Reason)
			}
		}
		// A record whose set lacks an action the catalog would allow still denies it: the record is the cap.
		for _, dropped := range permitted {
			if a, _ := cat.Action(dropped); a.ResourceKind == "repository" {
				continue
			}
			rest := make([]string, 0, len(permitted))
			for _, id := range permitted {
				if id != dropped {
					rest = append(rest, id)
				}
			}
			rec := session.Record{Delegation: &session.Delegation{Actor: uuid.New(), Reason: "r", PermittedActions: rest}}
			s := delegatedSnapshot{fakeSnapshot: orgSnapshot(role), on: true, d: Delegation{
				Chain:                []Hop{{ActorPrincipalID: rec.Delegation.Actor.String(), PermittedActions: rec.Delegation.PermittedActions}},
				TargetOrganizationID: testOrg, Assurance: "aal1",
			}}
			if got := e.Evaluate(orgRequest(dropped), s); got.Allowed || got.Reason != ReasonImpersonationDenied {
				t.Errorf("role %s: %s outside the record's permitted set: %+v", role, dropped, got)
			}
		}
	}
}

// The repository read follows the record under the shipped catalog: the four organization roles delegate it, and a record without it denies.
func TestDelegation_RepositoryReadOfARealSessionRecord(t *testing.T) {
	cat := policycatalog.Default()
	e := NewEvaluator()
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		permitted, err := cat.DelegatedActions(role)
		if err != nil {
			t.Fatal(err)
		}
		var rest []string
		for _, id := range permitted {
			if id != repoAction {
				rest = append(rest, id)
			}
		}
		if len(rest) != len(permitted)-1 {
			t.Fatalf("role %s: %s is not in the delegated set %v", role, repoAction, permitted)
		}
		mk := func(p []string) delegatedGrantSnapshot {
			s := delegatedRepo(p)
			s.grantFake = repoSnapshot(role)
			return s
		}
		if got := e.Evaluate(repoRequest("acme/api"), mk(permitted)); !got.Allowed || got.Reason != ReasonAllowedByMembershipDefault {
			t.Errorf("role %s: %+v", role, got)
		}
		if got := e.Evaluate(repoRequest("acme/api"), mk(rest)); got.Allowed || got.Reason != ReasonImpersonationDenied {
			t.Errorf("role %s without the action in the record: %+v", role, got)
		}
	}
}

// Every deny the delegation wrapper returns is cache class "none" (a deny is never cached). The literal, not the constant.
func TestDelegation_EveryDelegatedDenyIsCacheNone(t *testing.T) {
	e := NewEvaluator()
	all := allOrgActions(t)
	with := func(mut func(*delegatedSnapshot)) Snapshot {
		s := delegated("owner", all, "")
		mut(&s)
		return s
	}
	cases := []struct {
		name   string
		req    Request
		snap   Snapshot
		reason Reason
	}{
		{"empty chain", orgRequest("org.read"), with(func(s *delegatedSnapshot) { s.d.Chain = nil }), ReasonDelegationInvalid},
		{"nested chain", orgRequest("org.read"), with(func(s *delegatedSnapshot) { s.d.Chain = append(s.d.Chain, Hop{ActorPrincipalID: "prn_b"}) }), ReasonImpersonationDenied},
		{"empty actor", orgRequest("org.read"), with(func(s *delegatedSnapshot) { s.d.Chain[0].ActorPrincipalID = "" }), ReasonDelegationInvalid},
		{"empty target", orgRequest("org.read"), with(func(s *delegatedSnapshot) { s.d.TargetOrganizationID = "" }), ReasonDelegationInvalid},
		{"bad assurance", orgRequest("org.read"), with(func(s *delegatedSnapshot) { s.d.Assurance = "aal9" }), ReasonDelegationInvalid},
		{"other org", Request{Action: "org.read", ResourceKind: "organization", OrganizationID: "org-2"}, with(func(s *delegatedSnapshot) {}), ReasonDelegationInvalid},
		{"action not permitted", orgRequest("org.read"), delegated("owner", []string{"analytics.read"}, ""), ReasonImpersonationDenied},
		{"platform scope action", platformRequest("admin.users"), func() Snapshot {
			s := delegated("owner", []string{"admin.users"}, "")
			s.fakeSnapshot.platformRole = "platform_admin"
			return s
		}(), ReasonImpersonationDenied},
		{"unknown action", orgRequest("org.fly"), delegated("owner", all, ""), ReasonUnknownAction},
		{"floor under a non-owner", orgRequest("members.manage"), delegated("owner", all, ""), ReasonImpersonationDenied},
		{"flag not allowed under a non-owner", orgRequest("settings.write"), delegated("owner", all, ""), ReasonImpersonationDenied},
	}
	for _, c := range cases {
		got := e.Evaluate(c.req, c.snap)
		if got.Allowed || got.Reason != c.reason {
			t.Errorf("%s: %+v want deny %s", c.name, got, c.reason)
		}
		if string(got.CacheClass) != "none" {
			t.Errorf("%s: cache class %q want %q", c.name, got.CacheClass, "none")
		}
	}
}
