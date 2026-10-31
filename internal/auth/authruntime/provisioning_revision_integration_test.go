//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
)

const orgStateRoute = "/internal/v1/organizations/state"

// revisionEnv is the real stack with the suspend and state routes mounted by planted profile rows, one superuser and
// one organization.
type revisionEnv struct {
	*routesEnv
	t      *testing.T
	ctx    context.Context
	header map[string]string
	super  uuid.UUID
	org    uuid.UUID
}

func newRevisionEnv(t *testing.T, ctx context.Context) *revisionEnv {
	t.Helper()
	withProfileRow(t, orgStateRoute)
	e, header := newProvisioningEnv(t, ctx)
	_, org := e.seed(t, ctx)
	return &revisionEnv{routesEnv: e, t: t, ctx: ctx, header: header, super: e.account(t, ctx, acctMail(), true, true), org: org}
}

func (r *revisionEnv) state() (status string, revision int64, code int) {
	r.t.Helper()
	got := r.doWith(r.t, orgStateRoute, jsonBody(r.t, map[string]any{"organization_id": r.org.String()}), r.header)
	if got.status != http.StatusOK {
		return "", 0, got.status
	}
	out := object(r.t, got)
	if len(out) != 3 || out["organization_id"] != r.org.String() {
		r.t.Fatalf("state answer = %v, want organization_id, status and revision", out)
	}
	return out["status"].(string), int64(out["revision"].(float64)), got.status
}

func (r *revisionEnv) verb(path string, ifRevision *int64) answer {
	r.t.Helper()
	body := map[string]any{"organization_id": r.org.String(), "actor_principal_id": r.super.String()}
	if ifRevision != nil {
		body["if_revision"] = *ifRevision
	}
	return r.doWith(r.t, path, jsonBody(r.t, body), r.header)
}

func (r *revisionEnv) counts() (outbox, audit int) {
	r.t.Helper()
	if err := r.admin.QueryRow(r.ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = 'org_' || $1::text`, r.org).Scan(&outbox); err != nil {
		r.t.Fatal(err)
	}
	if err := r.admin.QueryRow(r.ctx, `SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $1 AND event_type IN ('auth.organization.suspended', 'auth.organization.reactivated')`, r.org).Scan(&audit); err != nil {
		r.t.Fatal(err)
	}
	return outbox, audit
}

func (r *revisionEnv) wantStatus(want string) int64 {
	r.t.Helper()
	status, revision, code := r.state()
	if code != http.StatusOK || status != want {
		r.t.Fatalf("state = %q (%d), want %q", status, code, want)
	}
	return revision
}

func (r *revisionEnv) wantConflict(got answer, what string) {
	r.t.Helper()
	if got.status != http.StatusConflict || object(r.t, got)["reason_code"] != "state_changed" {
		r.t.Fatalf("%s: %d %s, want 409 state_changed", what, got.status, got.body)
	}
}

// The state route answers the status and the revision the aggregate holds (one statement), moves with each real change,
// refuses an unknown organization, and is dark without its row (the other cells plant it).
func TestOrgStateRouteThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r := newRevisionEnv(t, ctx)
	r0 := r.wantStatus("active")
	var stored int64
	if err := r.admin.QueryRow(ctx, `SELECT COALESCE(max(revision), 0) FROM auth.aggregate_revisions WHERE aggregate_type = 'organization' AND aggregate_id = 'org_' || $1::text`, r.org).Scan(&stored); err != nil || stored != r0 {
		t.Fatalf("the route answered revision %d, the aggregate holds %d (%v)", r0, stored, err)
	}
	if got := r.verb(authapi.PatternOrgSuspend, nil); got.status != http.StatusOK {
		t.Fatalf("suspend: %d %s", got.status, got.body)
	}
	if r1 := r.wantStatus("suspended"); r1 <= r0 {
		t.Fatalf("revision after a suspend = %d, want more than %d", r1, r0)
	}
	if got := r.doWith(t, orgStateRoute, jsonBody(t, map[string]any{"organization_id": uuid.NewString()}), r.header); got.status != http.StatusNotFound ||
		object(t, got)["reason_code"] != "organization_not_found" {
		t.Fatalf("state of an unknown organization: %d %s", got.status, got.body)
	}
	if got := r.do(t, http.MethodPost, orgStateRoute, jsonBody(t, map[string]any{"organization_id": r.org.String()}), ""); got.status != http.StatusUnauthorized {
		t.Fatalf("state with no credential: %d", got.status)
	}
}

// A request carrying a stale revision changes nothing: after a suspend, a reactivate that carries the revision read
// before it is refused 409 and the organization stays suspended; the mirror holds for a stale suspend after a
// reactivate. The refusal writes no outbox event and no success audit row.
func TestStaleRevisionIsRefusedOnBothRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r := newRevisionEnv(t, ctx)
	r0 := r.wantStatus("active")
	if got := r.verb(authapi.PatternOrgSuspend, &r0); got.status != http.StatusOK || object(t, got)["suspended"] != true {
		t.Fatalf("suspend at the current revision: %d %s", got.status, got.body)
	}
	r1 := r.wantStatus("suspended")
	outbox, audit := r.counts()
	r.wantConflict(r.verb(authapi.PatternOrgReactivate, &r0), "stale reactivate after a suspend")
	r.wantStatus("suspended")
	if o, a := r.counts(); o != outbox || a != audit {
		t.Fatalf("the 409 wrote (outbox %d to %d, audit %d to %d)", outbox, o, audit, a)
	}
	if got := r.verb(authapi.PatternOrgReactivate, &r1); got.status != http.StatusOK || object(t, got)["reactivated"] != true {
		t.Fatalf("reactivate at the current revision: %d %s", got.status, got.body)
	}
	r2 := r.wantStatus("active")
	outbox, audit = r.counts()
	r.wantConflict(r.verb(authapi.PatternOrgSuspend, &r1), "stale suspend after a reactivate")
	if r.wantStatus("active") != r2 {
		t.Fatalf("the revision moved on a refused call")
	}
	if o, a := r.counts(); o != outbox || a != audit {
		t.Fatalf("the 409 wrote (outbox %d to %d, audit %d to %d)", outbox, o, audit, a)
	}
}

// A no-op with a stale revision is a 409 too, on both routes (the organization is already at the target status), and a
// no-op at the current revision is the 200 of today with nothing written.
func TestStaleRevisionNoOpIsRefusedOnBothRoutesThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r := newRevisionEnv(t, ctx)
	r0 := r.wantStatus("active")
	r.verb(authapi.PatternOrgSuspend, nil)
	r.verb(authapi.PatternOrgReactivate, nil)
	current := r.wantStatus("active")
	r.wantConflict(r.verb(authapi.PatternOrgReactivate, &r0), "stale reactivate of an active organization")
	if got := r.verb(authapi.PatternOrgReactivate, &current); got.status != http.StatusOK || object(t, got)["reactivated"] != false {
		t.Fatalf("reactivate no-op at the current revision: %d %s", got.status, got.body)
	}
	r.verb(authapi.PatternOrgSuspend, nil)
	suspended := r.wantStatus("suspended")
	outbox, audit := r.counts()
	r.wantConflict(r.verb(authapi.PatternOrgSuspend, &current), "stale suspend of a suspended organization")
	if got := r.verb(authapi.PatternOrgSuspend, &suspended); got.status != http.StatusOK || object(t, got)["suspended"] != false {
		t.Fatalf("suspend no-op at the current revision: %d %s", got.status, got.body)
	}
	if o, a := r.counts(); o != outbox || a != audit {
		t.Fatalf("a no-op or a 409 wrote (outbox %d to %d, audit %d to %d)", outbox, o, audit, a)
	}
}

// A refused actor is still refused before the revision is looked at (authz after the row lock, then the revision).
func TestRevisionIsCheckedAfterTheActorRuleThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	r := newRevisionEnv(t, ctx)
	owner, org := r.seed(t, ctx)
	stale := int64(999999)
	got := r.doWith(t, authapi.PatternOrgSuspend, jsonBody(t, map[string]any{"organization_id": org.String(), "actor_principal_id": owner.String(),
		"if_revision": stale}), r.header)
	if got.status != http.StatusForbidden || object(t, got)["reason_code"] != "superuser_required" {
		t.Fatalf("a non-superuser with a stale revision: %d %s, want 403 superuser_required", got.status, got.body)
	}
}
