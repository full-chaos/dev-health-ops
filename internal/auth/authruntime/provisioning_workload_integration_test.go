//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const workloadProvisioningIssuer = "https://auth.example.test"

// workloadProvisioningEnv is the real stack with the provisioning routes on,
// an EdDSA signing key, and the go-api workload subject configured: the
// provisioning routes take the same workload path as the session issue route.
type workloadProvisioningEnv struct {
	*routesEnv
	svc      map[string]string
	svcID    uuid.UUID
	signer   *token.Signer
	workload uuid.UUID
	subject  string
}

func newWorkloadProvisioningEnv(t *testing.T, ctx context.Context) *workloadProvisioningEnv {
	t.Helper()
	useKeyStates(t, staticKeyStates(nil))
	dir := t.TempDir()
	writeSigningKey(t, dir, "k1.pem")
	workload := uuid.New()
	subject := session.WirePrincipalID(workload)
	e := newRoutesEnvWith(t, ctx, map[string]string{
		authconfig.EnvUsersOwned:                  "true",
		authconfig.EnvMembershipRoutes:            "true",
		authconfig.EnvInviteTokenSecret:           provisioningInviteSecret,
		authconfig.EnvInviteTokenSecretPrevious:   provisioningInvitePrevious,
		authconfig.EnvSigningKeyFile:              filepath.Join(dir, "k1.pem"),
		authconfig.EnvSigningKeyID:                "k1",
		authconfig.EnvAccessTokenClass:            authconfig.AccessClassEdDSA,
		authconfig.EnvAccessTokenAudience:         authapi.AccessTokenAudience,
		authconfig.EnvTokenIssuerURL:              workloadProvisioningIssuer,
		authconfig.EnvSessionIssueWorkloadSubject: subject,
	})
	svc, svcID := e.issuerCredential(t, ctx, "auth-session-issuer", `["memberships:write", "sessions:issue"]`, false, false)
	return &workloadProvisioningEnv{routesEnv: e, svc: map[string]string{"Authorization": "Bearer " + svc}, svcID: svcID,
		signer: token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "k1.pem"), KeyID: "k1"}), workload: workload, subject: subject}
}

// mint signs a go-api workload token; mutate edits the claims first. It
// returns the Authorization header and the jti.
func (w *workloadProvisioningEnv) mint(t *testing.T, ctx context.Context, mutate func(*contracts.WorkloadTokenClaims)) (map[string]string, string) {
	t.Helper()
	now := time.Now()
	claims := contracts.WorkloadTokenClaims{Iss: workloadProvisioningIssuer, Aud: "auth-service", Sub: w.subject, PrincipalType: "workload",
		Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300}
	if mutate != nil {
		mutate(&claims)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	compact, _, err := w.signer.Sign(ctx, contracts.WorkloadTokenTyp, payload)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{"Authorization": "Bearer " + compact}, claims.Jti
}

// callerAttributes reads the caller attributes of the one audit row of
// eventType in org (with subject, when it is not nil).
func (w *workloadProvisioningEnv) callerAttributes(t *testing.T, ctx context.Context, eventType string, org uuid.UUID, subject *uuid.UUID) map[string]string {
	t.Helper()
	var principal, jti, credential *string
	if err := w.admin.QueryRow(ctx, `SELECT attributes->>'caller_principal_id', attributes->>'workload_jti', attributes->>'credential_id'
FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2 AND ($3::uuid IS NULL OR subject_principal_id = $3)`,
		eventType, org, subject).Scan(&principal, &jti, &credential); err != nil {
		t.Fatalf("%s audit row: %v", eventType, err)
	}
	out := map[string]string{}
	for key, value := range map[string]*string{"caller_principal_id": principal, "workload_jti": jti, "credential_id": credential} {
		if value != nil {
			out[key] = *value
		}
	}
	return out
}

func (w *workloadProvisioningEnv) wantWorkloadCaller(t *testing.T, ctx context.Context, eventType string, org uuid.UUID, subject *uuid.UUID, jti string) {
	t.Helper()
	got := w.callerAttributes(t, ctx, eventType, org, subject)
	if len(got) != 2 || got["caller_principal_id"] != w.workload.String() || got["workload_jti"] != jti {
		t.Fatalf("%s caller attributes = %v, want the workload %s and jti %s", eventType, got, w.workload, jti)
	}
}

// I1: the workload token adds a member through configure: both planes are
// written and the audit row names the human actor and the workload caller.
func TestProvisioningWorkloadTokenAddsAMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	owner, org := w.seed(t, ctx)
	member := w.account(t, ctx, acctMail(), false, true)
	header, jti := w.mint(t, ctx, nil)
	got := w.doWith(t, "/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org.String(),
		"principal_id": member.String(), "actor_principal_id": owner.String(), "role": "member"}), header)
	if got.status != http.StatusOK {
		t.Fatalf("add: %d %s", got.status, got.body)
	}
	if pub, auth := w.memberRows(t, ctx, org, member); pub != "member" || auth != "active" {
		t.Fatalf("after add: public %q auth %q", pub, auth)
	}
	var actor string
	if err := w.admin.QueryRow(ctx, `SELECT actor_principal_id::text FROM auth.security_audit_events
WHERE event_type = 'auth.membership.added' AND organization_id = $1 AND subject_principal_id = $2`, org, member).Scan(&actor); err != nil || actor != owner.String() {
		t.Fatalf("added audit actor = %q (%v), want the owner", actor, err)
	}
	w.wantWorkloadCaller(t, ctx, "auth.membership.added", org, &member, jti)
}

// I2: a workload token with another subject or audience, or past its
// lifetime, is a 401 and writes nothing.
func TestProvisioningWorkloadTokenRefusals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	owner, org := w.seed(t, ctx)
	member := w.account(t, ctx, acctMail(), false, true)
	add := jsonBody(t, map[string]any{"organization_id": org.String(), "principal_id": member.String(), "actor_principal_id": owner.String(), "role": "member"})
	for name, mutate := range map[string]func(*contracts.WorkloadTokenClaims){
		"another subject":  func(c *contracts.WorkloadTokenClaims) { c.Sub = session.WirePrincipalID(uuid.New()) },
		"another audience": func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" },
		"expired":          func(c *contracts.WorkloadTokenClaims) { c.Iat, c.Nbf, c.Exp = c.Iat-400, c.Nbf-400, c.Exp-400 },
	} {
		header, _ := w.mint(t, ctx, mutate)
		if got := w.doWith(t, "/internal/v1/memberships/add", add, header); got.status != http.StatusUnauthorized {
			t.Fatalf("%s: %d %s, want 401", name, got.status, got.body)
		}
	}
	if pub, auth := w.memberRows(t, ctx, org, member); pub != "" || auth != "" {
		t.Fatalf("refused tokens wrote rows: %q %q", pub, auth)
	}
}

// I3: the session issuer credential holding memberships:write still works
// beside the workload path, and its audit row names the credential row.
func TestProvisioningServiceCredentialStillWorksBesideTheWorkloadPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	owner, org := w.seed(t, ctx)
	member := w.account(t, ctx, acctMail(), false, true)
	got := w.doWith(t, "/internal/v1/memberships/add", jsonBody(t, map[string]any{"organization_id": org.String(),
		"principal_id": member.String(), "actor_principal_id": owner.String(), "role": "member"}), w.svc)
	if got.status != http.StatusOK {
		t.Fatalf("add: %d %s", got.status, got.body)
	}
	if attrs := w.callerAttributes(t, ctx, "auth.membership.added", org, &member); len(attrs) != 1 || attrs["credential_id"] != w.svcID.String() {
		t.Fatalf("caller attributes = %v, want credential_id %s only", attrs, w.svcID)
	}
}

// I4: an invite is created and accepted on the workload token; each invite
// audit row names the caller of its call. The accept commits one audit row
// (auth.invite.accepted); the membership it adds writes none of its own.
func TestProvisioningWorkloadTokenInviteCreateAndAccept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	owner, org := w.seed(t, ctx)
	address := acctMail()
	createHeader, createJTI := w.mint(t, ctx, nil)
	created := w.doWith(t, "/internal/v1/invites/create", jsonBody(t, map[string]any{"organization_id": org.String(), "email": address,
		"role": "member", "actor_principal_id": owner.String()}), createHeader)
	if created.status != http.StatusOK {
		t.Fatalf("create: %d %s", created.status, created.body)
	}
	invitee := w.account(t, ctx, address, false, true)
	acceptHeader, acceptJTI := w.mint(t, ctx, nil)
	accepted := w.doWith(t, "/internal/v1/invites/accept", jsonBody(t, map[string]any{"token": object(t, created)["token"],
		"principal_id": invitee.String()}), acceptHeader)
	if accepted.status != http.StatusOK {
		t.Fatalf("accept: %d %s", accepted.status, accepted.body)
	}
	if pub, auth := w.memberRows(t, ctx, org, invitee); pub != "member" || auth != "active" {
		t.Fatalf("after accept: public %q auth %q", pub, auth)
	}
	w.wantWorkloadCaller(t, ctx, "auth.invite.created", org, nil, createJTI)
	w.wantWorkloadCaller(t, ctx, "auth.invite.accepted", org, &invitee, acceptJTI)
}

// I5: a superuser deprovisions on the workload token; the audit row names the
// caller.
func TestProvisioningWorkloadTokenDeprovision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	_, org := w.seed(t, ctx)
	super := w.account(t, ctx, acctMail(), true, true)
	header, jti := w.mint(t, ctx, nil)
	got := w.doWith(t, "/internal/v1/organizations/deprovision", jsonBody(t, map[string]any{"organization_id": org.String(),
		"actor_principal_id": super.String()}), header)
	if got.status != http.StatusOK || object(t, got)["suspended"] != true {
		t.Fatalf("deprovision: %d %s", got.status, got.body)
	}
	w.wantWorkloadCaller(t, ctx, orgdeprovision.EventDeprovisioned, org, nil, jti)
}

// I6: a superuser reactivates on the workload token; the audit row names the
// caller.
func TestProvisioningWorkloadTokenReactivate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := newWorkloadProvisioningEnv(t, ctx)
	_, org := w.seed(t, ctx)
	super := w.account(t, ctx, acctMail(), true, true)
	body := jsonBody(t, map[string]any{"organization_id": org.String(), "actor_principal_id": super.String()})
	if got := w.doWith(t, "/internal/v1/organizations/deprovision", body, w.svc); got.status != http.StatusOK {
		t.Fatalf("deprovision: %d %s", got.status, got.body)
	}
	header, jti := w.mint(t, ctx, nil)
	got := w.doWith(t, "/internal/v1/organizations/reactivate", body, header)
	if got.status != http.StatusOK || object(t, got)["reactivated"] != true {
		t.Fatalf("reactivate: %d %s", got.status, got.body)
	}
	w.wantWorkloadCaller(t, ctx, "auth.organization.reactivated", org, nil, jti)
}
